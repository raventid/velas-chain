use std::collections::HashMap;
use std::iter::FromIterator;

use super::*;

use evm::backend::{Backend, Basic};
use evm_schema::{EvmSchema, HashedAddress};
use evm_state::{evm, Account, Apply, BlockNum, BlockVersion, Code, EvmBackend, H160, H256, U256};
use evm_state::{BackendProvider, ChainContext, EvmConfig, TransactionContext};

#[derive(Debug, Clone)]
pub struct ChangedAccount {
    pub storage: HashMap<H256, H256>,
    pub balance: U256,
    pub nonce: U256,
    pub code: Option<Code>,
}
#[derive(Debug)]
pub struct ExecutorContext<'a, A, C, S> {
    pub(crate) backend: &'a mut EvmSchema<A, C, S>,
    chain_context: ChainContext,
    tx_context: TransactionContext,
    config: EvmConfig,
    block_info: BlockInfo,
    changes: &'a mut BTreeMap<H160, ChangedAccount>,
    used_gas: &'a mut u64,
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub root: H256,
    pub num: BlockNum,
    pub block_version: BlockVersion,
    pub timestamp: u64,
}

impl<'a, AccountMap, CodeMap, StorageMap> Backend
    for ExecutorContext<'a, AccountMap, CodeMap, StorageMap>
where
    // account
    AccountMap: AsyncMap<K = (HashedAddress, BlockNum)>,
    AccountMap: AsyncMap<V = Account>,
    AccountMap: AsyncMapSearch,
    // code
    CodeMap: AsyncMap<K = (HashedAddress, BlockNum)>,
    CodeMap: AsyncMap<V = Code>,
    CodeMap: AsyncMapSearch,
    // storage
    StorageMap: AsyncMap<K = (HashedAddress, H256, BlockNum)>,
    StorageMap: AsyncMap<V = H256>,
    StorageMap: AsyncMapSearch,
{
    fn gas_price(&self) -> U256 {
        self.tx_context.gas_price.into()
    }

    fn origin(&self) -> H160 {
        self.tx_context.origin
    }

    fn block_coinbase(&self) -> H160 {
        self.tx_context.coinbase
    }

    fn block_number(&self) -> U256 {
        self.block_info.num.into()
    }

    fn block_timestamp(&self) -> U256 {
        self.block_info.timestamp.into()
    }

    fn block_hash(&self, number: U256) -> H256 {
        let current_block = self.block_number();
        if number >= current_block
            || current_block - number - U256::one()
                >= U256::from(self.chain_context.last_hashes.len())
        {
            H256::default()
        } else {
            let index = if self.block_info.block_version >= BlockVersion::VersionConsistentHashes {
                // Fix bug with block_hash calculation
                self.chain_context.last_hashes.len() - (current_block - number).as_usize()
            } else {
                (current_block - number - U256::one()).as_usize()
            };
            self.chain_context.last_hashes[index]
        }
    }

    fn block_difficulty(&self) -> U256 {
        self.chain_context.difficulty
    }

    fn block_gas_limit(&self) -> U256 {
        self.config.gas_limit.into()
    }

    fn chain_id(&self) -> U256 {
        self.config.chain_id.into()
    }

    fn exists(&self, address: H160) -> bool {
        if let Some(ChangedAccount { .. }) = self.changes.get(&address) {
            return true;
        }
        self.backend
            .find_last_account(address, self.block_info.num)
            .is_some()
    }

    fn basic(&self, address: H160) -> Basic {
        if let Some(ChangedAccount { balance, nonce, .. }) = self.changes.get(&address) {
            return Basic {
                balance: *balance,
                nonce: *nonce,
            };
        }

        let Account { balance, nonce, .. } = self
            .backend
            .find_last_account(address, self.block_info.num)
            .unwrap_or_default();

        Basic { balance, nonce }
    }

    fn code(&self, address: H160) -> Vec<u8> {
        if let Some(ChangedAccount { code, .. }) = self.changes.get(&address) {
            if let Some(v) = code {
                return v.clone().into();
            }
        }
        let code = self
            .backend
            .find_code(address, self.block_info.num)
            .unwrap_or_default();
        code.into()
    }

    fn storage(&self, address: H160, index: H256) -> H256 {
        if let Some(ChangedAccount { storage, .. }) = self.changes.get(&address) {
            if let Some(v) = storage.get(&index) {
                return *v;
            }
        }
        self.backend
            .find_storage(address, index, self.block_info.num)
            .unwrap_or_default()
    }

    fn original_storage(&self, address: H160, index: H256) -> Option<H256> {
        Some(self.storage(address, index))
    }
}

pub struct EvmBigTableExecutorProvider<'a, A, C, S> {
    pub schema: &'a mut EvmSchema<A, C, S>,
    pub changes: &'a mut BTreeMap<H160, ChangedAccount>,
    pub used_gas: &'a mut u64,
    pub block_info: BlockInfo,
}

impl<'a, State, AccountMap, CodeMap, StorageMap> BackendProvider<'a, State>
    for EvmBigTableExecutorProvider<'a, AccountMap, CodeMap, StorageMap>
where
    // account
    AccountMap: AsyncMap<K = (HashedAddress, BlockNum)>,
    AccountMap: AsyncMap<V = Account>,
    AccountMap: AsyncMapSearch,
    // code
    CodeMap: AsyncMap<K = (HashedAddress, BlockNum)>,
    CodeMap: AsyncMap<V = Code>,
    CodeMap: AsyncMapSearch,
    // storage
    StorageMap: AsyncMap<K = (HashedAddress, H256, BlockNum)>,
    StorageMap: AsyncMap<V = H256>,
    StorageMap: AsyncMapSearch,
{
    type Output = ExecutorContext<'a, AccountMap, CodeMap, StorageMap>;
    fn construct(
        self,
        backend: &'a mut EvmBackend<State>,
        chain_context: ChainContext,
        tx_context: TransactionContext,
        config: EvmConfig,
    ) -> Self::Output {
        ExecutorContext {
            backend: self.schema,
            block_info: self.block_info,
            changes: self.changes,
            used_gas: self.used_gas,
            chain_context,
            tx_context,
            config,
        }
    }

    fn gas_left(this: &Self::Output) -> u64 {
        this.config.gas_limit // TODO reduce at apply - this.backend.used_gas()
    }
    // TODO: implement lapply
    fn apply<Applies, I>(this: Self::Output, values: Applies, used_gas: u64)
    where
        Applies: IntoIterator<Item = Apply<I>>,
        I: IntoIterator<Item = (H256, H256)>,
    {
        for apply in values {
            match apply {
                Apply::Modify {
                    address,
                    basic,
                    code,
                    storage,
                    reset_storage: _,
                } => {
                    log::debug!(
                        "Apply::Modify address = {:?}, basic = {:?}, code = {:?}",
                        address,
                        basic,
                        code
                    );

                    let new_storage = HashMap::<H256, H256>::from_iter(storage);
                    log::debug!("Apply::Modify storage = {:?}", new_storage);

                    let account_state = this.changes.get(&address).cloned();
                    let account_change = match account_state {
                        None => ChangedAccount {
                            nonce: basic.nonce,
                            balance: basic.balance,
                            storage: new_storage,
                            code: code.map(From::from),
                        },
                        Some(ChangedAccount {
                            nonce,
                            balance,
                            storage,
                            code: old_code,
                        }) => ChangedAccount {
                            nonce: basic.nonce,
                            balance: basic.balance,
                            storage: storage.into_iter().chain(new_storage).collect(),
                            code: old_code.or_else(|| code.map(From::from)),
                        },
                    };
                    this.changes.insert(address, account_change);
                }
                Apply::Delete { address } => {
                    // this.changes.insert(address, ChangedAccount::Removed);
                }
            }
        }

        *this.used_gas += used_gas;
    }
}
