use super::*;

use evm::backend::{Backend, Basic};
use evm_schema::EvmSchema;
use evm_state::{evm, Account, BlockNum, BlockVersion, Code, H160, H256, U256};
use evm_state::{ChainContext, EvmConfig, TransactionContext};

#[derive(Debug)]
pub struct ExecutorContext<'a, A, C, S> {
    pub(crate) backend: &'a mut EvmSchema<A, C, S>,
    chain_context: ChainContext,
    tx_context: TransactionContext,
    config: EvmConfig,
    block_info: BlockInfo,
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    root: H256,
    num: BlockNum,
    block_version: BlockVersion,
    timestamp: u64,
}

impl<'a, AccountMap, CodeMap, StorageMap> Backend
    for ExecutorContext<'a, AccountMap, CodeMap, StorageMap>
where
    // account
    AccountMap: AsyncMap<K = (H160, BlockNum)>,
    AccountMap: AsyncMap<V = Account>,
    AccountMap: AsyncMapSearch,
    // code
    CodeMap: AsyncMap<K = H160>,
    CodeMap: AsyncMap<V = Code>,
    // storage
    StorageMap: AsyncMap<K = (H160, H256, BlockNum)>,
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
        self.backend
            .find_last_account(address, self.block_info.num)
            .is_some()
    }

    fn basic(&self, address: H160) -> Basic {
        let Account { balance, nonce, .. } = self
            .backend
            .find_last_account(address, self.block_info.num)
            .unwrap_or_default();

        Basic { balance, nonce }
    }

    fn code(&self, address: H160) -> Vec<u8> {
        let code = self.backend.find_code(address).unwrap_or_default();
        code.into()
    }

    fn storage(&self, address: H160, index: H256) -> H256 {
        self.backend
            .find_storage(address, index, self.block_info.num)
            .unwrap_or_default()
    }

    fn original_storage(&self, address: H160, index: H256) -> Option<H256> {
        Some(self.storage(address, index))
    }
}
