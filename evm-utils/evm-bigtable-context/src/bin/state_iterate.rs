use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use anyhow::Result;
use dashmap::DashMap;
use evm_bigtable_context::context::{BlockInfo, ChangedAccount, EvmBigTableExecutorProvider};
use evm_bigtable_context::evm_schema::EvmSchema;
use evm_bigtable_context::memory::SerializedMap;
use evm_state::{
    storage, Account, AccountState, BlockNum, BlockVersion, Code, Storage, TransactionInReceipt,
    TransactionReceipt, H256,
};
use evm_state::{Context, EvmBackend, Executor, ExitError, ExitSucceed, H160}; // simulation
use log::LevelFilter;
use log::*;
use rayon::prelude::*;
use solana_runtime::evm_snapshot::{inspectors, Walker};
use solana_storage_bigtable::LedgerStorage;

#[derive(Debug, structopt::StructOpt)]
enum Args {
    IterateAndPush {
        //Path to evm-state database.
        #[structopt(long = "evm-state-path")]
        evm_state_path: String,
        // #[structopt(long = "starting-root")]
        // starting_root: Option<H256>,
        #[structopt(long = "starting-block")]
        start_block: BlockNum,
        #[structopt(long = "ending-block")]
        end_block: BlockNum,
        #[structopt(long = "dry-run")]
        dry_run: bool,
        #[structopt(long = "validate-by-replay-tx")]
        replay_txs: bool,
    },
    FullBackup {
        // Path to evm-state database.
        #[structopt(long = "evm-state-path")]
        evm_state_path: String,
        // The block number when snapshot should be created.
        #[structopt(long = "block-num")]
        block: BlockNum,
        // Root hash of state to store.
        #[structopt(long = "root")]
        root: H256,
        // Not push any changes, just print them.
        #[structopt(long = "dry-run")]
        dry_run: bool,
    },
}

#[allow(clippy::type_complexity)]
fn noop_precompile(
    _: H160,
    _: &[u8],
    _: Option<u64>,
    _: &Context,
) -> Option<Result<(ExitSucceed, Vec<u8>, u64), ExitError>> {
    None
}

type Simulator = EvmSchema<
    SerializedMap<(HashedAddress, BlockNum), Account>,
    SerializedMap<HashedAddress, Code>,
    SerializedMap<(HashedAddress, H256, BlockNum), H256>,
>;
//TODO Commands:
// 1. Make full-backup :
// - Creates full backup of state with root __ at block __.
// 2. Iterate-push:
// - Iterate over bigtable block headers - find any changes - push changes to bigtable
// 3. Iterate-tx-validate: (submode of 2)
// - Iterate over bigtable blocks - find any changes in in state - try to execute block txs, and validate compare change list.
//
// rayon multithread is not important in single tree traversal - we have less than 10k accounts, but

pub fn simulate_tx(
    simulator: &mut Simulator,
    txs: Vec<TransactionReceipt>,
    num: BlockNum,
    root: H256,
    timestamp: u64,
    block_version: BlockVersion,
) -> BTreeMap<H160, ChangedAccount> {
    let mut used_gas = 0;
    let mut changes_cache = Default::default();

    let mut executor = Executor::with_config(
        EvmBackend::default(),
        Default::default(),
        Default::default(),
    );
    for tx in txs {
        let execution_context = EvmBigTableExecutorProvider {
            schema: &mut *simulator,
            changes: &mut changes_cache,
            used_gas: &mut used_gas,
            block_info: BlockInfo {
                root,
                num,
                block_version,
                timestamp,
            },
        };

        let (tx_hash, caller, nonce, gas_price, gas_limit, action, data, value, chain_id) =
            match tx.transaction {
                TransactionInReceipt::Unsigned(t) => (
                    t.tx_id_hash(),
                    t.caller,
                    t.unsigned_tx.nonce,
                    t.unsigned_tx.gas_price,
                    t.unsigned_tx.gas_limit,
                    t.unsigned_tx.action,
                    t.unsigned_tx.input,
                    t.unsigned_tx.value,
                    t.chain_id,
                ),
                TransactionInReceipt::Signed(t) => (
                    t.tx_id_hash(),
                    t.caller()
                        .expect("Cannot retrive caller from committed tx."),
                    t.nonce,
                    t.gas_price,
                    t.gas_limit,
                    t.action,
                    t.input,
                    t.value,
                    t.signature
                        .chain_id()
                        .expect("We didn't support Null at chain_id"),
                ),
            };
        let exit_result = executor
            .transaction_execute_raw(
                execution_context,
                caller,
                nonce,
                gas_price,
                gas_limit,
                action,
                data,
                value,
                chain_id.into(),
                tx_hash,
                noop_precompile,
            )
            .unwrap();
    }
    changes_cache
}

/// account hashed address
/// It's impossible to recover addres from hash. So save its hash in collection.

#[derive(Debug, Default)]
struct AccountChange {
    new_state: Account,
    code: Option<Code>,
    storage: HashMap<H256, H256>,
}

type HashedAddress = H256;
#[derive(Debug)]
struct CurrentState {
    accounts: DashMap<HashedAddress, Account>,
    storage: DashMap<HashedAddress, DashMap<H256, H256>>,
    codes: DashMap<H256, Code>,
    block_num: BlockNum,
}

impl CurrentState {
    // Init state from root
    fn new(block_num: BlockNum, root: H256, storage: &Storage, dry_run: bool) -> Self {
        info!(
            "Initializing block with number = {}, root = {:?}",
            block_num, root
        );

        let mut current_state = Self {
            block_num,
            accounts: DashMap::new(),
            storage: DashMap::new(),
            codes: DashMap::new(),
        };
        current_state
            .next_block_inner(storage, root)
            .expect("Root not found");
        current_state
    }

    async fn next_block(
        &mut self,
        evm_state: &Storage,
        bigtable_storage: &LedgerStorage,
        replay_simulator: Option<&mut Simulator>,
    ) -> Option<HashMap<HashedAddress, AccountChange>> {
        debug!("Requested block = {}", self.block_num);
        let header;
        let changes_by_tx: Option<BTreeMap<H160, ChangedAccount>> =
            if let Some(simulator) = replay_simulator {
                let block = bigtable_storage
                .get_evm_confirmed_full_block(self.block_num)
                .await
                .expect(
                    "No root hash found, expecting block before starting to be found in bigtable",
                );
                header = block.header;
                todo!()
            } else {
                header = bigtable_storage
                .get_evm_confirmed_block_header(self.block_num)
                .await
                .expect(
                    "No root hash found, expecting block before starting to be found in bigtable",
                );
                None
            };

        info!(
            "Found block, requesting changes: block_num = {}, state_root = {:?}",
            header.block_number, header.state_root
        );
        let result = self.next_block_inner(evm_state, header.state_root);
        if let Some(changes) = &result {
            for (k, acc) in changes {
                debug!("{:?} => {:?}", k, acc.new_state);
            }
            self.block_num += 1;
        }

        if let Some(simulator) = replay_simulator {
            todo!()
        }

        result
    }

    // Proceed next block, find differences, apply changes to state, and return changed fields.
    fn next_block_inner(
        &mut self,
        storage: &Storage,
        root: H256,
    ) -> Option<HashMap<HashedAddress, AccountChange>> {
        let db = storage.db();
        if !storage.check_root_exist(root) {
            return None;
        }
        let accounts_state_walker = Walker::new(db, memorizer::AccountsKeysCollector::default());
        accounts_state_walker
            .traverse(Default::default(), root)
            .unwrap();

        let changed_accounts: HashMap<HashedAddress, AccountChange> = accounts_state_walker
            .inspector
            .account_keys
            .par_iter()
            .filter(|r| {
                let key = r.key();
                // keep only new accounts, or accounts that have changes in its state.
                if let Some(found_account) = self.accounts.get(key) {
                    if found_account.value() == r.value() {
                        return false;
                    }
                    debug!("Found changed account: key={:?}, before={:?}, after={:?}", key, found_account.value(), r.value());
                }
                true
            })
            .map(|r|{
                let key = *r.key();
                let acc = r.value();
                let mut account_change = AccountChange::default();
                let code_hash = acc.code_hash;
                // if storage state was not changed - ignore
                let storage_chaned = !matches!(self.accounts.get(&key), Some(found_account) if found_account.storage_root == r.storage_root );
                let code_changed = !matches!(self.accounts.get(&key), Some(found_account) if found_account.code_hash == r.code_hash );

                account_change.new_state = acc.clone();
                if code_changed {
                    account_change.code = if let Some(code_data) = storage.get::<storage::Codes>(code_hash) {
                        Some(code_data)
                    } else {
                        assert_eq!(code_hash, Code::empty().hash());
                        None
                    };
                }
                if storage_chaned {
                    let storage_state = Walker::new(
                        db,
                        memorizer::StorageCollector {
                            storage_data: DashMap::new(),
                        },
                    );
                    storage_state
                        .traverse(Default::default(), acc.storage_root)
                        .unwrap();
                    ;

                    let exist_data = self.storage.get(&key);
                    for (idx, data) in storage_state.inspector.storage_data {
                        let idx_changed = matches!(exist_data.as_ref().map(|s|s.get(&idx)), Some(Some(s)) if *s.value() == data);
                        if idx_changed {
                            account_change.storage.insert(idx, data);
                        }
                    }
                }
                (key, account_change)
            })
            .collect();

        for (key, change) in &changed_accounts {
            debug!(
                "Apply change account: key={:?}, after={:?}",
                key, change.new_state
            );
            self.accounts.insert(*key, change.new_state);
            let mut entry = self.storage.entry(*key).or_insert(DashMap::new());
            for (idx, data) in &change.storage {
                entry.insert(*idx, *data);
            }
            if let Some(code) = &change.code {
                self.codes.insert(*key, code.clone());
            }
        }
        Some(changed_accounts)
    }
}

#[paw::main]
#[tokio::main]
async fn main(args: Args) {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(LevelFilter::Off);
    builder.parse_env("RUST_LOG");
    builder.init();

    match args {
        Args::IterateAndPush {
            evm_state_path,
            start_block,
            end_block,
            dry_run,
            replay_txs,
        } => {
            let storage = Storage::open_persistent(evm_state_path).unwrap();

            let ledger_storage = LedgerStorage::new(true, Duration::from_secs(10).into())
                .await
                .unwrap();
            let block_num = start_block
                .checked_sub(1)
                .expect("No root hash found, expecting starting block > 0");
            info!(
                "Reading root from ledger storage with block_num = {}",
                block_num
            );
            let root =
            // if let Some(root) = starting_root {
            //     root
            // } else 
            {
                ledger_storage
                    .get_evm_confirmed_block_header(
                        block_num
                    )
                    .await
                    .expect("No root hash found, expecting block before starting to be found in bigtable")
                    .state_root
            };

            if dry_run {
                info!("Dry run, do nothing after collecting keys ...");
            }

            let mut state = CurrentState::new(block_num, root, &storage, dry_run);

            //         let mut simulator = if replay_txs {
            //             let mut simulator = Simulator::new_serialized_tmp().unwrap();
            //             simulator.push_account_change()
            // //             push_account_change(
            // //     &self,
            // //     key: H160,
            // //     block_num: BlockNum,
            // //     state: Account,
            // //     code: Option<Code>,
            // //     storage_updates: HashMap<H256, H256>,
            // // )
            //         }
            //         else {
            //             None
            //         };
            for block in start_block..=end_block {
                if let Some(_) = state.next_block(&storage, &ledger_storage, None).await {
                } else {
                    error!("Block {} not found in state, stopping.", block);
                    std::process::exit(1);
                }
                assert_eq!(state.block_num, block);
            }
        }
        Args::FullBackup {
            evm_state_path,
            block,
            root,
            dry_run,
        } => {
            todo!()
        }
    }
}

pub mod memorizer {
    use super::*;
    use dashmap::DashMap;
    use evm_state::U256;
    use solana_runtime::evm_snapshot::Inspector;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    pub struct AccountsKeysCollector {
        pub account_keys: DashMap<H256, Account>,
    }

    impl Inspector<Account> for AccountsKeysCollector {
        fn inspect_raw<Data: AsRef<[u8]>>(&self, key: H256, data: &Data) -> Result<bool> {
            Ok(false)
        }
        fn inspect_typed(&self, key: Vec<u8>, account: &Account) -> Result<()> {
            if key.len() != 32 {
                anyhow::bail!("Key len({}) is not equal to 32", key.len());
            }
            let key = H256::from_slice(&key);
            self.account_keys.insert(key, account.clone());
            Ok(())
        }
    }

    pub struct StorageCollector {
        pub storage_data: DashMap<H256, H256>,
    }

    impl Inspector<U256> for StorageCollector {
        fn inspect_raw<Data: AsRef<[u8]>>(&self, key: H256, data: &Data) -> Result<bool> {
            Ok(false)
        }
        fn inspect_typed(&self, key: Vec<u8>, data: &U256) -> Result<()> {
            if key.len() != 32 {
                anyhow::bail!("Key len({}) is not equal to 32", key.len());
            }
            let key = H256::from_slice(&key);
            let mut data_h256 = [0u8; 32];
            data.to_big_endian(&mut data_h256);
            self.storage_data.insert(key, data_h256.into());
            Ok(())
        }
    }
}
