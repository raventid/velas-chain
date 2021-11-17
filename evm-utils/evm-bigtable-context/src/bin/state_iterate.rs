use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::{ensure, Result};
use dashmap::{mapref::one::Ref, DashMap};
use evm_bigtable_context::bigtable::{BigTable, BigtableProvider, DEFAULT_INSTANCE_NAME};
use evm_bigtable_context::context::{BlockInfo, ChangedAccount, EvmBigTableExecutorProvider};
use evm_bigtable_context::evm_schema::{hash_address, hash_index, EvmSchema, HashedAddress};
use evm_bigtable_context::memory::{SerializedMap, SerializedMapProvider};
use evm_state::{
    storage, Account, AccountState, BlockNum, BlockVersion, Code, Storage, TransactionAction,
    TransactionInReceipt, TransactionReceipt, H256,
};
use evm_state::{Context, EvmBackend, EvmConfig, Executor, ExitError, ExitSucceed, H160}; // simulation
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
        // during simulation cleanup balance of swapper after swap to native.
        #[structopt(long = "cleanup-after-swap")]
        cleanup_native_balance_after_swap: bool,
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

        // Push changes even if root hash is different from that we know by block number.
        #[structopt(long = "force")]
        force: bool,
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
    SerializedMap<(HashedAddress, BlockNum), Code>,
    SerializedMap<(HashedAddress, H256, BlockNum), H256>,
>;

type BigtableSchema = EvmSchema<
    Arc<BigTable<(HashedAddress, BlockNum), Account>>,
    Arc<BigTable<(HashedAddress, BlockNum), Code>>,
    Arc<BigTable<(HashedAddress, H256, BlockNum), H256>>,
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
    cleanup_native_balance_after_swap: bool,
) -> Result<BTreeMap<H160, ChangedAccount>, anyhow::Error> {
    const AMOUNT_FOR_SIMULATE: u64 = 10_000_000_000_000_000; // just big enough value for simulate to pass

    info!(
        "Simulating block: block_num = {}, state_root = {:?}",
        num, root
    );
    let mut used_gas = 0;
    let mut changes_cache = Default::default();

    if txs.is_empty() {
        return Ok(changes_cache);
    }
    let first_tx_chain_id = match &txs[0].transaction {
        TransactionInReceipt::Unsigned(t) => t.chain_id,
        TransactionInReceipt::Signed(t) => t
            .signature
            .chain_id()
            .expect("We didn't support Null at chain_id"),
    };
    let mut executor = Executor::with_config(
        EvmBackend::default(),
        Default::default(),
        EvmConfig {
            chain_id: first_tx_chain_id,
            ..Default::default()
        },
    );
    for tx in txs {
        debug!("Simulate tx = {:?}", tx);

        let (tx_hash, caller, nonce, gas_price, gas_limit, action, data, value, chain_id, unsigned) =
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
                    true,
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
                    false,
                ),
            };

        let (user_accounts) = if let TransactionAction::Call(address) = action {
            let mut meta_keys = vec![];

            // Shortcut for swap tokens to native, will add solana account to transaction.
            if address == *solana_evm_loader_program::precompiles::ETH_TO_VLX_ADDR {
                debug!("Found transferToNative transaction");
                match solana_evm_loader_program::precompiles::ETH_TO_VLX_CODE.parse_abi(&data) {
                    Ok(pk) => {
                        info!("Adding account to meta = {}", pk);

                        let user_account = RefCell::new(Default::default());
                        meta_keys.push((user_account, pk))
                    }
                    Err(e) => {
                        error!("Error in parsing abi = {}", e);
                    }
                }
            }

            meta_keys
        } else {
            vec![]
        };

        // system tx are unsigned
        if unsigned {
            // check if it native swap, then predeposit, amount, to pass transaction
            if caller == *solana_evm_loader_program::precompiles::ETH_TO_VLX_ADDR {
                let amount = value + gas_limit * gas_price;
                changes_cache
                    .entry(caller)
                    .or_insert_with(|| {
                        let account = simulator.find_last_account(caller, num).unwrap_or_default();
                        ChangedAccount {
                            nonce: account.nonce,
                            balance: account.balance,
                            ..Default::default()
                        }
                    })
                    .balance += amount;
            }
        }

        let user_accounts: Vec<_> = user_accounts
            .iter()
            .map(|(user_account, pk)| {
                solana_sdk::keyed_account::KeyedAccount::new(pk, false, user_account)
            })
            .collect();

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
        let exit_result = executor.transaction_execute_raw(
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
            solana_evm_loader_program::precompiles::simulation_entrypoint(
                true, // support precompile
                AMOUNT_FOR_SIMULATE,
                &user_accounts,
            ),
        )?;

        if cleanup_native_balance_after_swap {
            changes_cache
                .entry(*solana_evm_loader_program::precompiles::ETH_TO_VLX_ADDR)
                .or_insert_with(|| {
                    let account = simulator
                        .find_last_account(
                            *solana_evm_loader_program::precompiles::ETH_TO_VLX_ADDR,
                            num,
                        )
                        .unwrap_or_default();
                    ChangedAccount {
                        nonce: account.nonce,
                        balance: account.balance,
                        ..Default::default()
                    }
                })
                .balance = 0.into();
        }

        trace!("Simulate tx result = {:?}", exit_result);
    }
    Ok(changes_cache)
}

/// account hashed address
/// It's impossible to recover addres from hash. So save its hash in collection.

#[derive(Debug, Default)]
struct AccountChange {
    new_state: Account,
    code: Option<Code>,
    storage: HashMap<H256, H256>,
}

#[derive(Debug)]
struct CurrentState {
    accounts: DashMap<HashedAddress, Account>,
    storage: DashMap<HashedAddress, DashMap<H256, H256>>,
    codes: DashMap<H256, Code>,
    next_block: BlockNum,
}

impl CurrentState {
    // Init state from root
    fn new(next_block: BlockNum, root: H256, storage: &Storage) -> Self {
        info!(
            "Initializing state with next_block = {}, root = {:?}",
            next_block, root
        );

        let mut current_state = Self {
            next_block,
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
        mut replay_simulator: Option<&mut Simulator>,
        cleanup_native_balance_after_swap: bool,
    ) -> Option<(
        HashMap<HashedAddress, AccountChange>,
        Result<(), anyhow::Error>,
    )> {
        debug!("Requested block = {}", self.next_block);
        let header;
        let changes_by_tx: Option<Result<BTreeMap<H160, ChangedAccount>, _>> =
            if let Some(simulator) = replay_simulator.as_mut() {
                let block = bigtable_storage
                .get_evm_confirmed_full_block(self.next_block)
                .await
                .expect(
                    "No root hash found, expecting block before starting to be found in bigtable",
                );
                header = block.header;
                Some(simulate_tx(
                    *simulator,
                    block.transactions.into_iter().map(|(_, v)| v).collect(),
                    header.block_number,
                    header.state_root,
                    header.timestamp,
                    header.version,
                    cleanup_native_balance_after_swap,
                ))
            } else {
                header = bigtable_storage
                .get_evm_confirmed_block_header(self.next_block)
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
            self.next_block += 1;
        }

        // TODO: Simulate precompiles
        if let Some(simulator_changes) = changes_by_tx {
            let mut result = result.expect("Simulator succeed but changes not found"); // TODO: Can be rewrited to use simulated changes.

            let simulator = replay_simulator.unwrap();

            // TODO: changes are introduced by transaction execution, but
            // currently our simulator can't process system transaction like "Native to evm swap" correctly.
            // assert!(
            //     result.len() <= simulator_changes.len(),
            //     "Change len different"
            // );
            let simulation_result = || {
                for (acc_addr, simulator_acc) in simulator_changes? {
                    let hashed_addr = hash_address(acc_addr);
                    let (new_account, code, storage) =
                        if let Some(new_acc) = result.get(&hashed_addr) {
                            (
                                new_acc.new_state,
                                new_acc.code.clone(),
                                Some(&new_acc.storage),
                            )
                        } else {
                            // Sometimes simulation can report false positive changes (for example when balance was incremented and then decremented)
                            (
                                // search in current state, and return default if nothing found.
                                self.accounts
                                    .get(&hashed_addr)
                                    .map(|v| *v.value())
                                    .unwrap_or_default(),
                                None, // if code didn't changed didn't try to search it
                                None, // get storage later, because hashmap and dashmap have no traits in common
                            )
                        };
                    ensure!(new_account.nonce == simulator_acc.nonce, "nonce not equal");
                    ensure!(
                        new_account.balance == simulator_acc.balance,
                        "balance not equal"
                    );
                    if let Some(_) = simulator_acc.code {
                        //check only code if reported as changed by executor
                        ensure!(code == simulator_acc.code, "code not equal");
                    }

                    // assert_eq!(
                    //     state_changed_acc.storage.len()
                    //     acc.storage.len(),
                    //     "storage len not equal"
                    // );
                    for (idx, data) in simulator_acc.storage {
                        dbg!(idx);
                        let idx = hash_index(idx);
                        let storage_data =
                            if let Some(data) = storage.and_then(|s| s.get(&idx).copied()) {
                                data
                            } else {
                                // Sometimes simulation can report false positive changes for storage too
                                // (for example when balance was incremented and then decremented)
                                self.storage
                                    .get(&hashed_addr)
                                    .and_then(|s| s.get(&idx).map(|v| *v.value()))
                                    .unwrap_or_default()
                            };
                        ensure!(storage_data == data, "storage not equal");
                    }
                }
                Ok(())
            };
            let simulation_result = simulation_result();
            Some((result, simulation_result))
        } else {
            result.map(|v| (v, Ok(())))
        }
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

                    account_change.storage = evm_bigtable_context::utils::diff_map(self.storage.get(&key), storage_state.inspector.storage_data);
                }
                (key, account_change)
            })
            .collect();

        for (key, change) in &changed_accounts {
            debug!(
                "Apply change account: key={:?}, after={:?}",
                key, change.new_state,
            );
            trace!("{:?}=>{:?}", key, change.storage);
            self.accounts.insert(*key, change.new_state);
            let mut entry = self.storage.entry(*key).or_insert(DashMap::new());
            for (idx, data) in &change.storage {
                if *data == H256::zero() {
                    //don't waste storage on empty data
                    entry.remove(idx);
                } else {
                    entry.insert(*idx, *data);
                }
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
            cleanup_native_balance_after_swap,
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

            let mut state = CurrentState::new(start_block, root, &storage);
            let init_block_num = 0;
            let mut simulator = if replay_txs {
                let mut mem_provider = SerializedMapProvider::default();
                let mut simulator = Simulator::new_serialized_tmp(&mut mem_provider).unwrap();
                for ref_kv in &state.accounts {
                    let key = ref_kv.key();
                    let account = ref_kv.value();
                    let code = state.codes.get(key).map(|v| v.value().clone());
                    let storage_updates = state
                        .storage
                        .get(key)
                        .map(|v| v.value().iter().map(|v| (*v.key(), *v.value())).collect())
                        .unwrap_or_default();
                    simulator.push_account_change_hashed_full(
                        *key,
                        init_block_num,
                        account.clone(),
                        code,
                        storage_updates,
                    );
                }
                Some(simulator)
            } else {
                None
            };
            for block in start_block..=end_block {
                assert_eq!(state.next_block, block);
                match state
                    .next_block(
                        &storage,
                        &ledger_storage,
                        simulator.as_mut(),
                        cleanup_native_balance_after_swap,
                    )
                    .await
                {
                    Some((changes, e)) => {
                        debug!("Debug push account changes at block {}", block);
                        for (key, account) in changes {
                            if let Some(simulator) = &mut simulator {
                                simulator.push_account_change_hashed(
                                    key,
                                    block,
                                    account.new_state,
                                    account.code,
                                    account.storage,
                                );
                            }
                        }
                        if let Err(e) = e {
                            if dry_run {
                                error!("Error during block processing {}", e)
                            } else {
                                panic!("Error during block processing {}", e)
                            }
                        }
                    }
                    None => {
                        error!("Block {} not found in state, stopping.", block);
                        std::process::exit(1);
                    }
                }
                assert_eq!(state.next_block, block + 1);
            }
        }
        Args::FullBackup {
            evm_state_path,
            block,
            root,
            dry_run,
            force,
        } => {
            let storage = Storage::open_persistent(evm_state_path).unwrap();
            let ledger_storage = LedgerStorage::new(true, Duration::from_secs(10).into())
                .await
                .unwrap();
            if dry_run {
                info!("Dry run, do nothing after collecting keys ...");
            }

            let expected_root = {
                ledger_storage
                    .get_evm_confirmed_block_header(
                        block
                    )
                    .await
                    .expect("No root hash found, expecting block before starting to be found in bigtable")
                    .state_root
            };

            info!(
                "Trying to make snapshot with root:{:?} at block {:?}",
                root, block
            );

            if expected_root != root {
                warn!("Found that block state root, is different from one that you provide: block_root:{:?}, provided:{:?}", expected_root, root);
                if force {
                    error!("--force argument found, making snapshot anyway");
                } else {
                    std::process::exit(1)
                }
            }

            // Creating state from our root, with next_block = block + 1;
            let mut state = CurrentState::new(block + 1, root, &storage);

            let mut provider = BigtableProvider::new(DEFAULT_INSTANCE_NAME, dry_run).unwrap();
            let mut bigtable_schema = BigtableSchema::new_bigtable(&mut provider).unwrap();
            for ref_kv in &state.accounts {
                let key = ref_kv.key();
                let account = ref_kv.value();
                let code = state.codes.get(key).map(|v| v.value().clone());
                let storage_updates = state
                    .storage
                    .get(key)
                    .map(|v| v.value().iter().map(|v| (*v.key(), *v.value())).collect())
                    .unwrap_or_default();
                bigtable_schema.push_account_change_hashed_full(
                    *key,
                    block,
                    account.clone(),
                    code,
                    storage_updates,
                );
            }
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
