use evm_bigtable_context::bigtable::{execute_and_handle_errors, BigtableProvider};
use evm_bigtable_context::evm_schema::*;
use evm_state::{Account, Code, H160, H256};
use log::LevelFilter;
use std::collections::HashMap;
use std::ops::RangeInclusive;
fn create_storage_from_u8<I: Iterator<Item = (u8, u8)>>(i: I) -> HashMap<H256, H256> {
    i.map(|(k, v)| (H256::repeat_byte(k), H256::repeat_byte(v)))
        .collect()
}

fn first() {
    execute_and_handle_errors(|| {
        use tokio::runtime::Runtime;

        let mut bt_provider =
            BigtableProvider::new("insert_and_find_account_bt_first", true).unwrap();
        let evm_schema = EvmSchema::new_bigtable(&mut bt_provider).unwrap();

        let account_key = H160::repeat_byte(0x37);
        let account = Account {
            nonce: 1.into(),
            balance: 2.into(),
            storage_root: H256::repeat_byte(0x2),
            code_hash: H256::repeat_byte(0x3),
        };
        let some_code = Code::new(vec![1, 2, 3, 4]);

        let block = 0;

        let storage_updates = create_storage_from_u8((0..5).into_iter().map(|i| (i, 0xff)));
        evm_schema.push_account_change(
            account_key,
            block,
            account.clone(),
            some_code.clone().into(),
            storage_updates,
        );

        let block = 1;

        let mut account_update = account.clone();
        account_update.balance = 1231.into();
        account_update.nonce = 7.into();

        let storage_updates = create_storage_from_u8((5..7).into_iter().map(|i| (i, 0xee)));
        evm_schema.push_account_change(
            account_key,
            block,
            account_update.clone(),
            None,
            storage_updates,
        );

        assert_eq!(evm_schema.find_code(account_key, block).unwrap(), some_code);

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account_update, 1..=4)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account_update, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account, 0..=0)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account, 1..=4)
            .unwrap());
    })
    .unwrap();
}

fn second() {
    execute_and_handle_errors(|| {
        use tokio::runtime::Runtime;

        let mut bt_provider =
            BigtableProvider::new("insert_and_find_account_bt_second", true).unwrap();
        let mut evm_schema = EvmSchema::new_bigtable(&mut bt_provider).unwrap();
        let account_key = H160::repeat_byte(0x37);
        let account = Account {
            nonce: 1.into(),
            balance: 2.into(),
            storage_root: H256::repeat_byte(0x2),
            code_hash: H256::repeat_byte(0x3),
        };
        let some_code = Code::new(vec![1, 2, 3, 4]);

        let some_other_code = Code::new(vec![3, 2, 3, 4]);

        let block = 0;

        let storage_updates = create_storage_from_u8((0..5).into_iter().map(|i| (i, 0xff)));
        evm_schema.push_account_change(
            account_key,
            block,
            account.clone(),
            some_code.clone().into(),
            storage_updates.clone(),
        );

        let block = 1;

        let mut account_update = account.clone();
        account_update.balance = 1231.into();
        account_update.nonce = 7.into();

        let storage_updates_snapshot =
            create_storage_from_u8((5..7).into_iter().map(|i| (i, 0xee)));
        evm_schema.push_account_change_hashed_full(
            // note that all addresses are hashed
            hash_address(account_key),
            block,
            account_update.clone(),
            some_other_code.clone().into(),
            storage_updates_snapshot
                .clone()
                .into_iter()
                .map(|(k, v)| (hash_index(k), v))
                .collect(),
        );

        let block = 7;

        let mut account_update_last = account.clone();
        account_update_last.balance = 6331.into();
        account_update_last.nonce = 7.into();

        let storage_updates_last = create_storage_from_u8((9..12).into_iter().map(|i| (i, 0x3e)));
        evm_schema.push_account_change(
            account_key,
            block,
            account_update_last.clone(),
            None,
            storage_updates_last.clone(),
        );
        // check zero block state

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_code_in_block_range(account_key, &some_code, 0..=0)
            .unwrap());

        // updated account is not available at zero block
        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account_update, 0..=0)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account_update_last, 0..=0)
            .unwrap());

        // storage updates are not available too

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_snapshot, 0..=0)
            .unwrap());

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_last, 0..=0)
            .unwrap());

        // 1st block changes

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account_update, 1..=6)
            .unwrap());

        assert!(evm_schema
            .check_code_in_block_range(account_key, &some_other_code, 1..=15)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_snapshot, 1..=20)
            .unwrap());

        // zero block updates are not actual after full backup (account replaced, storage)

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account, 1..=4)
            .unwrap());

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 1..=4)
            .unwrap());

        assert!(!evm_schema // note: in real case code is immutable
            .check_code_in_block_range(account_key, &some_code, 1..=15)
            .unwrap());

        // latest block update

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account_update_last, 7..=15)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_last, 7..=15)
            .unwrap());

        // check that storage was available only after commit
        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_last, 0..=6)
            .unwrap());

        // reapply last changes as full changes, after this code would be not availabe, and storage would be limmited available
        evm_schema.push_account_change_hashed_full(
            hash_address(account_key),
            block,
            account_update_last.clone(),
            None,
            storage_updates_last.clone(),
        );

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account_update_last, 7..=15)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_last, 7..=15)
            .unwrap());

        assert!(evm_schema // note: in real case code is immutable
            .find_code(account_key, 15)
            .is_none());

        //any storage updates since last full snapshot not known
        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_snapshot, 7..=15)
            .unwrap());

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 7..=15)
            .unwrap());
    })
    .unwrap();
}

fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(LevelFilter::Off);
    builder.parse_env("RUST_LOG");
    builder.init();
    first();
    second();
}
