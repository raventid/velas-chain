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

fn main() {
    execute_and_handle_errors(|| {
        use tokio::runtime::Runtime;

        let mut builder = env_logger::Builder::new();
        builder.filter_level(LevelFilter::Off);
        builder.parse_env("RUST_LOG");
        builder.init();

        log::info!("test");
        let mut bt_provider = BigtableProvider::new("insert_and_find_account_bt", true)
            .unwrap();
        let evm_schema = EvmSchema::new_bigtable(&mut bt_provider)
            .unwrap();

        let account_key = H160::repeat_byte(0x37);
        log::info!("test2q");

        evm_schema.find_code(account_key).unwrap_or_default();
        log::info!("test3q");
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
            some_code.clone(),
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
            some_code.clone(),
            storage_updates,
        );

        assert_eq!(evm_schema.find_code(account_key).unwrap(), some_code);

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
