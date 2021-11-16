use evm_bigtable_context::bigtable::BigtableProvider;
use evm_bigtable_context::context::{BlockInfo, ChangedAccount, EvmBigTableExecutorProvider};
use evm_bigtable_context::evm_schema::*;
use evm_state::BackendProvider;
use evm_state::{
    empty_trie_hash, evm::backend::Basic, Account, Backend, Code, Context, EvmBackend, Executor,
    ExitError, ExitReason, ExitSucceed, TransactionAction, H160, H256, U256,
};
use evm_state::{HELLO_WORLD_ABI, HELLO_WORLD_CODE, HELLO_WORLD_CODE_SAVED, HELLO_WORLD_RESULT};
use log::LevelFilter;
use rlp::RlpStream;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::ops::RangeInclusive;

fn create_storage_from_u8<I: Iterator<Item = (u8, u8)>>(i: I) -> HashMap<H256, H256> {
    i.map(|(k, v)| (H256::repeat_byte(k), H256::repeat_byte(v)))
        .collect()
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

fn name_to_key(name: &str) -> H160 {
    let hash = H256::from_slice(Keccak256::digest(name.as_bytes()).as_slice());
    hash.into()
}

fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(LevelFilter::Off);
    builder.parse_env("RUST_LOG");
    builder.init();

    let code = hex::decode(HELLO_WORLD_CODE).unwrap();
    let data = hex::decode(HELLO_WORLD_ABI).unwrap();

    let mut executor = Executor::with_config(
        EvmBackend::default(),
        Default::default(),
        Default::default(),
    );
    let gas_limit = 300000000;
    let tx_hash = H256::repeat_byte(0x00);

    let mut rlp = RlpStream::new_list(2);
    rlp.append(&name_to_key("caller"));
    rlp.append(&U256::zero());

    let hello_addr = H160::from(H256::from_slice(
        Keccak256::digest(rlp.out().as_ref()).as_slice(),
    ));

    let mut bt_provider = BigtableProvider::new("insert_and_find_account_bt_evm", true).unwrap();
    let mut evm_schema = EvmSchema::new_bigtable(&mut bt_provider).unwrap();

    // cleanup code and account state of contract 0 block
    // This is needed because bigtable persist between test runs,
    // and code is stored in immutable manner, without block_num as version.
    evm_schema.push_account_change(
        hello_addr,
        0,
        Account {
            nonce: U256::zero(),
            balance: U256::zero(),
            storage_root: H256::zero(),
            code_hash: H256::zero(),
        },
        Some(vec![].into()),
        Default::default(),
    );

    let mut used_gas = 0;
    let mut changes_cache = Default::default();
    let execution_context = EvmBigTableExecutorProvider {
        schema: &mut evm_schema,
        changes: &mut changes_cache,
        used_gas: &mut used_gas,
        block_info: BlockInfo {
            root: empty_trie_hash(),
            num: 100,
            block_version: evm_state::BlockVersion::VersionConsistentHashes,
            timestamp: 0,
        },
    };

    let exit_reason = executor
        .transaction_execute_raw(
            execution_context,
            name_to_key("caller"),
            U256::zero(),
            0.into(),
            gas_limit.into(),
            TransactionAction::Create,
            code,
            U256::zero(),
            Some(0xdead),
            tx_hash,
            noop_precompile,
        )
        .unwrap();

    assert!(matches!(
        exit_reason.exit_reason,
        ExitReason::Succeed(ExitSucceed::Returned)
    ));
    // push some block
    let block_num = 100;
    for (
        key,
        ChangedAccount {
            balance,
            nonce,
            storage,
            code,
        },
    ) in changes_cache
    {
        evm_schema.push_account_change(
            key,
            block_num,
            Account {
                nonce,
                balance,
                storage_root: H256::zero(),
                code_hash: H256::zero(),
            },
            code,
            storage,
        )
    }
    let mut changes_cache = Default::default();

    //
    // Execute with invalid block_num
    //
    {
        let execution_context = EvmBigTableExecutorProvider {
            schema: &mut evm_schema,
            changes: &mut changes_cache,
            used_gas: &mut used_gas,
            block_info: BlockInfo {
                root: empty_trie_hash(),
                num: block_num - 10,
                block_version: evm_state::BlockVersion::VersionConsistentHashes,
                timestamp: 0,
            },
        };

        let tx_hash = H256::repeat_byte(0x11);
        let exit_result = executor
            .transaction_execute_raw(
                execution_context,
                name_to_key("caller"),
                1.into(),
                0.into(),
                300000.into(),
                TransactionAction::Call(hello_addr),
                data.to_vec(),
                U256::zero(),
                Some(0xdead),
                tx_hash,
                noop_precompile,
            )
            .unwrap_err();
        assert_eq!(
            exit_result,
            evm_state::error::Error::NonceNotEqual {
                tx_nonce: 1.into(),
                state_nonce: 0.into()
            }
        );
    }

    let execution_context = EvmBigTableExecutorProvider {
        schema: &mut evm_schema,
        changes: &mut changes_cache,
        used_gas: &mut used_gas,
        block_info: BlockInfo {
            root: empty_trie_hash(),
            num: block_num + 10,
            block_version: evm_state::BlockVersion::VersionConsistentHashes,
            timestamp: 0,
        },
    };

    let tx_hash = H256::repeat_byte(0x11);
    let exit_result = executor
        .transaction_execute_raw(
            execution_context,
            name_to_key("caller"),
            1.into(),
            0.into(),
            300000.into(),
            TransactionAction::Call(hello_addr),
            data.to_vec(),
            U256::zero(),
            Some(0xdead),
            tx_hash,
            noop_precompile,
        )
        .unwrap();

    let result = hex::decode(HELLO_WORLD_RESULT).unwrap();
    match (exit_result.exit_reason, exit_result.exit_data) {
        (ExitReason::Succeed(ExitSucceed::Returned), res) if res == result => {}
        any_other => panic!("Not expected result={:?}", any_other),
    }

    let mut backend = executor.deconstruct();

    let execution_context = EvmBigTableExecutorProvider {
        schema: &mut evm_schema,
        changes: &mut changes_cache,
        used_gas: &mut used_gas,
        block_info: BlockInfo {
            root: empty_trie_hash(),
            num: block_num,
            block_version: evm_state::BlockVersion::VersionConsistentHashes,
            timestamp: 0,
        },
    };

    let mut state = execution_context.construct(
        &mut backend,
        Default::default(),
        Default::default(),
        Default::default(),
    );

    let contract_before = state.code(hello_addr);
    assert_eq!(
        &contract_before,
        &hex::decode(HELLO_WORLD_CODE_SAVED).unwrap()
    );

    assert_eq!(
        state.basic(hello_addr),
        Basic {
            nonce: 1.into(),
            balance: 0.into(),
        }
    );
    assert_eq!(
        state.basic(name_to_key("caller")),
        Basic {
            nonce: 2.into(),
            balance: 0.into(),
        }
    );
}
