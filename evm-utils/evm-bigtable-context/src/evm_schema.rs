use sha3::{Digest, Keccak256};
use std::collections::{BTreeSet, HashMap};

// keys;
use crate::{
    bigtable::{BigTable, BigtableProvider},
    memory::{SerializedMap, SerializedMapProvider},
};
use evm_state::{BlockNum, H160, H256};

use std::ops::RangeInclusive;

//values
use super::*;
use evm_state::types::{Account, Code};
use log::*;
use memory::typed::{MemMap, MemMapProvider};

impl FixedSizedKey for H160 {
    const SIZE: usize = 20;

    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(self.as_bytes())
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        H160::from_slice(&buffer)
    }
}

impl FixedSizedKey for H256 {
    const SIZE: usize = 32;

    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(self.as_bytes())
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        H256::from_slice(&buffer)
    }
}

impl FixedSizedKey for BlockNum {
    const SIZE: usize = std::mem::size_of::<BlockNum>();

    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(&self.to_be_bytes())
    }
    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(buffer);
        BlockNum::from_be_bytes(bytes)
    }
}

impl RangeValue for BlockNum {
    fn min() -> Self {
        BlockNum::MIN
    }
    fn max() -> Self {
        BlockNum::MAX
    }
}

impl RangeValue for H160 {
    fn min() -> Self {
        H160::repeat_byte(0x00)
    }
    fn max() -> Self {
        H160::repeat_byte(0xff)
    }
}

impl RangeValue for H256 {
    fn min() -> Self {
        H256::repeat_byte(0x00)
    }
    fn max() -> Self {
        H256::repeat_byte(0xff)
    }
}

pub type HashedAddress = H256;
pub fn hash_address(address: H160) -> HashedAddress {
    H256::from_slice(Keccak256::digest(address.as_bytes()).as_slice())
}

pub fn hash_index(idx: H256) -> HashedAddress {
    H256::from_slice(Keccak256::digest(idx.as_bytes()).as_slice())
}

//
// Versioned account storage
//
// Tables:
// 1. Accounts (evm-accounts)
// H160 -> Account
//
// Primary index: H160
// Secondary index: BlockNum

//
// 2. AccountCode (evm-accounts-code)
// Immutable (H256 -> Code) or (H160 -> Code)
//
// Primary index: H160
//

// 3. AccountStorage (evm-accounts-storage)
// H160 -> (H256 -> B)
//
// Primary Index: H160
// Secondary Index: H256
// Third Index: BlockNum
//

#[derive(Debug, Clone)]
pub struct EvmSchema<AccountMap, CodeMap, StorageMap> {
    accounts: AccountMap,
    code: CodeMap,
    storage: StorageMap,
    full_backups: BTreeSet<BlockNum>,
}

impl
    EvmSchema<
        MemMap<(HashedAddress, BlockNum), Account>,
        MemMap<(HashedAddress, BlockNum), Code>,
        MemMap<(HashedAddress, H256, BlockNum), H256>,
    >
{
    pub fn new_mem_tmp(provider: &mut MemMapProvider) -> Result<Self> {
        Ok(Self {
            accounts: provider.take_map_shared(String::from("evm-accounts"))?,
            storage: provider.take_map_shared(String::from("evm-account-storage"))?,
            code: provider.take_map_shared(String::from("evm-account-code"))?,
            full_backups: BTreeSet::new(),
        })
    }
}

impl
    EvmSchema<
        SerializedMap<(HashedAddress, BlockNum), Account>,
        SerializedMap<(HashedAddress, BlockNum), Code>,
        SerializedMap<(HashedAddress, H256, BlockNum), H256>,
    >
{
    pub fn new_serialized_tmp(provider: &mut SerializedMapProvider) -> Result<Self> {
        Ok(Self {
            accounts: provider.take_map_shared(String::from("evm-accounts"))?,
            storage: provider.take_map_shared(String::from("evm-account-storage"))?,
            code: provider.take_map_shared(String::from("evm-account-code"))?,
            full_backups: provider.list_full_backups(),
        })
    }
}

impl
    EvmSchema<
        Arc<BigTable<(HashedAddress, BlockNum), Account>>,
        Arc<BigTable<(HashedAddress, BlockNum), Code>>,
        Arc<BigTable<(HashedAddress, H256, BlockNum), H256>>,
    >
{
    pub fn new_bigtable(provider: &mut BigtableProvider) -> Result<Self> {
        Ok(Self {
            accounts: provider.take_map_shared(String::from("evm-accounts"))?,
            storage: provider.take_map_shared(String::from("evm-account-storage"))?,
            code: provider.take_map_shared(String::from("evm-account-code"))?,
            full_backups: provider.list_full_backups(),
        })
    }
}

#[doc(hidden)]
impl<AccountMap, CodeMap, StorageMap> EvmSchema<AccountMap, CodeMap, StorageMap>
// declaration
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
    fn last_snapshot_since(&self, key: H256, last_block_num: BlockNum) -> Option<BlockNum> {
        self.full_backups
            .range(..last_block_num)
            .rev()
            .next()
            .copied()
    }
    pub fn find_last_account(&self, key: H160, last_block_num: BlockNum) -> Option<Account> {
        debug!(
            "Searching for account state = {:?}, since_block = {}",
            key, last_block_num
        );
        self.find_last_account_hashed(hash_address(key), last_block_num)
    }
    pub fn find_code(&self, key: H160, last_block_num: BlockNum) -> Option<Code> {
        debug!("Searching for account code = {:?}", key);
        let code = self.find_code_hashed(hash_address(key), last_block_num);
        // debug!("Searching for account code = {:?}, code = {:?}", key, code);
        code
    }
    pub fn find_storage(&self, key: H160, index: H256, last_block_num: BlockNum) -> Option<H256> {
        debug!(
            "Searching for storage = {:?}, {:?}, since_block = {}",
            key, index, last_block_num
        );
        self.find_storage_hashed(hash_address(key), hash_index(index), last_block_num)
    }
    pub fn push_account_change(
        &self,
        key: H160,
        block_num: BlockNum,
        state: Account,
        code: Option<Code>,
        storage_updates: HashMap<H256, H256>,
    ) {
        let mut new_storage = HashMap::new();
        for (k, v) in storage_updates {
            new_storage.insert(hash_index(k), v);
        }
        self.push_account_change_hashed(hash_address(key), block_num, state, code, new_storage)
    }

    pub fn find_last_account_hashed(
        &self,
        key: HashedAddress,
        last_block_num: BlockNum,
    ) -> Option<Account> {
        debug!(
            "Searching for account state = {:?}, since_block = {}",
            key, last_block_num
        );
        self.accounts.search_rev(
            (key,),
            last_block_num,
            dbg!(self.last_snapshot_since(key, last_block_num)),
            None,
            |_, ((addr, block), _is_full, account)| {
                debug_assert_eq!(key, addr);
                debug!(
                    "Found account at block = {}, account = {:?}",
                    block, account
                );
                ControlFlow::Break(Some(account))
            },
        )
    }
    pub fn find_code_hashed(&self, key: HashedAddress, last_block_num: BlockNum) -> Option<Code> {
        self.code.search_rev(
            (key,),
            last_block_num,
            self.last_snapshot_since(key, last_block_num),
            None,
            |_, ((addr, block), _is_full, code)| {
                debug_assert_eq!(key, addr);
                trace!("Found code at block = {}, code = {:?}", block, code);
                ControlFlow::Break(Some(code))
            },
        )
    }
    pub fn find_storage_hashed(
        &self,
        key: HashedAddress,
        index: H256,
        last_block_num: BlockNum,
    ) -> Option<H256> {
        debug!(
            "Searching for account storage = ({:?}, {:?}), since_block = {}",
            key, index, last_block_num
        );
        self.storage.search_rev(
            (key, index),
            last_block_num,
            dbg!(self.last_snapshot_since(key, last_block_num)),
            None,
            |_, ((addr, index_found, block), _is_full, account)| {
                debug_assert_eq!(key, addr);
                debug_assert_eq!(index, index_found);
                debug!(
                    "Found account storage at block = {}, account = {:?}",
                    block, account
                );
                ControlFlow::Break(Some(account))
            },
        )
    }
    pub fn push_account_change_hashed(
        &self,
        key: HashedAddress,
        block_num: BlockNum,
        state: Account,
        code: Option<Code>,
        storage_updates: HashMap<H256, H256>,
    ) {
        if let Some(code) = code {
            self.code.set((key, block_num), code);
        }
        for (storage_idx, storage_value) in storage_updates {
            self.storage
                .set((key, storage_idx, block_num), storage_value);
        }
        self.accounts.set((key, block_num), state);
    }

    // publish all changes
    pub fn push_account_change_hashed_full(
        &mut self,
        key: HashedAddress,
        block_num: BlockNum,
        state: Account,
        code: Option<Code>,
        storage_updates: HashMap<H256, H256>,
    ) where
        AccountMap: WriteFull,
        CodeMap: WriteFull,
        StorageMap: WriteFull,
    {
        if let Some(code) = code {
            self.code.set_full((key, block_num), code);
        }
        for (storage_idx, storage_value) in storage_updates {
            self.storage
                .set_full((key, storage_idx, block_num), storage_value);
        }
        self.accounts.set_full((key, block_num), state);
        self.full_backups.insert(block_num);
    }
}

impl<AccountMap, CodeMap, StorageMap> EvmSchema<AccountMap, CodeMap, StorageMap>
// declaration
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
    // Return None, if results of queries is different;
    // Return true if all data in rage was found;
    // Return false if all data in rage not found;
    pub fn check_account_in_block_range(
        &self,
        key: H160,
        account: &Account,
        blocks: RangeInclusive<BlockNum>,
    ) -> Option<bool> {
        blocks.into_iter().fold(None, |init, b| {
            let new_account = self.find_last_account(key, b).unwrap_or_default();
            let new_result = new_account == *account;
            init.filter(|v| *v == new_result)
                .or_else(|| Some(new_result))
        })
    }

    pub fn check_storage_in_block_range(
        &self,
        key: H160,
        storage: &HashMap<H256, H256>,
        blocks: RangeInclusive<BlockNum>,
    ) -> Option<bool> {
        blocks.into_iter().fold(None, |mut init, b| {
            for (idx, data_before) in storage {
                let data = self.find_storage(key, *idx, dbg!(b)).unwrap_or_default();
                let new_result = dbg!(data) == dbg!(*data_before);
                init = init
                    .filter(|v| *v == new_result)
                    .or_else(|| Some(new_result));
            }
            init
        })
    }

    pub fn check_code_in_block_range(
        &self,
        key: H160,
        code: &Code,
        blocks: RangeInclusive<BlockNum>,
    ) -> Option<bool> {
        blocks.into_iter().fold(None, |mut init, b| {
            let data = self.find_code(key, b).unwrap_or_default();
            let new_result = data == *code;
            init.filter(|v| *v == new_result)
                .or_else(|| Some(new_result))
        })
    }
}

#[cfg(test)]
mod test {

    use crate::memory::SerializedMapProvider;

    use super::*;

    impl_trait_test_for_type! {test_h160_key => H160}
    impl_trait_test_for_type! {test_H256_key => H256}
    impl_trait_test_for_type! {test_BlockNum_key => BlockNum}
    impl_trait_test_for_type! {test_account_key => (H160,BlockNum)}
    impl_trait_test_for_type! {test_account_key_hashed => (H256,BlockNum)}
    impl_trait_test_for_type! {test_account_storage_key => (H160,H256, BlockNum)}

    fn create_storage_from_u8<I: Iterator<Item = (u8, u8)>>(i: I) -> HashMap<H256, H256> {
        i.map(|(k, v)| (H256::repeat_byte(k), H256::repeat_byte(v)))
            .collect()
    }

    #[test]
    fn insert_and_find_account() {
        let mut mem_provider = MemMapProvider::default();
        let evm_schema = EvmSchema::new_mem_tmp(&mut mem_provider).unwrap();

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

        assert!(evm_schema
            .check_code_in_block_range(account_key, &some_code, 0..=4)
            .unwrap());
    }

    #[test]
    fn insert_and_find_account_serialized() {
        let mut mem_provider = SerializedMapProvider::default();
        let evm_schema = EvmSchema::new_serialized_tmp(&mut mem_provider).unwrap();

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
            None,
            storage_updates.clone(),
        );

        let block = 1;

        let mut account_update = account.clone();
        account_update.balance = 1231.into();
        account_update.nonce = 7.into();

        let storage_updates2 = create_storage_from_u8((5..7).into_iter().map(|i| (i, 0xee)));
        evm_schema.push_account_change(
            account_key,
            block,
            account_update.clone(),
            some_code.clone().into(),
            storage_updates2.clone(),
        );

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

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates2, 1..=4)
            .unwrap());

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates2, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 0..=0)
            .unwrap());

        // pervious storage should be saved
        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 1..=4)
            .unwrap());

        // note that in this test code inserted in 1st block.
        assert!(!evm_schema
            .check_code_in_block_range(account_key, &some_code, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_code_in_block_range(account_key, &some_code, 1..=4)
            .unwrap());
    }

    #[test]
    fn insert_and_find_account_serialized_with_full_snapshot() {
        env_logger::Builder::new().init();
        let mut mem_provider = SerializedMapProvider::default();
        let mut evm_schema = EvmSchema::new_serialized_tmp(&mut mem_provider).unwrap();

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

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates_snapshot, 7..=15)
            .unwrap());

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 7..=15)
            .unwrap());
    }
}
