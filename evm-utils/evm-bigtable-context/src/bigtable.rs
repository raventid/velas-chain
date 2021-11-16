use std::collections::BTreeSet;
use std::fmt::Debug;
use std::time::Duration;
use std::{marker::PhantomData, ops::Sub};

use super::*;
use solana_storage_bigtable::bigtable::BigTableConnection;
use solana_storage_bigtable::bigtable::Result;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
pub const DEFAULT_INSTANCE_NAME: &'static str = "solana-ledger";

#[derive(Clone)]
pub struct BigTable<K, V> {
    connection: BigTableConnection,
    table: String,
    runtime: Arc<Runtime>,
    _pk: PhantomData<K>,
    _pv: PhantomData<V>,
}

impl<K, V> BigTable<K, V> {
    fn new(connection: BigTableConnection, table: String, runtime: Arc<Runtime>) -> Self {
        Self {
            connection: connection.clone(),
            table,
            runtime: runtime,
            _pk: PhantomData,
            _pv: PhantomData,
        }
    }
}

impl<K, V> AsyncMap for Arc<BigTable<K, V>>
where
    K: Clone + FixedSizedKey,
    V: Clone + rlp::Decodable + rlp::Encodable + Default + Debug,
{
    type K = K;
    type V = V;

    fn get(&self, key: &Self::K) -> Option<Self::V> {
        self.runtime
            .block_on({
                self.connection
                    .client()
                    .get_rlp_cell(&self.table, key.hex_encoded_reverse())
            })
            .or_else(|e| match e {
                // Empty row = None
                solana_storage_bigtable::bigtable::Error::RowNotFound => Ok(None),
                e => Err(e),
            })
            .map_err(anyhow::Error::from)
            .unwrap()
    }
    fn set(&self, key: Self::K, value: Self::V) {
        self.runtime
            .block_on({
                self.connection
                    .client()
                    .put_rlp_cells(&self.table, &[(key.hex_encoded_reverse(), value)])
            })
            .map_err(anyhow::Error::from)
            .unwrap();
    }
    fn remove(&self, key: &Self::K) {
        self.set(key.clone(), Default::default())
    }
}

impl<K, V> WriteFull for Arc<BigTable<K, V>>
where
    K: Clone + FixedSizedKey,
    V: Clone + rlp::Decodable + rlp::Encodable + Default + Debug,
{
    // TODO: Split serialization and set write interface.
    fn set_full(&self, key: Self::K, value: Self::V) {
        let mut key = key.hex_encoded_reverse();
        key.push_str(crate::FULL_POSTFIX);

        self.runtime
            .block_on({
                self.connection
                    .client()
                    .put_rlp_cells(&self.table, &[(key, value)])
            })
            .map_err(anyhow::Error::from)
            .unwrap();
    }
}

impl<K, V> AsyncMapSearch for Arc<BigTable<K, V>>
where
    K: MultiPrefixKey + Clone,
    Self::K: MultiPrefixKey,
    <Self::K as MultiPrefixKey>::Suffix: RangeValue,
    <K as MultiPrefixKey>::Prefixes: Clone,
    V: Clone + rlp::Encodable + rlp::Decodable + Default + Debug,
{
    fn search_rev<F, Reducer>(
        &self,
        prefix: <Self::K as MultiPrefixKey>::Prefixes,
        last_suffix: <Self::K as MultiPrefixKey>::Suffix,
        first_suffix: Option<<Self::K as MultiPrefixKey>::Suffix>,
        init: Reducer,
        func: F,
    ) -> Reducer
    where
        F: FnMut(Reducer, (Self::K, bool, Self::V)) -> ControlFlow<Reducer, Reducer>,
    {
        const LIMIT: i64 = 10;
        // Note in bigtable we store in reverse index, so end suffix is actually starting point.
        //
        // Example key: 0xaabbcc...f1a0fffffffffffffffa
        // which correspond to some multikey: (0xaabbcc...f1a0, 0x5) (0x5 is stored in reversed format = fffffffffffffffa)
        //
        let start = K::rebuild(prefix.clone(), last_suffix).hex_encoded_reverse();
        let mut end = K::rebuild(
            prefix.clone(),
            first_suffix.unwrap_or(<Self::K as MultiPrefixKey>::Suffix::min()),
        )
        .hex_encoded_reverse();
        end.push_str(crate::FULL_POSTFIX);

        let result = self
            .runtime
            .block_on(self.connection.client().get_row_data(
                &self.table,
                Some(start),
                Some(end),
                LIMIT,
            ))
            .map_err(anyhow::Error::from)
            .unwrap();
        match result
            .into_iter()
            .flat_map(|(k, v)| v.into_iter().map(move |(c, v)| (k.clone(), (c, v))))
            .filter_map(|(mut k, (c, v))| {
                if c == "rlp" {
                    let (hex_key, full) = if k.ends_with(crate::FULL_POSTFIX) {
                        (
                            hex::decode(&k[..k.len() - crate::FULL_POSTFIX.len()])
                                .map_err(anyhow::Error::from)
                                .unwrap(),
                            true,
                        )
                    } else {
                        (hex::decode(&k).map_err(anyhow::Error::from).unwrap(), false)
                    };
                    let key = Self::K::from_buffer_ord_bytes(&hex_key);
                    Some((
                        key,
                        full,
                        rlp::decode(
                            &solana_storage_bigtable::compression::decompress(&*v)
                                .map_err(anyhow::Error::from)
                                .unwrap(),
                        )
                        .map_err(anyhow::Error::from)
                        .unwrap(),
                    ))
                } else {
                    None
                }
            })
            .try_fold(init, func)
        {
            ControlFlow::Break(breaked) => breaked,
            ControlFlow::Continue(traverse) => traverse,
        }
        //todo: Change limit
    }
}

type SharedAnyWithDetails = (Arc<dyn Any + Send + Sync + 'static>, &'static str);

pub struct BigtableProvider {
    connection: BigTableConnection,
    runtime: Arc<Runtime>,
    created_maps: BTreeMap<String, SharedAnyWithDetails>,
}

impl BigtableProvider {
    pub fn new(instance_name: &str, read_only: bool) -> Result<Self> {
        let runtime = Runtime::new()?;
        let connection = runtime.block_on(BigTableConnection::new(
            instance_name,
            read_only,
            None, //Some(Duration::from_secs(5)),
        ))?;
        Ok(Self {
            runtime: Arc::new(runtime),
            connection,
            created_maps: BTreeMap::new(),
        })
    }

    pub fn take_map_shared<K: Send + Sync + 'static + Ord, V: Send + Sync + 'static>(
        &mut self,
        column: String,
    ) -> std::result::Result<Arc<BigTable<K, V>>, crate::Error> {
        let type_name = std::any::type_name::<(K, V)>();
        match self.created_maps.entry(column.clone()) {
            Entry::Vacant(v) => {
                let map = Arc::new(BigTable::new(
                    self.connection.clone(),
                    column,
                    self.runtime.clone(),
                ));
                let any_map = map.clone() as Arc<dyn Any + Send + Sync + 'static>;
                v.insert((any_map, type_name));
                Ok(map)
            }
            Entry::Occupied(o) => {
                o.get()
                    .0
                    .clone()
                    .downcast()
                    .map_err(|_| Error::ThisColumnContainDifferMapType {
                        column,
                        expected_type: String::from(type_name),
                        exist_type: String::from(o.get().1),
                    })
            }
        }
    }

    pub fn list_full_backups(&self) -> BTreeSet<u64> {
        BTreeSet::new()
    }
}

pub fn execute_and_handle_errors<F, T>(func: F) -> std::result::Result<T, anyhow::Error>
where
    F: FnMut() -> T + std::panic::UnwindSafe,
{
    match std::panic::catch_unwind(func) {
        Ok(t) => Ok(t),
        Err(e) => match e.downcast::<anyhow::Error>() {
            Ok(e) => Err(*e),
            Err(e) => {
                if let Ok(e) = e.downcast::<String>() {
                    anyhow::bail!(*e.clone())
                } else {
                    anyhow::bail!("Error in converting bigtable error to anyhow.")
                }
            }
        },
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    // This is the way, how rocksdb\bigtable ord their values.
    #[test]
    fn lexigoraphical_sort_example() {
        assert!("abc" < "abd"); // 'd' > 'c'
        assert!("abc" < "abce"); // length different
        let data: BTreeSet<_> = ["fff", "ffe", "ffd", "ffefull"].iter().copied().collect();
        let result: Vec<&str> = data.range("ffe"..="fff").copied().collect();

        // lexicographical order iterator from some ffe to fff should return full version of ffe after ffe.
        assert_eq!(result, ["ffe", "ffefull", "fff"])
    }
}
