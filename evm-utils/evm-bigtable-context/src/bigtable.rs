use std::{marker::PhantomData, ops::Sub};

use super::*;
use solana_storage_bigtable::bigtable::BigTableConnection;
use solana_storage_bigtable::bigtable::Result;
use tokio::runtime::Runtime;

pub struct BigTable<K, V> {
    connection: BigTableConnection,
    table: String,
    runtime: Runtime,
    _pk: PhantomData<K>,
    _pv: PhantomData<V>,
}

impl<K, V> BigTable<K, V> {
    async fn new(instance_name: &str, table: String) -> Result<Self> {
        Ok(Self {
            connection: BigTableConnection::new(instance_name, false, None).await?,
            table,
            runtime: Runtime::new()?,
            _pk: PhantomData,
            _pv: PhantomData,
        })
    }
}

impl<K, V> AsyncMap for BigTable<K, V>
where
    K: Clone + Ord + FixedSizedKey,
    // K: tupleops::TupleAppend<<K as MultiPrefixKey>::Prefixes, <K as MultiPrefixKey>::Suffix>,
    V: Clone + rlp::Decodable + rlp::Encodable + Default,
{
    type K = K;
    type V = V;

    // type PrefixIter = Box<dyn Iterator<Item = V>>;

    fn get(&self, key: &Self::K) -> Option<Self::V> {
        self.runtime
            .block_on({
                self.connection
                    .client()
                    .get_rlp_cell(&self.table, key.hex_encoded_reverse())
            })
            .unwrap_or(None)
    }
    fn set(&self, key: Self::K, value: Self::V) {
        self.runtime
            .block_on({
                self.connection
                    .client()
                    .put_rlp_cells(&self.table, &[(key.hex_encoded_reverse(), value)])
            })
            .unwrap();
    }
    fn remove(&self, key: &Self::K) {
        self.set(key.clone(), Default::default())
    }
}

impl<K, V> AsyncMapSearch for BigTable<K, V>
where
    K: MultiPrefixKey + Clone,
    Self::K: MultiPrefixKey,
    <Self::K as MultiPrefixKey>::Suffix: RangeValue,
    <K as MultiPrefixKey>::Prefixes: Clone,
    // K: tupleops::TupleAppend<<K as MultiPrefixKey>::Prefixes, <K as MultiPrefixKey>::Suffix>,
    V: Clone + rlp::Encodable + rlp::Decodable + Default,
{
    fn search_rev<F, Reducer>(
        &self,
        prefix: <Self::K as MultiPrefixKey>::Prefixes,
        end_suffix: <Self::K as MultiPrefixKey>::Suffix,
        init: Reducer,
        func: F,
    ) -> Reducer
    where
        F: FnMut(Reducer, (Self::K, Self::V)) -> ControlFlow<Reducer, Reducer>,
    {
        const LIMIT: i64 = 1000;
        // Note in bigtable we store in reverse index, so end suffix is actually starting point.
        //
        // Example key: 0xaabbcc...f1a0fffffffffffffffa
        // which correspond to some multikey: (0xaabbcc...f1a0, 0x5) (0x5 is stored in reversed format = fffffffffffffffa)
        //
        let start = K::rebuild(prefix.clone(), end_suffix);
        let end = K::rebuild(prefix.clone(), <Self::K as MultiPrefixKey>::Suffix::min());

        let result = self
            .runtime
            .block_on(self.connection.client().get_row_data(
                &self.table,
                Some(start.hex_encoded_reverse()),
                Some(end.hex_encoded_reverse()),
                LIMIT,
            ))
            .unwrap();
        match result
            .into_iter()
            .flat_map(|(k, v)| v.into_iter().map(move |(c, v)| (k.clone(), (c, v))))
            .filter_map(|(k, (c, v))| {
                if c == "rlp" {
                    let key = todo!();
                    Some((key, rlp::decode(&mut &*v).unwrap()))
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
