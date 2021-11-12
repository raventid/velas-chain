//! Same implementation like in typed.rs, but instead store in BTreeMap<String, Vec<u8>>
//! Data is serialized using rlp
//! String is hex encoded with reverse order. (like in bigtable implementation).

use std::{fmt::Debug, marker::PhantomData};

use crate::*;
#[derive(Debug)]
pub struct SerializedMapInner<K, V> {
    map: BTreeMap<String, Vec<u8>>,
    _pk: PhantomData<K>,
    _pv: PhantomData<V>,
}

pub type SerializedMap<K, V> = Arc<Mutex<SerializedMapInner<K, V>>>;

impl<K, V> Default for SerializedMapInner<K, V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            _pk: PhantomData,
            _pv: PhantomData,
        }
    }
}

impl<K, V> Clone for SerializedMapInner<K, V> {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            _pk: PhantomData,
            _pv: PhantomData,
        }
    }
}

type SharedAnyWithDetails = (Arc<dyn Any + Send + Sync + 'static>, &'static str);
#[derive(Debug, Default)]
pub struct SerializedMapProvider {
    created_maps: BTreeMap<String, SharedAnyWithDetails>,
}

impl<K, V> AsyncMap for SerializedMap<K, V>
where
    K: Clone + FixedSizedKey,
    V: Clone + rlp::Decodable + rlp::Encodable + Default + Debug,
{
    type K = K;
    type V = V;

    // type PrefixIter = Box<dyn Iterator<Item = V>>;

    fn get(&self, key: &Self::K) -> Option<Self::V> {
        let b = self.lock().unwrap();
        BTreeMap::get(&b.map, &key.hex_encoded_reverse()).map(|d| rlp::decode(&*d).unwrap())
    }
    fn set(&self, key: Self::K, value: Self::V) {
        let value = rlp::encode(&value).to_vec();
        let mut b = self.lock().unwrap();
        BTreeMap::insert(&mut b.map, key.hex_encoded_reverse(), value);
    }
    fn remove(&self, key: &Self::K) {
        let mut b = self.lock().unwrap();
        BTreeMap::remove(&mut b.map, &key.hex_encoded_reverse());
    }
}

impl<K, V> AsyncMapSearch for SerializedMap<K, V>
where
    K: MultiPrefixKey + Clone,
    Self::K: MultiPrefixKey,
    <K as MultiPrefixKey>::Prefixes: Clone,
    <K as MultiPrefixKey>::Suffix: RangeValue,
    V: Clone + rlp::Decodable + rlp::Encodable + Default + Debug,
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
        let b = self.lock().unwrap();

        // Start and end is stored in reverse ordered format, thats why range is reversed too.
        let start = K::rebuild(prefix.clone(), end_suffix).hex_encoded_reverse();
        let end = K::rebuild(prefix.clone(), <Self::K as MultiPrefixKey>::Suffix::min())
            .hex_encoded_reverse();
        match b
            .map
            .range(start..=end)
            .map(|(k, v)| {
                let hex_key = hex::decode(&k).map_err(anyhow::Error::from).unwrap();
                let key = Self::K::from_buffer_ord_bytes(&hex_key);
                (key, rlp::decode(&*v).map_err(anyhow::Error::from).unwrap())
            })
            .try_fold(init, func)
        {
            ControlFlow::Break(breaked) => breaked,
            ControlFlow::Continue(traverse) => traverse,
        }
    }
}

impl SerializedMapProvider {
    pub fn take_map_shared<
        K: Send + Sync + 'static + Ord + Default,
        V: Send + Sync + 'static + Default,
    >(
        &mut self,
        column: String,
    ) -> Result<SerializedMap<K, V>> {
        let type_name = std::any::type_name::<(K, V)>();
        match self.created_maps.entry(column.clone()) {
            Entry::Vacant(v) => {
                let map = Self::take_map_unique(column);
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

    pub fn take_map_unique<K: Ord + Default, V: Default>(_column: String) -> SerializedMap<K, V> {
        Default::default()
    }
}

#[cfg(test)]
mod tests {
    use rlp::Decodable;

    use super::*;
    use crate::AsyncMap;

    #[test]
    fn iter() {
        let db = SerializedMapProvider::take_map_unique("phrase".into());

        db.set((2, 0), String::from("!"));
        db.set((2, 5), String::from("Hi"));
        db.set((2, 4), String::from(" my"));
        db.set((2, 3), String::from(" name"));

        db.set((2, 2), String::from(" is"));

        // Push to wrong keys
        db.set((1, 0), String::from(" Max"));
        db.set((1, 5), String::from(" Andrew"));

        db.set((u32::MAX, u32::MAX), String::from(" Stew"));

        // push name and replace
        db.set((2, 1), String::from(" Serg"));
        db.set((2, 1), String::from(" Vlad"));

        assert_eq!(
            // give all data with key (2,_)
            db.fold_prefix_rev((2,), String::new(), |mut i, (_, v)| {
                i.push_str(&v);
                i
            }),
            "Hi my name is Vlad!"
        );
    }
}
