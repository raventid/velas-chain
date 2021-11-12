//! Simplest implementation of AsyncMap - just BTreeMap<K,V>, where K: Ord

use crate::*;

pub type MemMap<K, V> = Arc<Mutex<BTreeMap<K, V>>>;

type SharedAnyWithDetails = (Arc<dyn Any + Send + Sync + 'static>, &'static str);
#[derive(Debug, Default)]
pub struct MemMapProvider {
    created_maps: BTreeMap<String, SharedAnyWithDetails>,
}

impl<K, V> AsyncMap for MemMap<K, V>
where
    K: Clone + Ord,
    // K: tupleops::TupleAppend<<K as MultiPrefixKey>::Prefixes, <K as MultiPrefixKey>::Suffix>,
    V: Clone,
{
    type K = K;
    type V = V;

    // type PrefixIter = Box<dyn Iterator<Item = V>>;

    fn get(&self, key: &Self::K) -> Option<Self::V> {
        let b = self.lock().unwrap();
        BTreeMap::get(&*b, key).cloned()
    }
    fn set(&self, key: Self::K, value: Self::V) {
        let mut b = self.lock().unwrap();
        BTreeMap::insert(&mut *b, key, value);
    }
    fn remove(&self, key: &Self::K) {
        let mut b = self.lock().unwrap();
        BTreeMap::remove(&mut *b, key);
    }
}

impl<K, V> AsyncMapSearch for MemMap<K, V>
where
    K: MultiPrefixKey + Clone,
    Self::K: MultiPrefixKey,
    <Self::K as MultiPrefixKey>::Suffix: RangeValue,
    <K as MultiPrefixKey>::Prefixes: Clone,
    // K: tupleops::TupleAppend<<K as MultiPrefixKey>::Prefixes, <K as MultiPrefixKey>::Suffix>,
    V: Clone,
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
        let start = <Self::K as MultiPrefixKey>::rebuild(
            prefix.clone(),
            <Self::K as MultiPrefixKey>::Suffix::min(),
        );
        let end = <Self::K as MultiPrefixKey>::rebuild(prefix, end_suffix);

        match b
            .range(start..=end)
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .try_fold(init, func)
        {
            ControlFlow::Break(breaked) => breaked,
            ControlFlow::Continue(traverse) => traverse,
        }
    }
}

impl MemMapProvider {
    pub fn take_map_shared<K: Send + Sync + 'static + Ord, V: Send + Sync + 'static>(
        &mut self,
        column: String,
    ) -> Result<MemMap<K, V>> {
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

    pub fn take_map_unique<K: Ord, V>(_column: String) -> MemMap<K, V> {
        Arc::new(Mutex::new(BTreeMap::<K, V>::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncMap;
    use super::*;

    #[test]
    fn iter() {
        let db = MemMapProvider::take_map_unique("phrase".into());

        db.set((2, 0), "!");
        db.set((2, 5), "Hi");
        db.set((2, 4), " my");
        db.set((2, 3), " name");

        db.set((2, 2), " is");

        // Push to wrong keys
        db.set((1, 0), " Max");
        db.set((1, 5), " Andrew");

        db.set((u32::MAX, u32::MAX), " Stew");

        // push name and replace
        db.set((2, 1), " Serg");
        db.set((2, 1), " Vlad");

        assert_eq!(
            db.fold_prefix_rev((2,), String::new(), |mut i, (_, v)| {
                i.push_str(v);
                i
            }),
            "Hi my name is Vlad!"
        );
    }
}
