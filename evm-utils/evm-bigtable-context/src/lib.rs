use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    ops::ControlFlow,
    sync::{Arc, Mutex},
};
pub mod bigtable;
pub mod context;
pub mod evm_schema;
pub mod memory;
pub mod serialization;
pub mod utils;

pub use serialization::*;
use snafu::{ResultExt, Snafu};

const FULL_POSTFIX: &str = "full";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Map with this column exist but type differ, column:{}, exist_type:{}, expected_type:{}",
        column,
        exist_type,
        expected_type
    ))]
    ThisColumnContainDifferMapType {
        column: String,
        exist_type: String,
        expected_type: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;
// TODO: implement for all primitive types
// TODO: Implement for tuples of primitive types

// For testing purposes use sync method, then replace to async.
pub type FutureOutput<T> = T;

// TODO:
// 1. Multimap
// 2. Errors?
pub trait AsyncMap {
    type K;
    type V;

    // Because rust didn't support GAT's this is impossible
    // to create PrefixIterator with some bound to &self lifetime.
    // type PrefixIter: Iterator<Item = Self::V>;
    // fn iter(&self, prefix: <Self::K as MultiPrefixKey>::Prefix) -> Self::PrefixIter;

    // TODO:
    // K - replace by Borrow,
    // V - replace by Cow<V>,
    fn get(&self, key: &Self::K) -> Option<Self::V>;
    fn set(&self, key: Self::K, value: Self::V);

    /// Removes elemnt from collection.
    ///
    /// Most of kvs storages didn't retrive removed element,
    /// so if you need to retain it, use `get` before.
    fn remove(&self, key: &Self::K);
}

// The same write mechanism but use the power of string ordered key, to create
// additional key with `FULL_POSTFIX` at end.
// This should allow fast iteration to this
pub trait WriteFull: AsyncMap {
    fn set_full(&self, key: Self::K, value: Self::V);
}

pub trait AsyncMapSearch: AsyncMap
where
    Self::K: MultiPrefixKey,
{
    // TODO: replace k,v by Cow;

    fn fold_prefix_rev<F, Reducer>(
        &self,
        prefix: <Self::K as MultiPrefixKey>::Prefixes,
        init: Reducer,
        mut func: F,
    ) -> Reducer
    where
        F: FnMut(Reducer, (Self::K, bool, Self::V)) -> Reducer,
        <Self::K as MultiPrefixKey>::Suffix: RangeValue,
    {
        self.search_rev(
            prefix,
            <Self::K as MultiPrefixKey>::Suffix::max(),
            None,
            false,
            init,
            |init, arg| ControlFlow::Continue(func(init, arg)),
        )
    }

    /// Try fold from back, returning all items in inclusive range
    /// (prefix, end_suffix)..=(prefix, 0);
    /// Provide fold iterator. For the next reasons:
    /// 1. Fold allows do any iterating with returning value effectively.
    /// 2. Fold can replace any other iterator
    /// 3. Because of impossibility to use "external iterator" (see comments above `type PrefixIter`).
    fn search_rev<F, Reducer>(
        &self,
        prefix: <Self::K as MultiPrefixKey>::Prefixes,
        last_suffix: <Self::K as MultiPrefixKey>::Suffix,
        first_suffix: Option<<Self::K as MultiPrefixKey>::Suffix>,
        only_full: bool,
        init: Reducer,
        func: F,
    ) -> Reducer
    where
        F: FnMut(Reducer, (Self::K, bool, Self::V)) -> ControlFlow<Reducer, Reducer>;
}

pub trait MultiMapProvider<Col> {
    // Take shared version of map that was already created.
    fn take_map_shared<Map, K, V>(&self, column: Col) -> Result<Map>
    where
        Map: AsyncMap<K = K, V = V>;

    // Take unique version of map.
    //
    // Note: Internally multi unique version of maps can reffer to single map.
    // This is depend on map implementation.
    fn take_map_unique<Map, K, V>(column: Col) -> Result<Map>
    where
        Map: AsyncMap<K = K, V = V>;
}

#[macro_export]
macro_rules! impl_trait_test_for_type {
    ($name: ident => ($($type:ty),*)) => {
        #[allow(non_snake_case)]
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            #[test]
            fn should_write_min() {
                const SIZE: usize = <($($type),*) as $crate::FixedSizedKey>::SIZE;
                const SUFFIX_SIZE: usize = <<($($type),*) as MultiPrefixKey>::Suffix as $crate::FixedSizedKey>::SIZE;
                let mut buffer = [0u8; SIZE];
                let data = <($($type),*) as $crate::RangeValue>::min();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);
                let (f, s) = buffer.split_at(SIZE - SUFFIX_SIZE);
                assert!(f.iter().all(|i| *i == 0));
                assert!(s.iter().all(|i| *i == 0xff));

                let data = <($($type),*) as $crate::RangeValue>::max();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);

                let (f, s) = buffer.split_at(SIZE - SUFFIX_SIZE);
                assert!(f.iter().all(|i| *i == 0xff));
                assert!(s.iter().all(|i| *i == 0));
            }

            #[test]
            #[should_panic]
            fn overflow_panic() {
                let mut buffer = [0u8; <($($type),*) as $crate::FixedSizedKey>::SIZE - 1];
                let data = <($($type),*) as $crate::RangeValue>::min();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer)
            }
        }
    };
    ($name: ident => $type:ty) => {
        #[allow(non_snake_case)]
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            #[test]
            fn should_write_min_max() {
                let mut buffer = [0u8; <$type as $crate::FixedSizedKey>::SIZE];
                let data = <$type as $crate::RangeValue>::min();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);

                assert!(buffer.iter().all(|i| *i == 0));

                let data = <$type as $crate::RangeValue>::max();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);

                assert!(buffer.iter().all(|i| *i == 0xff));
            }

            #[test]
            fn buffer_roundtrip() {
                let mut buffer = [0u8; <$type as $crate::FixedSizedKey>::SIZE];

                let data = <$type as $crate::RangeValue>::min();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);
                let new_data: $type = $crate::FixedSizedKey::from_buffer_ord_bytes(&buffer);
                assert_eq!(data, new_data);

                let data = <$type as $crate::RangeValue>::max();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);
                let new_data: $type = $crate::FixedSizedKey::from_buffer_ord_bytes(&buffer);
                assert_eq!(data, new_data);
            }

            #[test]
            #[should_panic]
            fn overflow_panic() {
                let mut buffer = [0u8; <$type as $crate::FixedSizedKey>::SIZE - 1];
                let data = <$type as $crate::RangeValue>::min();
                $crate::FixedSizedKey::write_ord_bytes(&data, &mut buffer);
            }


            #[test]
            #[should_panic]
            fn buffer_small() {
                let buffer = [0u8; <$type as $crate::FixedSizedKey>::SIZE - 1];
                <$type as $crate::FixedSizedKey>::from_buffer_ord_bytes(&buffer);
            }

            #[test]
            #[should_panic]
            fn buffer_big() {
                let buffer = [0u8; <$type as $crate::FixedSizedKey>::SIZE + 1];
                <$type as $crate::FixedSizedKey>::from_buffer_ord_bytes(&buffer);
            }
        }
    };
}
#[cfg(test)]
mod tests {
    use super::*;

    impl_trait_test_for_type! {test_u64 => u64}
    impl_trait_test_for_type! {test_u32 => u32}
}
