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
use snafu::{ResultExt, Snafu};

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
pub trait FixedSizedKey: Ord {
    const SIZE: usize;
    // TODO: replace by writter fn write_ord_bytes(&self, writer: impl Write) -> Result<usize>;
    fn write_ord_bytes(&self, buffer: &mut [u8]); // write failure is assertion

    // THIS trait is firstly implemented for abstraction over bigtable, thats why in tuple this method will reverse last element byte representation.
    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self;

    fn hex_encoded_reverse(&self) -> String {
        let mut buffer = vec![0u8; Self::SIZE];
        self.write_ord_bytes(&mut buffer);
        hex::encode(&buffer)
    }
}

// TODO: Sealed
// TODO: Implement for all tuples.
pub trait MultiPrefixKey: FixedSizedKey {
    /// Type of key without last component
    type Prefixes;
    type Suffix;
    fn rebuild(head: Self::Prefixes, tail: Self::Suffix) -> Self
    where
        Self: Sized;
}

impl<T1: FixedSizedKey, T2: FixedSizedKey> FixedSizedKey for (T1, T2) {
    const SIZE: usize = T1::SIZE + T2::SIZE;
    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        let (b1, b2) = buffer.split_at_mut(T1::SIZE);
        self.0.write_ord_bytes(b1);
        self.1.write_ord_bytes(b2);
        // reverse suffix
        for i in b2 {
            *i ^= 0xff;
        }
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let (b1, preb2) = buffer.split_at(T1::SIZE);
        let mut b2 = vec![0u8; T2::SIZE];
        for (r, i) in b2.iter_mut().zip(preb2) {
            *r = i ^ 0xff;
        }
        (
            T1::from_buffer_ord_bytes(b1),
            T2::from_buffer_ord_bytes(&b2),
        )
    }
}

impl<T1: FixedSizedKey, T2: FixedSizedKey, T3: FixedSizedKey> FixedSizedKey for (T1, T2, T3) {
    const SIZE: usize = T1::SIZE + T2::SIZE + T3::SIZE;
    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        let (b1, b2) = buffer.split_at_mut(T1::SIZE);
        self.0.write_ord_bytes(b1);
        let (b2, b3) = b2.split_at_mut(T2::SIZE);
        self.1.write_ord_bytes(b2);
        self.2.write_ord_bytes(b3);

        // reverse suffix
        for i in b3 {
            *i ^= 0xff;
        }
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let (b1, b2) = buffer.split_at(T1::SIZE);

        let (b2, preb3) = b2.split_at(T2::SIZE);
        let mut b3 = vec![0u8; T3::SIZE];
        for (r, i) in b3.iter_mut().zip(preb3) {
            *r = *i ^ 0xff;
        }
        (
            T1::from_buffer_ord_bytes(b1),
            T2::from_buffer_ord_bytes(&b2),
            T3::from_buffer_ord_bytes(&b3),
        )
    }
}

impl<T1: FixedSizedKey, T2: FixedSizedKey> MultiPrefixKey for (T1, T2) {
    type Prefixes = (T1,);
    type Suffix = T2;
    fn rebuild((head,): Self::Prefixes, tail: Self::Suffix) -> Self
    where
        Self: Sized,
    {
        (head, tail)
    }
}

impl<T1: FixedSizedKey, T2: FixedSizedKey, T3: FixedSizedKey> MultiPrefixKey for (T1, T2, T3) {
    type Prefixes = (T1, T2);
    type Suffix = T3;
    fn rebuild((head, head2): Self::Prefixes, tail: Self::Suffix) -> Self
    where
        Self: Sized,
    {
        (head, head2, tail)
    }
}

trait MultiMapProvider<Col> {
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
        F: FnMut(Reducer, (Self::K, Self::V)) -> Reducer,
        <Self::K as MultiPrefixKey>::Suffix: RangeValue,
    {
        self.search_rev(
            prefix,
            <Self::K as MultiPrefixKey>::Suffix::max(),
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
        end_suffix: <Self::K as MultiPrefixKey>::Suffix,
        init: Reducer,
        func: F,
    ) -> Reducer
    where
        F: FnMut(Reducer, (Self::K, Self::V)) -> ControlFlow<Reducer, Reducer>;
}

// TODO: macros
impl FixedSizedKey for u32 {
    const SIZE: usize = std::mem::size_of::<Self>();
    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(&self.to_be_bytes())
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(buffer);
        u32::from_be_bytes(bytes)
    }
}

///
///
///  Demo
///
///

/// Is default = min value?;
pub trait RangeValue {
    fn min() -> Self;
    fn max() -> Self;
}
impl RangeValue for u32 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
}

impl<T1: RangeValue, T2: RangeValue> RangeValue for (T1, T2) {
    fn min() -> Self {
        (T1::min(), T2::min())
    }
    fn max() -> Self {
        (T1::max(), T2::max())
    }
}
impl<T1: RangeValue, T2: RangeValue, T3: RangeValue> RangeValue for (T1, T2, T3) {
    fn min() -> Self {
        (T1::min(), T2::min(), T3::min())
    }
    fn max() -> Self {
        (T1::max(), T2::max(), T3::max())
    }
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
