use dashmap::{mapref::one::Ref, DashMap};
use evm_state::H256;
use std::collections::HashMap;
/// Return map that represent differences of two dashmaps,
/// H256::zero() if data was removed.
/// Need O(N+M) time, and O(N+M) memory
pub fn diff_map(
    ext_data: Option<Ref<H256, DashMap<H256, H256>>>,
    mut changed_store: DashMap<H256, H256>,
) -> HashMap<H256, H256> {
    use std::str::FromStr;
    // iterate over existing storage, to check if some fields was cleared.
    let mut ext_data: HashMap<H256, H256> = if let Some(ext_data) = ext_data {
        ext_data
            .value()
            .iter()
            .filter_map(|r| {
                let idx = r.key();
                let data = r.value();
                let (_, new_data) = changed_store.remove(idx).unwrap_or_default(); // if none - then changed_store contain 0x00.00 at this place
                if *data != new_data {
                    return Some((*idx, new_data));
                }
                None
            })
            .collect()
    } else {
        Default::default()
    };
    // iterate over remaining data and insert it if changed
    for (idx, new_data) in changed_store {
        let result = ext_data.insert(idx, new_data).is_none();
        debug_assert!(result);
    }
    ext_data
}

#[cfg(test)]
mod test {
    use super::*;

    fn create_storage_from_u8<I: Iterator<Item = (u8, u8)>>(i: I) -> DashMap<H256, H256> {
        i.map(|(k, v)| (H256::repeat_byte(k), H256::repeat_byte(v)))
            .collect()
    }

    fn do_check_diff(
        map: DashMap<H256, H256>,
        changed_store: DashMap<H256, H256>,
        result: HashMap<H256, H256>,
    ) {
        let map_for_ref = DashMap::new();
        map_for_ref.insert(H256::zero(), map);
        assert_eq!(
            diff_map(map_for_ref.get(&H256::zero()), changed_store),
            result
        )
    }
    #[test]
    fn check_diff() {
        let mut source = create_storage_from_u8((0..5).into_iter().map(|i| (i, 0xff)));
        // source should be tolerate to 0x00 in state
        source.insert(H256::repeat_byte(0x5), H256::zero());

        let mut dst = create_storage_from_u8((11..13).into_iter().map(|i| (i, 0xef)));
        dst.insert(H256::repeat_byte(0x1), H256::repeat_byte(0x66));
        dst.insert(H256::repeat_byte(0x4), H256::repeat_byte(0xff));

        // mark all keys that was in source to be removed (0x00..00)
        let mut result: HashMap<_, _> = source.iter().map(|r| (*r.key(), H256::zero())).collect();
        // then add all keys that was in dst
        result.extend(dst.clone().into_iter());

        // remove from diff key that didn't change
        result.remove(&H256::repeat_byte(0x4));

        // remove from diff key that was empty, and remain empty
        result.remove(&H256::repeat_byte(0x5));
        do_check_diff(source, dst, result)
    }
}
