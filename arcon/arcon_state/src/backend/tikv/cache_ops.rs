#[cfg(feature = "metrics")]
use crate::metrics_utils::*;
use crate::{
    data::{Key, Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::protobuf,
    CacheOps, Handle, MapOps, MapState, Tikv,
};

use std::{borrow::BorrowMut, collections::HashMap, convert::TryInto};

impl CacheOps for Tikv {
    fn hashmap_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<(V, bool)>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let mut map = self.cache.hash.borrow_mut();
        let cache_size = self.cache.size;

        if let Some(serialized) = map.get(&key) {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some((value, true)))
        } else if let Some(serialized1) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized1.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized1)?;
            if map.len() >= cache_size.try_into().unwrap() {
                // If it is full, remove the first element to make space
                if let Some((k, v)) = map.clone().iter().next() {
                    map.remove(k);
                }
                // let mut tmp = Vec::new();
                // for (k, v) in map.iter() {
                //     tmp.push((k.to_owned(), v.to_owned()));
                // }
                // self.batch_put(&handle.id, tmp);
                // map.clear();
            }
            map.insert(key, serialized1);
            Ok(Some((value, false)))
        } else {
            // println!("Not found! {:?}", key);
            Ok(None)
        }
    }

    fn hashmap_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        let mut map = self.cache.hash.borrow_mut();
        let cache_size = self.cache.size;

        // if map.contains_key(&key) {
        //     map.insert(key, serialized);
        // } else {
        //     if map.len() >= cache_size.try_into().unwrap() {
        //         for (k, v) in map.iter() {
        //             self.put(&handle.id, k, v);
        //         }
        //         map.clear();
        //     }
        //     map.insert(key, serialized);
        // }
        map.remove(&key);
        self.put(&handle.id, key, serialized);


        Ok(())
    }

    fn lru_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<(V, bool)>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let mut map = self.cache.lru.borrow_mut();
        let cache_size = self.cache.size;

        if let Some(serialized) = map.get(&key) {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some((value, true)))
        } else if let Some(serialized1) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized1.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized1)?;
            // if map.len() >= cache_size.try_into().unwrap() {
            //     // If it is full, remove the first element to make space
            //     map.pop_lru();
            //     // // for (k, v) in map.iter() {
            //     // //     self.put(&handle.id, k, v);
            //     // // }
            //     // let mut tmp = Vec::new();
            //     // for (k, v) in map.iter() {
            //     //     tmp.push((k.to_owned(), v.to_owned()));
            //     // }
            //     // self.batch_put(&handle.id, tmp);
            //     // map.clear();
            // }
            map.put(key, serialized1);
            Ok(Some((value, false)))
        } else {
            // println!("Not found! {:?}", key);
            Ok(None)
        }
    }

    fn lru_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        let mut map = self.cache.lru.borrow_mut();
        let cache_size = self.cache.size;

        // if map.contains(&key) {
        //     map.put(key, serialized);
        // } else {
        //     if map.len() >= cache_size.try_into().unwrap() {
        //         for (k, v) in map.iter() {
        //             self.put(&handle.id, k, v);
        //         }
        //         map.clear();
        //     }
        //     map.put(key, serialized);
        // }
        map.pop(&key);
        self.put(&handle.id, key, serialized);

        Ok(())
    }

    fn tiny_lfu_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<(V, bool)>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let mut map = self.cache.tinylfu.borrow_mut();
        let cache_size = self.cache.size;

        if let Some(serialized) = map.get(&key) {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some((value, true)))
        } else if let Some(serialized1) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized1.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized1)?;
            // if map.len() >= cache_size.try_into().unwrap() {

            //     // // for (k, v) in map.iter() {
            //     // //     self.put(&handle.id, k, v);
            //     // // }
            //     // let mut tmp = Vec::new();
            //     // for (k, v) in map.iter() {
            //     //     tmp.push((k.to_owned(), v.to_owned()));
            //     // }
            //     // self.batch_put(&handle.id, tmp);
            //     // map.clear();
            // }
            map.insert(key, serialized1);
            Ok(Some((value, false)))
        } else {
            // println!("Not found! {:?}", key);
            Ok(None)
        }
    }

    fn tiny_lfu_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        let mut map = self.cache.tinylfu.borrow_mut();
        let cache_size = self.cache.size;

        // if map.contains(&key) {
        //     map.insert(key, serialized);
        // } else {
        //     if map.len() >= cache_size.try_into().unwrap() {
        //         for (k, v) in map.iter() {
        //             self.put(&handle.id, k, v);
        //         }
        //         map.clear();
        //     }
        //     map.insert(key, serialized);
        // }

        map.remove(&key);
        self.put(&handle.id, key, serialized);

        Ok(())
    }

    fn hybrid_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<(V, bool)>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let mut map = self.cache.lru.borrow_mut();
        let cache_size = self.cache.size;

        if let Some(serialized) = map.get(&key) {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some((value, true)))
        } else if let Some(serialized1) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized1.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized1)?;
            if map.len() >= cache_size.try_into().unwrap() {
                if let Some((K_POP, V_POP)) = map.pop_lru() {
                    self.put(&handle.id, K_POP, V_POP);
                }
                // // for (k, v) in map.iter() {
                // //     self.put(&handle.id, k, v);
                // // }
                // let mut tmp = Vec::new();
                // for (k, v) in map.iter() {
                //     tmp.push((k.to_owned(), v.to_owned()));
                // }
                // self.batch_put(&handle.id, tmp);
                // map.clear();
            }
            map.put(key, serialized1);
            Ok(Some((value, false)))
        } else {
            // println!("Not found! {:?}", key);
            Ok(None)
        }
    }

    fn hybrid_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        let mut map = self.cache.lru.borrow_mut();
        let cache_size = self.cache.size;

        if map.contains(&key) {
            map.put(key, serialized);
        } else {
            if map.len() >= cache_size.try_into().unwrap() {
                for (k, v) in map.iter() {
                    self.put(&handle.id, k, v);
                }
                map.clear();
            }
            map.put(key, serialized);
        }

        Ok(())
    }
}
