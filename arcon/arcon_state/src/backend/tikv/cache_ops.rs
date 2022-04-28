#[cfg(feature = "metrics")]
use crate::metrics_utils::*;
use crate::{
    data::{Key, Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::protobuf,
    Handle, MapOps, MapState, Tikv,
    CacheOps,
};

use std::collections::HashMap;

impl CacheOps for Tikv {
    fn hashmap_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let mut map = self.cache.hash;
        let cache_size = self.cache.size;

        if let Some(serialized) = map.get(&key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else if let Some(serialized) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            if map.len() >= cache_size {
                for (k, v) in map.iter() {
                    self.put(&handle.id, k, v);
                }
                map.clear();
            }
            map.insert(key, serialized);
            Ok(Some(value))
        } else {
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
        let mut map = self.cache.hash;
        let cache_size = self.cache.size;

        if map.contains_key(&key) {
            map.insert(key, serialized);
        } else {
            if map.len() >= cache_size {
                for (k, v) in map.iter() {
                    self.put(&handle.id, k, v);
                }
                map.clear();
            }
            map.insert(key, serialized);
        }

        Ok(())
    }

    fn lru_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(serialized) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn tiny_lfu_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(serialized) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn hybrid_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(serialized) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}
