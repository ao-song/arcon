#[cfg(feature = "metrics")]
use crate::metrics_utils::*;
use crate::{
    data::{Key, Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::{fixed_bytes, protobuf},
    Handle, MapOps, MapState, Tikv,
};

use tikv_client::{ColumnFamily, Error, RawClient};

use tokio::runtime::Runtime;

impl MapOps for Tikv {
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        let prefix = handle.serialize_metakeys()?;
        self.remove_prefix(&handle.id, prefix)
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
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

    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());
        self.put(&handle.id, key, serialized)?;

        Ok(())
    }

    fn map_fast_insert_by_ref<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
        value: &V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let serialized = protobuf::serialize(value)?;
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());
        self.put(&handle.id, key, serialized)?;

        Ok(())
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(&key)?;

        let old = if let Some(slice) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_written(handle.name(), slice.len() as u64, self.name.as_str());

            Some(protobuf::deserialize(&slice[..])?)
        } else {
            None
        };

        let serialized = protobuf::serialize(&value)?;
        self.put(&handle.id, key, serialized)?;

        Ok(old)
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        let cf = ColumnFamily::try_from(handle.id.as_ref()).unwrap();

        client_with_cf = self.client.with_cf(cf);

        Ok(self
            .rt
            .block_on(client_with_cf.batch_put(key_value_pairs).await?)?)
    }

    fn map_insert_all_by_ref<'a, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (&'a K, &'a V)>,
    ) -> Result<()> {
        let cf = ColumnFamily::try_from(handle.id.as_ref()).unwrap();

        client_with_cf = self.client.with_cf(cf);

        Ok(self
            .rt
            .block_on(client_with_cf.batch_put(key_value_pairs).await?)?)
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;

        let old = if let Some(slice) = self.get(&handle.id, &key)? {
            Some(protobuf::deserialize(&slice[..])?)
        } else {
            None
        };

        self.remove(&handle.id, &key)?;

        Ok(old)
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(key)?;
        self.remove(&handle.id, &key)?;

        Ok(())
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool> {
        let key = handle.serialize_metakeys_and_key(key)?;
        self.contains(&handle.id, &key)
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        unimplemented!();
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>> {
        unimplemented!();
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        unimplemented!();
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        unimplemented!();
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        unimplemented!();
    }
}
