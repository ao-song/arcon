use crate::{
    data::{Metakey, Value},
    error::*,
    serialization::protobuf,
    Handle, Tiered, ValueOps, ValueState,
};

#[cfg(feature = "metrics")]
use crate::metrics_utils::*;

impl ValueOps for Tiered {
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(&handle.id, &key)?;
        Ok(())
    }

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(serialized) = self.get(&handle.id, &key)? {
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn value_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        let old = if let Some(serialized) = self.get(&handle.id, &key)? {
            let value = protobuf::deserialize(&serialized)?;
            Some(value)
        } else {
            None
        };
        let serialized = protobuf::serialize(&value)?;
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());
        self.put(&handle.id, key, serialized)?;
        Ok(old)
    }

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(&value)?;
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());
        self.put(&handle.id, key, serialized)?;
        Ok(())
    }

    fn value_fast_set_by_ref<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: &T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(value)?;
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());
        self.put(&handle.id, key, serialized)?;
        Ok(())
    }
}
