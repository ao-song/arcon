#[cfg(feature = "metrics")]
use crate::metrics_utils::*;
use crate::{
    data::{Metakey, Value},
    error::*,
    serialization::protobuf,
    Handle, Reducer, ReducerOps, ReducerState, Tikv,
};

// Unimplemented module

impl ReducerOps for Tikv {
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        unimplemented!();
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        unimplemented!();
    }

    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()> {
        unimplemented!();
    }
}

pub fn make_reducer_merge<T, F>(reduce_fn: F) -> impl Clone
where
    F: Reducer<T>,
    T: Value,
{
    unimplemented!();
}
