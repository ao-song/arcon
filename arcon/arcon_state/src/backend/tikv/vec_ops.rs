use crate::{
    data::{Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::{fixed_bytes, fixed_bytes::FixedBytes, protobuf},
    Handle, Tikv, VecOps, VecState,
};
use std::{iter, mem};

// Unimplemented module

#[cfg(feature = "metrics")]
use crate::metrics_utils::*;

impl VecOps for Tikv {
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        unimplemented!();
    }

    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        unimplemented!();
    }

    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        unimplemented!();
    }

    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        unimplemented!();
    }

    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        value: Vec<T>,
    ) -> Result<()> {
        unimplemented!();
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        unimplemented!();
    }

    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        unimplemented!();
    }

    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool> {
        unimplemented!();
    }
}

pub(crate) fn vec_merge(
    _key: &[u8],
    first: Option<&[u8]>,
    rest: &mut MergeOperands,
) -> Option<Vec<u8>> {
    unimplemented!();
}