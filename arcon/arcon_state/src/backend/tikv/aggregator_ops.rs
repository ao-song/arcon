use crate::{data::Metakey, error::*, Aggregator, AggregatorOps, AggregatorState, Handle, Tikv};

// Unimplemented module

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

#[cfg(feature = "metrics")]
use crate::metrics_utils::*;

impl AggregatorOps for Tikv {
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        unimplemented!();
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result> {
        unimplemented!();
    }

    fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
        value: <A as Aggregator>::Input,
    ) -> Result<()> {
        unimplemented!();
    }
}

pub(crate) fn make_aggregator_merge<A>(aggregator: A) -> impl Clone
where
    A: Aggregator,
{
    unimplemented!();
}
