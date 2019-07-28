use crate::data::{ArconElement, ArconType, ArconVec};
use crate::error::*;
use crate::messages::protobuf::*;
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::streaming::channel::{Channel, ChannelPort};
use crate::streaming::task::{get_remote_msg, TaskMetric};
use crate::weld::*;
use kompact::*;
use std::sync::Arc;
use weld::*;

/// FlatMap task
///
/// A: Input Event
/// B: Port type for ChannelStrategy
/// C: Output Event
#[derive(ComponentDefinition)]
pub struct FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    ctx: ComponentContext<Self>,
    _in_channels: Vec<Channel<C, B, Self>>,
    out_channels: Box<ChannelStrategy<C, B, Self>>,
    pub event_port: ProvidedPort<ChannelPort<A>, Self>,
    udf: Arc<Module>,
    udf_ctx: WeldContext,
    metric: TaskMetric,
}

impl<A, B, C> FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    pub fn new(
        udf: Arc<Module>,
        in_channels: Vec<Channel<C, B, Self>>,
        out_channels: Box<ChannelStrategy<C, B, Self>>,
    ) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        FlatMap {
            ctx: ComponentContext::new(),
            event_port: ProvidedPort::new(),
            _in_channels: in_channels,
            out_channels,
            udf: udf.clone(),
            udf_ctx: ctx,
            metric: TaskMetric::new(),
        }
    }

    fn handle_event(&mut self, event: &ArconElement<A>) -> ArconResult<()> {
        if let Ok(result) = self.run_udf(&(event.data)) {
            // Result should be an ArconVec of elements
            // iterate over it and send
            for i in 0..result.len {
                let _ = self.push_out(ArconElement::new(result[i as usize]));
            }
        } else {
            // Just report the error for now...
            error!(self.ctx.log(), "Failed to execute UDF...",);
        }
        Ok(())
    }

    fn run_udf(&mut self, event: &A) -> ArconResult<ArconVec<C>> {
        let run: ModuleRun<ArconVec<C>> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }

    fn push_out(&mut self, event: ArconElement<C>) -> ArconResult<()> {
        let self_ptr = self as *const FlatMap<A, B, C>;
        let _ = self.out_channels.output(event, self_ptr, None)?;
        Ok(())
    }
}

impl<A, B, C> Provide<ControlPort> for FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A, B, C> Actor for FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconElement<A>>() {
            let _ = self.handle_event(event);
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                if let Ok(event) = get_remote_msg(msg) {
                    let _ = self.handle_event(&event);
                }
            } else {
                error!(self.ctx.log(), "Failed to deserialise StreamTaskMessage",);
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

impl<A, B, C> Require<B> for FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn handle(&mut self, _event: B::Indication) -> () {
        // ignore
    }
}

impl<A, B, C> Provide<ChannelPort<A>> for FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn handle(&mut self, event: ArconElement<A>) -> () {
        let _ = self.handle_event(&event);
    }
}

unsafe impl<A, B, C> Send for FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
}

unsafe impl<A, B, C> Sync for FlatMap<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::streaming::task::tests::*;

    #[test]
    fn flatmap_task_test_local() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || {
            let sink: TaskSink<i32> = TaskSink::new();
            sink
        });

        let channel = Channel::Local(sink_comp.actor_ref());
        let channel_strategy: Box<
            ChannelStrategy<i32, ChannelPort<i32>, FlatMap<ArconVec<i32>, ChannelPort<i32>, i32>>,
        > = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: vec[i32]| map(x, |a: i32| a + i32(5))");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let filter_task =
            system.create_and_start(move || FlatMap::new(module, Vec::new(), channel_strategy));

        let vec: Vec<i32> = vec![1, 2, 3, 4, 5];
        let arcon_vec = ArconVec::new(vec);
        let input = ArconElement::new(arcon_vec);

        let event_port = filter_task.on_definition(|c| c.event_port.share());
        system.trigger_r(input, &event_port);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        let expected: Vec<i32> = vec![6, 7, 8, 9, 10];
        assert_eq!(&comp_inspect.result, &expected);
        let _ = system.shutdown();
    }
}
