use std::{any::Any, panic::AssertUnwindSafe};

use async_channel::Sender;
use bevy_tasks::{ComputeTaskPool, Task, TaskPool};

use crate::system::IntoSystem;
use crate::{system::System, world::unsafe_world_cell::UnsafeWorldCell};

#[cfg(feature = "trace")]
use bevy_utils::tracing::{info_span, Instrument};

// TODO: add a drop impl to system task to not drop the task until the task is finished
pub struct SystemTask {
    // task is unused and is just used to store the task and drop it once SystemTask is dropped
    _task: Task<()>,
    send_start: Sender<(UnsafeWorldCell<'static>, Box<dyn System<In = (), Out = ()>>)>,
}

pub struct SystemResult {
    pub system_index: usize,
    pub system: Option<Box<dyn System<In = (), Out = ()>>>,
    pub result: Result<(), Box<dyn Any + Send>>,
}

impl SystemTask {
    pub(crate) fn new<'a>(
        index: usize,
        is_send: bool,
        send_finish: Sender<SystemResult>,
        _name: String,
    ) -> SystemTask {
        let (send_start, recv_start) = async_channel::bounded::<(
            UnsafeWorldCell<'static>,
            Box<dyn System<In = (), Out = ()>>,
        )>(1);

        #[cfg(feature = "trace")]
        let system_span = info_span!("system", name = &_name);

        let system_future = async move {
            loop {
                let Ok((world, mut system)) = recv_start.recv().await else { break; };
                #[cfg(feature = "trace")]
                let system_guard = system_span.enter();
                let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    // SAFETY:
                    // - The caller ensures that we have permission to
                    // access the world data used by the system.
                    // - `update_archetype_component_access` has been called.
                    unsafe { system.run_unsafe((), world) };
                }));
                #[cfg(feature = "trace")]
                drop(system_guard);
                // tell the executor that the system finished
                let result = SystemResult {
                    system_index: index,
                    system: Some(system),
                    result: res,
                };
                send_finish
                    .send(result)
                    .await
                    .unwrap_or_else(|error| unreachable!("{}", error));
            }
        };

        #[cfg(feature = "trace")]
        let task_span = info_span!("system_task", name = &_name);
        #[cfg(feature = "trace")]
        let system_future = system_future.instrument(task_span);

        let task = if is_send {
            ComputeTaskPool::init(TaskPool::default).spawn(system_future)
        } else {
            // TODO: this actually needs to spawn on the main thread executor
            ComputeTaskPool::init(TaskPool::default).spawn_local(system_future)
        };

        SystemTask {
            send_start,
            _task: task,
        }
    }

    /// SAFETY: must receive system back successfully before another use of system reference or using UnsafeWorldCell exclusively
    pub unsafe fn run<'scope>(
        &self,
        world: UnsafeWorldCell<'_>,
        system: &'scope mut Box<dyn System<In = (), Out = ()>>,
    ) {
        let world: UnsafeWorldCell<'static> = unsafe { std::mem::transmute(world) };
        let system = std::mem::replace(system, Box::new(IntoSystem::into_system(|| {})));
        self.send_start.try_send((world, system)).unwrap();
    }
}
