use std::{any::Any, panic::AssertUnwindSafe};

use async_channel::{Receiver, Sender};
use bevy_tasks::{ComputeTaskPool, Task, TaskPool};

use crate::{system::System, world::unsafe_world_cell::UnsafeWorldCell};

struct SystemTask {
    system: Option<Box<dyn System<In = (), Out = ()>>>,
    task: Task<()>,
    send_start: Sender<(
        UnsafeWorldCell<'static>,
        &'static mut Box<dyn System<In = (), Out = ()>>,
    )>,
    recv_finish:
        Receiver<Result<&'static mut Box<dyn System<In = (), Out = ()>>, Box<dyn Any + Send>>>,
}

impl SystemTask {
    pub fn new(system: Box<dyn System<In = (), Out = ()>>) -> SystemTask {
        let (send_start, recv_start) = async_channel::bounded::<(
            UnsafeWorldCell<'static>,
            &'static mut Box<dyn System<In = (), Out = ()>>,
        )>(1);

        // finish channel might need to be shared between all system tasks
        let (send_finish, recv_finish) = async_channel::bounded(1);

        let system_future = async move {
            loop {
                let Ok((world, system)) = recv_start.recv().await else { break; };
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
                send_finish
                    .send(res.map(|_| system))
                    .await
                    .unwrap_or_else(|error| unreachable!("{}", error));
            }
        };

        #[cfg(feature = "trace")]
        let task_span = info_span!("system_task", name = &*system.name());
        #[cfg(feature = "trace")]
        let system_future = system_future.instrument(task_span);

        let task = ComputeTaskPool::init(TaskPool::default).spawn(system_future);

        SystemTask {
            system: Some(system),
            send_start,
            recv_finish,
            task,
        }
    }
}
