use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc, Mutex, TryLockError},
};

use async_channel::{Receiver, Sender};
use bevy_tasks::{ComputeTaskPool, Scope, TaskPool};
use bevy_utils::syncunsafecell::SyncUnsafeCell;
use fixedbitset::FixedBitSet;
// std once cell is not thread safe
use once_cell::sync::OnceCell;

use super::{
    shared_access::SharedSystemAccess, Conditions, ExecutorKind, MainThreadExecutor,
    SyncUnsafeSchedule, SystemExecutor, SystemSchedule,
};
use crate::{
    archetype::ArchetypeComponentId,
    query::Access,
    schedule::BoxedCondition,
    system::BoxedSystem,
    world::{unsafe_world_cell::UnsafeWorldCell, World},
};

/// struct that contains all the data needed to run a system task
#[derive(Clone)]
struct SystemTask {
    /// index of system in systems vec
    system_id: usize,
    /// channel for checking if all dependencies have finished
    dependency_finished_channel: Receiver<()>,
    /// Number of systems this system is dependent on.
    dependencies_count: u32,
    /// systems that are dependent on this one
    dependents: Vec<DependentSystem>,

    archetype_component_access: Access<ArchetypeComponentId>,

    system_set_run_conditions: Vec<ThreadSafeRunCondition>,
    // TODO, see if we can make this just a Vec<BoxedCondition>.
    // Made it a ThreadSafeRunConditino because it needed to be clone
    system_run_conditions: Vec<ThreadSafeRunCondition>,
}

impl SystemTask {
    // TODO: need to feed the system in here somehow and then send it back after done
    fn get_task<'scope, 'env: 'scope>(
        &'scope self,
        systems: &'scope [SyncUnsafeCell<BoxedSystem>],
        world_cell: UnsafeWorldCell<'scope>,
        scope: &'scope Scope<'scope, 'env, ()>,
        mut shared_access: SharedSystemAccess,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'scope>> {
        // SAFETY: this is the only task that accesses this system
        let system = unsafe { &mut *systems[self.system_id].get() };

        let task = async move {
            let mut count = 0;
            while count < self.dependencies_count {
                // wait for a dependency to complete
                self.dependency_finished_channel.recv().await.unwrap();
                count += 1;
            }

            // TODO: figure out when to call update_archetype_component_access for conditions and
            // the system
            // TODO: shared access should account for exclusive systems and non send systems
            shared_access
                .wait_for_access(&self.archetype_component_access, self.system_id)
                .await;

            // SAFETY: shared access is holding locks for the data that all the run conditions access
            if unsafe { self.should_run(world_cell) } {
                // TODO: readd panic handling or fix in scope
                // SAFETY: shared_access is holding locks for the data this system accesses
                unsafe { system.run_unsafe((), world_cell) };
            }

            // run dependencies
            let mut first_dependent = None;
            for dependent in &self.dependents {
                match dependent {
                    DependentSystem::NotificationChannel(channel) => {
                        channel.send_blocking(()).unwrap();
                    }
                    DependentSystem::System(system_task) => {
                        if first_dependent.is_none() {
                            // let task = system_task.clone();
                            first_dependent = Some(system_task.get_task(
                                systems,
                                world_cell,
                                scope,
                                shared_access.clone(),
                            ));
                        } else {
                            scope.spawn(system_task.get_task(
                                systems,
                                world_cell,
                                scope,
                                shared_access.clone(),
                            ));
                        }
                    }
                }
            }
        };
        // TODO: add instrumentation back
        // let task = task.instrument(self.system_task_span)
        Box::pin(task)
    }

    // TODO: check the safety here
    /// # Safety
    /// * `world` must have permission to read any world data required by
    ///   the system's conditions: this includes conditions for the system
    ///   itself, and conditions for any of the system's sets.
    /// * `update_archetype_component` must have been called with `world`
    ///   for each run condition in `conditions`.
    unsafe fn should_run(&self, world: UnsafeWorldCell) -> bool {
        let mut should_run = true;
        for condition in self.system_set_run_conditions.iter() {
            if !condition.can_run(world) {
                should_run = false;
            }
        }

        for mut condition in self.system_run_conditions.iter() {
            if unsafe { !condition.can_run(world) } {
                should_run = false;
            }
        }

        should_run
    }
}

#[derive(Clone)]
struct ThreadSafeRunCondition {
    result: OnceCell<bool>,
    condition: Arc<Mutex<BoxedCondition>>,
}

impl ThreadSafeRunCondition {
    /// Safety: Must have a lock on all the data in `world` that `self.condition` needs to run.
    unsafe fn can_run(&self, world: UnsafeWorldCell) -> bool {
        if let Some(result) = self.result.get() {
            *result
        } else {
            let mut condition = self.condition.lock().unwrap();
            let run = unsafe { condition.run_unsafe((), world) };
            let result = self.result.set(run);
            if let Err(actual_val) = result {
                actual_val
            } else {
                run
            }
        }
    }
}

#[derive(Clone)]
enum DependentSystem {
    // channel to notify system that dependent is done.
    NotificationChannel(Sender<()>),
    System(SystemTask),
}

struct MultiThreadedExecutor {
    shared_access: SharedSystemAccess,
    roots: Vec<SystemTask>,
}

impl SystemExecutor for MultiThreadedExecutor {
    fn kind(&self) -> ExecutorKind {
        todo!()
    }

    fn init(&mut self, schedule: &SystemSchedule) {
        todo!()
    }

    fn run(
        &mut self,
        schedule: &mut SystemSchedule,
        // TODO: Make skip_systems work correctly
        _skip_systems: Option<FixedBitSet>,
        world: &mut World,
    ) {
        let thread_executor = world
            .get_resource::<MainThreadExecutor>()
            .map(|e| e.0.clone());
        let thread_executor = thread_executor.as_deref();

        let SyncUnsafeSchedule {
            systems,
            mut conditions,
        } = SyncUnsafeSchedule::new(schedule);

        ComputeTaskPool::get_or_init(TaskPool::default).scope_with_executor(
            false,
            thread_executor,
            |scope| {
                let world_cell = world.as_unsafe_world_cell();
                for root in &self.roots {
                    scope.spawn(root.get_task(
                        systems,
                        world_cell,
                        scope,
                        self.shared_access.clone(),
                    ));
                }
            },
        );

        // if self.apply_final_deferred {
        //     // Do one final apply buffers after all systems have completed
        //     // Commands should be applied while on the scope's thread, not the executor's thread
        //     let res = apply_deferred(&self.unapplied_systems, systems, world);
        //     if let Err(payload) = res {
        //         let mut panic_payload = self.panic_payload.lock().unwrap();
        //         *panic_payload = Some(payload);
        //     }
        //     self.unapplied_systems.clear();
        //     debug_assert!(self.unapplied_systems.is_clear());
        // }
    }

    fn set_apply_final_deferred(&mut self, value: bool) {
        todo!()
    }
}
