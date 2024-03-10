use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use bevy_tasks::{ComputeTaskPool, Scope, TaskPool};
use bevy_utils::syncunsafecell::SyncUnsafeCell;
use fixedbitset::FixedBitSet;
// std once cell is not thread safe
use once_cell::sync::OnceCell;

use super::{
    shared_access::SharedSystemAccess, ExecutorKind, MainThreadExecutor, SyncUnsafeSchedule,
    SystemExecutor, SystemSchedule,
};
use crate::{
    archetype::ArchetypeComponentId,
    query::Access,
    schedule::{BoxedCondition, NodeId},
    system::BoxedSystem,
    world::{unsafe_world_cell::UnsafeWorldCell, World},
};


/// References to data required by the executor.
/// This is copied to each system task so that can invoke the executor when they complete.
// These all need to outlive 'scope in order to be sent to new tasks,
// and keeping them all in a struct means we can use lifetime elision.
#[derive(Copy, Clone)]
struct Context<'scope, 'env, 'sys> {
    environment: &'env Environment<'env, 'sys>,
    scope: &'scope Scope<'scope, 'env, ()>,
}


/// Borrowed data used by the [`MultiThreadedExecutor`].
struct Environment<'env, 'sys> {
    executor: &'env MultiThreadedExecutor,
    systems: &'sys [SyncUnsafeCell<BoxedSystem>],
    conditions: Mutex<Conditions<'sys>>,
    world_cell: UnsafeWorldCell<'env>,
}

impl<'env, 'sys> Environment<'env, 'sys> {
    fn new(
        executor: &'env MultiThreadedExecutor,
        schedule: &'sys mut SystemSchedule,
        world: &'env mut World,
    ) -> Self {
        Environment {
            executor,
            systems: SyncUnsafeCell::from_mut(schedule.systems.as_mut_slice()).as_slice_of_cells(),
            conditions: Mutex::new(Conditions {
                system_conditions: &mut schedule.system_conditions,
                set_conditions: &mut schedule.set_conditions,
                sets_with_conditions_of_systems: &schedule.sets_with_conditions_of_systems,
                systems_in_sets_with_conditions: &schedule.systems_in_sets_with_conditions,
            }),
            world_cell: world.as_unsafe_world_cell(),
        }
    }
}

struct Conditions<'a> {
    system_conditions: &'a mut [Vec<BoxedCondition>],
    set_conditions: &'a mut [Vec<BoxedCondition>],
    sets_with_conditions_of_systems: &'a [FixedBitSet],
    systems_in_sets_with_conditions: &'a [FixedBitSet],
}

/// struct that contains all the data needed to run a system task
#[derive(Clone)]
struct SystemTask {
    /// index of system in systems vec
    system_id: usize,
    /// channel for checking if all dependencies have finished
    dependency_finished_channel: Receiver<()>,
    /// Number of systems this system is dependent on.
    dependencies_count: usize,
    /// systems that are dependent on this one
    dependents: Vec<DependentSystem>,

    is_send: bool,
    is_exclusive: bool,

    // archetype_component_access: Access<ArchetypeComponentId>,

    // system_set_run_conditions: Vec<ThreadSafeRunCondition>,
    // // TODO, see if we can make this just a Vec<BoxedCondition>.
    // // Made it a ThreadSafeRunConditino because it needed to be clone
    // system_run_conditions: Vec<ThreadSafeRunCondition>,
}

impl SystemTask {
    /// buil
    fn new(
        system_id: usize,
        assigned_systems: &mut FixedBitSet,
        schedule: &SystemSchedule,
        finish_receive_channels: &Vec<Receiver<()>>,
        finish_send_channels: &Vec<Sender<()>>,
    ) -> Self {
        assigned_systems.set(system_id, true);

        let dependents = &schedule.system_dependents[system_id];
        // take the system if available, or use the channel if not
        let dependents = dependents
            .iter()
            .map(|d| {
                if assigned_systems.contains(*d) {
                    DependentSystem::NotificationChannel(finish_send_channels[*d].clone())
                } else {
                    DependentSystem::System(SystemTask::new(
                        *d,
                        assigned_systems,
                        schedule,
                        finish_receive_channels,
                        finish_send_channels,
                    ))
                }
            })
            .collect();

        Self {
            system_id,
            dependency_finished_channel: finish_receive_channels[system_id].clone(),
            dependencies_count: schedule.system_dependencies[system_id],
            dependents,
            is_send: schedule.systems[system_id].is_send(),
            is_exclusive: schedule.systems[system_id].is_exclusive(),
        }
    }

    fn get_task<'scope, 'env: 'scope, 'sys>(
        &'scope self,
        context: &'scope Context<'scope, 'env, 'sys>,
        mut shared_access: SharedSystemAccess,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'scope>> {
        let task = async move {
            let mut count = 0;
            while count < self.dependencies_count {
                // wait for a dependency to complete
                self.dependency_finished_channel.recv().await.unwrap();
                count += 1;
            }

            // SAFETY: this is the only task that accesses this system
            let system = unsafe { &mut *context.environment.systems[self.system_id].get() };
            system.update_archetype_component_access(context.environment.world_cell);

            let archetype_component_access = system.archetype_component_access();

            // TODO: figure out when to call update_archetype_component_access for conditions and
            // the system
            // TODO: shared access should account for exclusive systems and non send systems
            shared_access
                .wait_for_access(archetype_component_access, self.system_id)
                .await;

            // SAFETY: shared access is holding locks for the data that all the run conditions access
            if unsafe { self.should_run(context.environment) } {
                // TODO: readd panic handling or fix in scope

                // SAFETY: shared_access is holding locks for the data this system accesses
                unsafe { system.run_unsafe((), context.environment.world_cell) };
            }

            // TODO: remove the access

            // run dependencies
            let mut first_dependent = None;
            for dependent in &self.dependents {
                match dependent {
                    DependentSystem::NotificationChannel(channel) => {
                        channel.send_blocking(()).unwrap();
                    }
                    DependentSystem::System(system_task) => {
                        // TODO: handle exclusive systems and apply deferred.
                        if first_dependent.is_none() {
                            // let task = system_task.clone();
                            first_dependent = Some(system_task.get_task(
                                context,
                                shared_access.clone(),
                            ));
                        } else {
                            context.scope.spawn(system_task.get_task(
                                context,
                                shared_access.clone(),
                            ));
                        }
                    }
                }
            }

            // TODO: we can probably unwrap this recursion into a loop somehow
            if let Some(task) = first_dependent {
                task.await;
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
    unsafe fn should_run(&self, env: &Environment<'_, '_>) -> bool {
        let mut should_run = true;
        for condition in self.system_set_run_conditions.iter() {
            if !condition.can_run(env.world_cell) {
                should_run = false;
            }
        }

        for condition in self.system_run_conditions.iter() {
            if unsafe { !condition.can_run(env.world_cell) } {
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

    apply_final_deferred: bool,
}

impl SystemExecutor for MultiThreadedExecutor {
    fn kind(&self) -> ExecutorKind {
        todo!()
    }

    fn init(&mut self, schedule: &SystemSchedule) {
        // build list of channels indexed by node id.
        let mut finish_send_channels = Vec::with_capacity(schedule.systems.len());
        let mut finish_receive_channels = Vec::with_capacity(schedule.systems.len());
        for _ in &schedule.system_ids {
            // TODO: is setting the capacity faster? we're probably overprovisioning memory here
            let (send, receive) = async_channel::bounded(schedule.systems.len());
            finish_send_channels.push(send);
            finish_receive_channels.push(receive);
        }

        self.roots = Vec::new();
        // keep track of which systems have been processed
        let mut assigned_systems = FixedBitSet::with_capacity(schedule.systems.len());
        for root in &schedule.roots {
            self.roots.push(SystemTask::new(
                root.index(),
                &mut assigned_systems,
                schedule,
                &finish_receive_channels,
                &finish_send_channels,
            ));
        }
    }

    fn run(
        &mut self,
        schedule: &mut SystemSchedule,
        world: &mut World,
        // TODO: Make skip_systems work correctly
        _skip_systems: Option<&FixedBitSet>,
    ) {
        let thread_executor = world
            .get_resource::<MainThreadExecutor>()
            .map(|e| e.0.clone());
        let thread_executor = thread_executor.as_deref();

        let SyncUnsafeSchedule {
            systems,
            mut conditions,
        } = SyncUnsafeSchedule::new(schedule);

        // wrap the schedule and the world to make it easier to pass between functions
        let environment = &Environment::new(self, schedule, world);

        ComputeTaskPool::get_or_init(TaskPool::default).scope_with_executor(
            false,
            thread_executor,
            |scope| {
                let context = Context { environment, scope };

                for root in &self.roots {
                    scope.spawn(root.get_task(
                        &context,
                        self.shared_access.clone(),
                    ));
                }
            },
        );

        if self.apply_final_deferred {
            // // Do one final apply buffers after all systems have completed
            // // Commands should be applied while on the scope's thread, not the executor's thread
            // let res = apply_deferred(&self.unapplied_systems, systems, world);
            // if let Err(payload) = res {
            //     let mut panic_payload = self.panic_payload.lock().unwrap();
            //     *panic_payload = Some(payload);
            // }
            // self.unapplied_systems.clear();
            // debug_assert!(self.unapplied_systems.is_clear());
        }
    }

    fn set_apply_final_deferred(&mut self, value: bool) {
        self.apply_final_deferred = value;
    }
}
