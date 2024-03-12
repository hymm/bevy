use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use bevy_tasks::{ComputeTaskPool, Scope, TaskPool};
use bevy_utils::syncunsafecell::SyncUnsafeCell;
use dashmap::DashMap;
use fixedbitset::FixedBitSet;
// std once cell is not thread safe
use once_cell::sync::OnceCell;

use super::{ExecutorKind, MainThreadExecutor, SystemExecutor, SystemSchedule};
use crate::{
    archetype::ArchetypeComponentId,
    query::Access,
    schedule::{executor::multi_threaded::evaluate_and_fold_conditions, BoxedCondition},
    system::BoxedSystem,
    world::{unsafe_world_cell::UnsafeWorldCell, World},
};

/// Borrowed data used by the [`MultiThreadedExecutor`].
struct Environment<'env, 'sys> {
    systems: &'sys [SyncUnsafeCell<BoxedSystem>],
    conditions: Conditions<'sys>,
    world_cell: UnsafeWorldCell<'env>,
}

impl<'env, 'sys> Environment<'env, 'sys> {
    fn new(schedule: &'sys mut SystemSchedule, world: &'env mut World) -> Self {
        Environment {
            systems: SyncUnsafeCell::from_mut(schedule.systems.as_mut_slice()).as_slice_of_cells(),
            conditions: Conditions {
                system_conditions: SyncUnsafeCell::from_mut(
                    schedule.system_conditions.as_mut_slice(),
                )
                .as_slice_of_cells(),
                set_conditions: Arc::new(Mutex::new(&mut schedule.set_conditions)),
                sets_with_conditions_of_systems: &schedule.sets_with_conditions_of_systems,
                // systems_in_sets_with_conditions: &schedule.systems_in_sets_with_conditions,
            },
            world_cell: world.as_unsafe_world_cell(),
        }
    }
}

struct Conditions<'a> {
    system_conditions: &'a [SyncUnsafeCell<Vec<BoxedCondition>>],
    set_conditions: Arc<Mutex<&'a mut [Vec<BoxedCondition>]>>,
    sets_with_conditions_of_systems: &'a [FixedBitSet],
    // systems_in_sets_with_conditions: &'a [FixedBitSet],
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

    // TODO: move this to SystemAccess
    archetype_component_access: Access<ArchetypeComponentId>,
    // system_set_run_conditions: Vec<ThreadSafeRunCondition>,
    // // TODO, see if we can make this just a Vec<BoxedCondition>.
    // // Made it a ThreadSafeRunConditino because it needed to be clone
    // system_run_conditions: Vec<ThreadSafeRunCondition>,
}

impl SystemTask {
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
            .map(|dependent_id| {
                if assigned_systems.contains(*dependent_id) {
                    DependentSystem::NotificationChannel(
                        finish_send_channels[*dependent_id].clone(),
                    )
                } else {
                    dbg!("dependent system");
                    DependentSystem::System((
                        finish_send_channels[*dependent_id].clone(),
                        SystemTask::new(
                            *dependent_id,
                            assigned_systems,
                            schedule,
                            finish_receive_channels,
                            finish_send_channels,
                        ),
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
            archetype_component_access: Default::default(),
        }
    }

    fn get_task<'scope, 'env: 'scope, 'sys>(
        &'scope self,
        scope: &'scope Scope<'scope, 'env, ()>,
        env: &'env Environment,
        mut executor: ExecutorState,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'scope>> {
        let task = async move {
            let mut count = 0;
            while count < self.dependencies_count {
                // wait for a dependency to complete
                self.dependency_finished_channel.recv().await.unwrap();
                count += 1;
            }

            // take a lock on all the access needed for the system and it's run conditions
            executor.wait_for_access(self, env).await;

            // SAFETY: shared access is holding locks for the data that all the run conditions access
            if unsafe { executor.should_run(self, env) } {
                // TODO: readd panic handling or fix in scope

                // SAFETY: this is the only task that accesses this system
                let system = unsafe { &mut *env.systems[self.system_id].get() };
                // SAFETY: shared_access is holding locks for the data this system accesses
                unsafe { system.run_unsafe((), env.world_cell) };
            }

            // TODO: it might be better to have a drop guard for this and mirror the mutex api
            executor.remove_access(self).await;

            // TODO: if there are no dependents left we should run the

            // run dependencies
            // FIX: should send finish to the dependent system too.
            let mut first_dependent = None;
            for dependent in &self.dependents {
                match dependent {
                    DependentSystem::NotificationChannel(channel) => {
                        channel.send_blocking(()).unwrap();
                    }
                    DependentSystem::System((channel, system_task)) => {
                        channel.send_blocking(()).unwrap();
                        if system_task.is_exclusive {
                            scope.spawn_on_scope(system_task.get_task(
                                scope,
                                env,
                                executor.clone(),
                            ));
                        } else if !system_task.is_send {
                            scope.spawn_on_external(system_task.get_task(
                                scope,
                                env,
                                executor.clone(),
                            ));
                        } else if first_dependent.is_none() {
                            first_dependent =
                                Some(system_task.get_task(scope, env, executor.clone()));
                        } else {
                            scope.spawn(system_task.get_task(scope, env, executor.clone()));
                        }
                    }
                }
            }

            if let Some(task) = first_dependent {
                task.await;
            }
        };
        // TODO: add instrumentation back
        // let task = task.instrument(self.system_task_span)
        Box::pin(task)
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
    System((Sender<()>, SystemTask)),
}

#[derive(Default)]
pub struct MultiThreadedExecutor {
    shared_access: ExecutorState,

    roots: Vec<SystemTask>,

    apply_final_deferred: bool,
}

impl SystemExecutor for MultiThreadedExecutor {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::MultiThreaded
    }

    fn init(&mut self, schedule: &SystemSchedule) {
        let mut access = self.shared_access.access.lock().unwrap();
        access.system_access = Vec::with_capacity(schedule.systems.len());

        // build list of channels indexed by node id.
        let mut finish_send_channels = Vec::with_capacity(schedule.systems.len());
        let mut finish_receive_channels = Vec::with_capacity(schedule.systems.len());
        for _ in &schedule.system_ids {
            // TODO: is setting the capacity faster? we're probably overprovisioning memory here
            let (send, receive) = async_channel::bounded(schedule.systems.len());
            finish_send_channels.push(send);
            finish_receive_channels.push(receive);
            // TODO: reinitialize all the system accesses here
            access.system_access.push(Default::default());
        }

        self.roots = Vec::new();
        // keep track of which systems have been processed
        let mut assigned_systems = FixedBitSet::with_capacity(schedule.systems.len());
        for root in &schedule.roots {
            if assigned_systems.contains(root.index()) {
                dbg!("something went wrong root was already assigned");
                continue;
            }
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

        // wrap the schedule and the world to make it easier to pass between functions
        let environment = &Environment::new(schedule, world);

        ComputeTaskPool::get_or_init(TaskPool::default).scope_with_executor(
            false,
            thread_executor,
            |scope| {
                // TODO: spawn the root tasks from multiple threads
                for root in &self.roots {
                    scope.spawn(root.get_task(scope, &environment, self.shared_access.clone()));
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

struct ExecutorState {
    access: Arc<Mutex<SystemAccess>>,
    access_updated_recv: async_broadcast::Receiver<()>,
    access_updated_send: async_broadcast::Sender<()>,
}

impl Clone for ExecutorState {
    fn clone(&self) -> Self {
        Self {
            access: self.access.clone(),
            access_updated_recv: self.access_updated_recv.clone(),
            access_updated_send: self.access_updated_send.clone(),
        }
    }
}

impl Default for ExecutorState {
    fn default() -> Self {
        let (mut access_updated_send, access_updated_recv) = async_broadcast::broadcast(1);
        access_updated_send.set_overflow(true);

        Self {
            access: Default::default(),
            access_updated_recv,
            access_updated_send,
        }
    }
}

/// data of active running systems' acccess
#[derive(Default)]
struct SystemAccess {
    access: Access<ArchetypeComponentId>,
    /// active access
    active_access: Arc<DashMap<usize, Access<ArchetypeComponentId>>>,
    local_thread_running: bool,
    // TODO: can this be a slice
    system_access: Vec<SyncUnsafeCell<Access<ArchetypeComponentId>>>,
    // TODO: we should move these out into some thread safe evaluation
    // /// sets that have already had their run conditions run
    // evaluated_sets: FixedBitSet,
    // /// systems whose set conditions evaluated to false earlier
    // skipped_systems: FixedBitSet,
}

impl SystemAccess {
    fn is_compatible(&self, system_meta: &SystemTask) -> bool {
        if system_meta.is_exclusive && self.active_access.len() > 0 {
            return false;
        }

        if !system_meta.is_send && self.local_thread_running {
            return false;
        }

        self.access
            .is_compatible(&system_meta.archetype_component_access)
    }

    fn update_access(&mut self, env: &Environment<'_, '_>, system_task: &SystemTask) {
        let archetype_component_access =
            unsafe { &mut *self.system_access[system_task.system_id].get() };
        // TODO: we should early out if there are no new archetypes
        let mut set_conditions = env.conditions.set_conditions.lock().unwrap();
        for set_idx in env.conditions.sets_with_conditions_of_systems[system_task.system_id].ones()
        // .difference(&access.evaluated_sets)
        {
            for condition in &mut set_conditions[set_idx] {
                condition.update_archetype_component_access(env.world_cell);
                // This is a bit pessimistic, since ones that might have been evaluated already are included
                archetype_component_access.extend(condition.archetype_component_access());
            }
        }

        // SAFETY: this is the only thread that is trying to start this system
        let conditions =
            unsafe { &mut *env.conditions.system_conditions[system_task.system_id].get() };
        for condition in conditions {
            condition.update_archetype_component_access(env.world_cell);
            archetype_component_access.extend(condition.archetype_component_access());
        }

        let system = unsafe { &mut *env.systems[system_task.system_id].get() };
        system.update_archetype_component_access(env.world_cell);
        archetype_component_access.extend(system.archetype_component_access());
    }
}

impl ExecutorState {
    pub async fn wait_for_access(&mut self, system_meta: &SystemTask, env: &Environment<'_, '_>) {
        loop {
            {
                let mut access = self.access.lock().unwrap();
                // TODO: should probalby update the access after checking if exclusive or non send
                access.update_access(env, system_meta);
                if access.is_compatible(system_meta) {
                    access
                        .access
                        .extend(&system_meta.archetype_component_access);
                    // TODO: it'd be better if we could hold a reference to the cached access in the task
                    // for cleanup instead of cloning here
                    access.active_access.insert(
                        system_meta.system_id,
                        system_meta.archetype_component_access.clone(),
                    );
                    break;
                }
            }
            self.access_updated_recv.recv().await.unwrap();
        }
    }

    // use when system is finished running
    pub async fn remove_access(&mut self, system_meta: &SystemTask) {
        // TODO: this should also release the local_thread_running bit if needed
        {
            let mut access = self.access.lock().unwrap();
            access.access.clear();
            // TODO: it might be cleaner to keep a reference count of the read accesses, so we don't
            // have to rebuild the whole access
            access.active_access.remove(&system_meta.system_id);
            let SystemAccess {
                ref mut access,
                ref active_access,
                ..
            } = *access;

            active_access
                .iter()
                .for_each(|active_access| access.extend(&active_access));
        }
        self.access_updated_send.broadcast(()).await.unwrap();
    }

    unsafe fn should_run(&mut self, system_meta: &SystemTask, env: &Environment<'_, '_>) -> bool {
        // TODO: figure out what to do to cache the set condition results without having to take the lock again
        // let mut should_run = !self.access.skipped_systems.contains(system_meta.system_id);
        let mut should_run = true;
        // TODO: don't take this lock if there are no set_conditions
        let mut set_conditions = env.conditions.set_conditions.lock().unwrap();
        for set_idx in env.conditions.sets_with_conditions_of_systems[system_meta.system_id].ones()
        {
            // if self.access.evaluated_sets.contains(set_idx) {
            //     continue;
            // }

            // Evaluate the system set's conditions.
            // SAFETY:
            // - The caller ensures that `world` has permission to read any data
            //   required by the conditions.
            // - `update_archetype_component_access` has been called for each run condition.
            let set_conditions_met = unsafe {
                evaluate_and_fold_conditions(&mut set_conditions[set_idx], env.world_cell)
            };

            // if !set_conditions_met {
            //     self.skipped_systems
            //         .union_with(&env.conditions.systems_in_sets_with_conditions[set_idx]);
            // }

            should_run &= set_conditions_met;
            // self.evaluated_sets.insert(set_idx);
        }

        let system_conditions =
            unsafe { &mut *env.conditions.system_conditions[system_meta.system_id].get() };

        // Evaluate the system's conditions.
        // SAFETY:
        // - The caller ensures that `world` has permission to read any data
        //   required by the conditions.
        // - `update_archetype_component_access` has been called for each run condition.
        let system_conditions_met =
            unsafe { evaluate_and_fold_conditions(system_conditions, env.world_cell) };

        // if !system_conditions_met {
        //     self.skipped_systems.insert(system_meta.system_id);
        // }

        should_run &= system_conditions_met;

        should_run
    }
}
