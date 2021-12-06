use crate::{
    archetype::{ArchetypeComponentId, ArchetypeGeneration},
    query::Access,
    schedule::{ParallelSystemContainer, ParallelSystemExecutor},
    world::World,
};
use async_channel::{Receiver, Sender};
#[cfg(feature = "trace")]
use bevy_utils::tracing::Instrument;
use async_mutex::Mutex;
use bevy_tasks::{ComputeTaskPool, TaskPool};
use bevy_utils::HashMap;
use fixedbitset::FixedBitSet;
use futures::{future::join_all};
use std::clone::Clone;
use std::future::Future;
use std::sync::Arc;

#[cfg(test)]
use SchedulingEvent::*;

struct SharedSystemAccess {
    access: Arc<Mutex<Access<ArchetypeComponentId>>>,
    // active access
    active_access: Arc<Mutex<HashMap<usize, Access<ArchetypeComponentId>>>>,
    access_updated_recv: Receiver<()>,
    access_updated_send: Sender<()>,
}

impl SharedSystemAccess {
    pub async fn wait_for_access(&mut self, other: &Access<ArchetypeComponentId>, index: usize) {
        loop {
            let mut access = self.access.lock().await;

            if access.is_compatible(other) {
                access.extend(other);
                drop(access);
                {
                    let mut active_access = self.active_access.lock().await;
                    active_access.insert(index, other.clone());
                }
                self.access_updated_send.send(()).await.unwrap();
                break;
            } else {
                drop(access);
                self.access_updated_recv.recv().await.unwrap();
                // drain any other messages
                while self.access_updated_recv.recv().await.is_ok() {}
            }
        }
    }

    // use when system is finished running
    pub async fn remove_access(&mut self, access_id: usize) {
        {
            let mut active_access = self.active_access.lock().await;
            active_access.remove(&access_id);
            {
                let mut access = self.access.lock().await;
                access.clear();
                for (_, active_access) in active_access.iter() {
                    access.extend(active_access);
                }
            } // drop mutex here
        }
        self.access_updated_send.send(()).await.unwrap();
    }
}

impl Clone for SharedSystemAccess {
    fn clone(&self) -> Self {
        SharedSystemAccess {
            access: self.access.clone(),
            active_access: self.active_access.clone(),
            access_updated_recv: self.access_updated_recv.clone(),
            access_updated_send: self.access_updated_send.clone(),
        }
    }
}

impl Default for SharedSystemAccess {
    fn default() -> Self {
        let (access_updated_send, access_updated_recv) = async_channel::unbounded();

        SharedSystemAccess {
            access: Default::default(),
            active_access: Default::default(),
            access_updated_recv,
            access_updated_send,
        }
    }
}

struct SystemSchedulingMetadata {
    /// Indices of systems that depend on this one, used to decrement their
    /// dependency counters when this system finishes.
    dependants: Vec<Receiver<()>>,
    finish_sender: Sender<()>,
    finish_receiver: Receiver<()>,
    /// Archetype-component access information.
    archetype_component_access: Access<ArchetypeComponentId>,
    /// Whether or not this system is send-able
    is_send: bool,
}

pub struct ParallelExecutor {
    /// Last archetypes generation observed by parallel systems.
    archetype_generation: ArchetypeGeneration,
    /// Cached metadata of every system.
    system_metadata: Vec<SystemSchedulingMetadata>,
    /// Systems that should be started at next opportunity.
    queued: FixedBitSet,
    /// Systems that are currently running.
    running: FixedBitSet,
    /// Systems that should run this iteration.
    should_run: FixedBitSet,
    /// Compound archetype-component access information of currently running systems.
    shared_access: SharedSystemAccess,
    #[cfg(test)]
    events_sender: Option<Sender<SchedulingEvent>>,
}

impl Default for ParallelExecutor {
    fn default() -> Self {
        Self {
            archetype_generation: ArchetypeGeneration::initial(),
            system_metadata: Default::default(),
            queued: Default::default(),
            running: Default::default(),
            should_run: Default::default(),
            shared_access: Default::default(),
            #[cfg(test)]
            events_sender: None,
        }
    }
}

impl ParallelSystemExecutor for ParallelExecutor {
    fn rebuild_cached_data(&mut self, systems: &[ParallelSystemContainer]) {
        self.system_metadata.clear();
        self.queued.grow(systems.len());
        self.running.grow(systems.len());
        self.should_run.grow(systems.len());

        // Construct scheduling data for systems.
        for container in systems.iter() {
            let system = container.system();
            let (finish_sender, finish_receiver) = async_channel::bounded(1);
            self.system_metadata.push(SystemSchedulingMetadata {
                finish_sender,
                finish_receiver,
                dependants: vec![],
                is_send: system.is_send(),
                archetype_component_access: Default::default(),
            });
        }
        // Populate the dependants lists in the scheduling metadata.
        for (dependant, container) in systems.iter().enumerate() {
            for dependency in container.dependencies() {
                let finish_receiver = self.system_metadata[dependant].finish_receiver.clone();
                self.system_metadata[*dependency]
                    .dependants
                    .push(finish_receiver);
            }
        }
    }

    fn run_systems(&mut self, systems: &mut [ParallelSystemContainer], world: &mut World) {
        #[cfg(test)]
        if self.events_sender.is_none() {
            let (sender, receiver) = async_channel::unbounded::<SchedulingEvent>();
            world.insert_resource(receiver);
            self.events_sender = Some(sender);
        }

        self.update_archetypes(systems, world);

        let compute_pool = world
            .get_resource_or_insert_with(|| ComputeTaskPool(TaskPool::default()))
            .clone();
        compute_pool.scope(|scope| {
            let (system_tasks, system_local_tasks) = self.prepare_systems(systems, world);
            scope.spawn(async move {
                join_all(system_tasks).await;
            });
            scope.spawn_local(async move {
                join_all(system_local_tasks).await;
            });
        });
    }
}

impl ParallelExecutor {
    /// Calls system.new_archetype() for each archetype added since the last call to
    /// [update_archetypes] and updates cached archetype_component_access.
    fn update_archetypes(&mut self, systems: &mut [ParallelSystemContainer], world: &World) {
        #[cfg(feature = "trace")]
        let span = bevy_utils::tracing::info_span!("update_archetypes");
        #[cfg(feature = "trace")]
        let _guard = span.enter();
        let archetypes = world.archetypes();
        let new_generation = archetypes.generation();
        let old_generation = std::mem::replace(&mut self.archetype_generation, new_generation);
        let archetype_index_range = old_generation.value()..new_generation.value();

        for archetype in archetypes.archetypes[archetype_index_range].iter() {
            for (index, container) in systems.iter_mut().enumerate() {
                let meta = &mut self.system_metadata[index];
                let system = container.system_mut();
                system.new_archetype(archetype);
                meta.archetype_component_access
                    .extend(system.archetype_component_access());
            }
        }
    }

    /// Populates `should_run` bitset, spawns tasks for systems that should run this iteration,
    /// queues systems with no dependencies to run (or skip) at next opportunity.
    fn prepare_systems<'scope>(
        &mut self,
        scope: &mut Scope<'scope, ()>,
        systems: &'scope mut [ParallelSystemContainer],
        world: &'scope World,
    ) -> (
        Vec<impl Future<Output = ()> + 'scope>,
        Vec<impl Future<Output = ()> + 'scope>,
    ) {
        #[cfg(feature = "trace")]
        let span = bevy_utils::tracing::info_span!("prepare_systems");
        #[cfg(feature = "trace")]
        let _guard = span.enter();
        self.should_run.clear();
        for (index, (system_data, system)) in
            self.system_metadata.iter_mut().zip(systems).enumerate()
        {
            // Spawn the system task.
            if system.should_run() {
                self.should_run.set(index, true);
                let start_receiver = system_data.start_receiver.clone();
                let finish_sender = self.finish_sender.clone();
                let dependants_finish_sender = system_data.finish_sender.clone();
                let dependants = system_data
                    .dependants
                    .iter()
                    .cloned()
                    .collect::<Vec<Receiver<()>>>();

                let mut shared_access = self.shared_access.clone();
                let system = system.system_mut();
                let archetype_component_access = system_data.archetype_component_access.clone();
                #[cfg(feature = "trace")] // NB: outside the task to get the TLS current span
                let system_span = bevy_utils::tracing::info_span!("system", name = &*system.name());
                #[cfg(feature = "trace")]
                let overhead_span =
                    bevy_utils::tracing::info_span!("system overhead", name = &*system.name());
                let task = async move {
                    // wait for all dependencies to complete
                    let mut dependants = dependants.iter();
                    while let Some(receiver) = dependants.next() {
                        receiver
                            .recv()
                            .await
                            .unwrap_or_else(|error| unreachable!(error));
                    }
                    shared_access
                        .wait_for_access(&archetype_component_access, index)
                        .await;
                    #[cfg(feature = "trace")]
                    let system_guard = system_span.enter();
                    unsafe { system.run_unsafe((), world) };
                    #[cfg(feature = "trace")]
                    drop(system_guard);
                    dependants_finish_sender
                        .send(())
                        .await
                        .unwrap_or_else(|error| unreachable!(error));
                    shared_access.remove_access(index).await;
                };

                #[cfg(feature = "trace")]
                let task = task.instrument(overhead_span);
                if system_data.is_send {
                    tasks.push(task);
                } else {
                    local_tasks.push(task);
                }
            });

        (tasks, local_tasks)
    }

    fn get_system_future<'scope>(
        system_data: &mut SystemSchedulingMetadata,
        system_container: &'scope ParallelSystemContainer,
        index: usize,
        world: &'scope World,
        mut shared_access: SharedSystemAccess,
    ) -> impl Future<Output = ()> + 'scope {
        let dependants_finish_sender = system_data.finish_sender.clone();
        let dependants = system_data
            .dependants
            .iter()
            .cloned()
            .collect::<Vec<Receiver<()>>>();

        let system = unsafe { system_container.system_mut_unsafe() };
        let archetype_component_access = system_data.archetype_component_access.clone();
        #[cfg(feature = "trace")] // NB: outside the task to get the TLS current span
        let system_span = bevy_utils::tracing::info_span!("system", name = &*system.name());
        async move {
            // wait for all dependencies to complete
            let mut dependants = dependants.iter();
            while let Some(receiver) = dependants.next() {
                receiver
                    .recv()
                    .await
                    .unwrap_or_else(|error| unreachable!(error));
            }
            shared_access
                .wait_for_access(&archetype_component_access, index)
                .await;
            #[cfg(feature = "trace")]
            let system_guard = system_span.enter();
            unsafe { system.run_unsafe((), world) };
            #[cfg(feature = "trace")]
            drop(system_guard);
            dependants_finish_sender
                .send(())
                .await
                .unwrap_or_else(|error| unreachable!(error));
            shared_access.remove_access(index).await;
        }
    }

    #[cfg(test)]
    fn emit_event(&self, event: SchedulingEvent) {
        let _ = self.events_sender.as_ref().unwrap().try_send(event);
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
enum SchedulingEvent {
    StartedSystems(usize),
}

#[cfg(test)]
mod tests {
    use super::SchedulingEvent::{self, *};
    use crate::{
        schedule::{SingleThreadedExecutor, Stage, SystemStage},
        system::{NonSend, Query, Res, ResMut},
        world::World,
    };
    use async_channel::Receiver;

    use crate as bevy_ecs;
    use crate::component::Component;
    #[derive(Component)]
    struct W<T>(T);

    fn receive_events(world: &World) -> Vec<SchedulingEvent> {
        let mut events = Vec::new();
        while let Ok(event) = world
            .get_resource::<Receiver<SchedulingEvent>>()
            .unwrap()
            .try_recv()
        {
            events.push(event);
        }
        events
    }

    #[test]
    fn trivial() {
        let mut world = World::new();
        fn wants_for_nothing() {}
        let mut stage = SystemStage::parallel()
            .with_system(wants_for_nothing)
            .with_system(wants_for_nothing)
            .with_system(wants_for_nothing);
        stage.run(&mut world);
        stage.run(&mut world);
        assert_eq!(
            receive_events(&world),
            vec![StartedSystems(3), StartedSystems(3),]
        )
    }

    #[test]
    fn resources() {
        let mut world = World::new();
        world.insert_resource(0usize);
        fn wants_mut(_: ResMut<usize>) {}
        fn wants_ref(_: Res<usize>) {}
        let mut stage = SystemStage::parallel()
            .with_system(wants_mut)
            .with_system(wants_mut);
        stage.run(&mut world);
        assert_eq!(
            receive_events(&world),
            vec![StartedSystems(1), StartedSystems(1),]
        );
        let mut stage = SystemStage::parallel()
            .with_system(wants_mut)
            .with_system(wants_ref);
        stage.run(&mut world);
        assert_eq!(
            receive_events(&world),
            vec![StartedSystems(1), StartedSystems(1),]
        );
        let mut stage = SystemStage::parallel()
            .with_system(wants_ref)
            .with_system(wants_ref);
        stage.run(&mut world);
        assert_eq!(receive_events(&world), vec![StartedSystems(2),]);
    }

    #[test]
    fn queries() {
        let mut world = World::new();
        world.spawn().insert(W(0usize));
        fn wants_mut(_: Query<&mut W<usize>>) {}
        fn wants_ref(_: Query<&W<usize>>) {}
        let mut stage = SystemStage::parallel()
            .with_system(wants_mut)
            .with_system(wants_mut);
        stage.run(&mut world);
        assert_eq!(
            receive_events(&world),
            vec![StartedSystems(1), StartedSystems(1),]
        );
        let mut stage = SystemStage::parallel()
            .with_system(wants_mut)
            .with_system(wants_ref);
        stage.run(&mut world);
        assert_eq!(
            receive_events(&world),
            vec![StartedSystems(1), StartedSystems(1),]
        );
        let mut stage = SystemStage::parallel()
            .with_system(wants_ref)
            .with_system(wants_ref);
        stage.run(&mut world);
        assert_eq!(receive_events(&world), vec![StartedSystems(2),]);
        let mut world = World::new();
        world.spawn().insert_bundle((W(0usize), W(0u32), W(0f32)));
        fn wants_mut_usize(_: Query<(&mut W<usize>, &W<f32>)>) {}
        fn wants_mut_u32(_: Query<(&mut W<u32>, &W<f32>)>) {}
        let mut stage = SystemStage::parallel()
            .with_system(wants_mut_usize)
            .with_system(wants_mut_u32);
        stage.run(&mut world);
        assert_eq!(receive_events(&world), vec![StartedSystems(2),]);
    }

    #[test]
    fn non_send_resource() {
        use std::thread;
        let mut world = World::new();
        world.insert_non_send(thread::current().id());
        fn non_send(thread_id: NonSend<thread::ThreadId>) {
            assert_eq!(thread::current().id(), *thread_id);
        }
        fn empty() {}
        let mut stage = SystemStage::parallel()
            .with_system(non_send)
            .with_system(non_send)
            .with_system(empty)
            .with_system(empty)
            .with_system(non_send)
            .with_system(non_send);
        stage.run(&mut world);
        assert_eq!(
            receive_events(&world),
            vec![
                StartedSystems(3),
                StartedSystems(1),
                StartedSystems(1),
                StartedSystems(1),
            ]
        );
        stage.set_executor(Box::new(SingleThreadedExecutor::default()));
        stage.run(&mut world);
    }
}
