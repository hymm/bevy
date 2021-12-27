use crate::{
    archetype::{ArchetypeComponentId, ArchetypeGeneration},
    query::Access,
    schedule::{
        finish_channel::{finish_channel, Receiver as FinishReceiver, Sender as FinishSender},
        ParallelSystemContainer, ParallelSystemExecutor,
    },
    world::World,
};
use async_broadcast::{broadcast, Receiver, Sender};
use async_mutex::Mutex;
use bevy_tasks::{ComputeTaskPool, Scope, TaskPool};
#[cfg(feature = "trace")]
use bevy_utils::tracing::Instrument;
use dashmap::DashMap;
use std::clone::Clone;
use std::future::Future;
use std::sync::Arc;

struct SharedSystemAccess {
    access: Arc<Mutex<Access<ArchetypeComponentId>>>,
    // active access
    active_access: Arc<DashMap<usize, Access<ArchetypeComponentId>>>,
    access_updated_recv: Receiver<()>,
    access_updated_send: Sender<()>,
}

impl SharedSystemAccess {
    pub async fn wait_for_access(&mut self, other: &Access<ArchetypeComponentId>, index: usize) {
        loop {
            {
                let mut access = self.access.lock().await;
                let is_compatible = access.is_compatible(other);
                if is_compatible {
                    access.extend(other);
                    self.active_access.insert(index, other.clone());
                    break;
                }
            }
            self.access_updated_recv.recv().await.unwrap();
        }
    }

    // use when system is finished running
    pub async fn remove_access(&mut self, access_id: usize) {
        {
            let mut access = self.access.lock().await;
            access.clear();
            self.active_access.remove(&access_id);
            self.active_access
                .iter()
                .for_each(|active_access| access.extend(&active_access));
        }
        self.access_updated_send.broadcast(()).await.unwrap();
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
        let (mut access_updated_send, access_updated_recv) = broadcast(1);
        access_updated_send.set_overflow(true);

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
    dependancies: Vec<FinishReceiver>,
    /// send channel to
    finish_sender: FinishSender,
    finish_receiver: FinishReceiver,
    /// Archetype-component access information.
    archetype_component_access: Access<ArchetypeComponentId>,
    /// Whether or not this system is send-able
    is_send: bool,
    /// number of dependants a system has
    dependants_total: usize,
}

pub struct ParallelExecutor {
    /// Last archetypes generation observed by parallel systems.
    archetype_generation: ArchetypeGeneration,
    /// Cached metadata of every system.
    system_metadata: Vec<SystemSchedulingMetadata>,
    /// Compound archetype-component access information of currently running systems.
    shared_access: SharedSystemAccess,
    // #[cfg(test)]
    // events_sender: Option<Sender<SchedulingEvent>>,
}

impl Default for ParallelExecutor {
    fn default() -> Self {
        Self {
            archetype_generation: ArchetypeGeneration::initial(),
            system_metadata: Default::default(),
            shared_access: Default::default(),
            // #[cfg(test)]
            // events_sender: None,
        }
    }
}

impl ParallelSystemExecutor for ParallelExecutor {
    fn rebuild_cached_data(&mut self, systems: &[ParallelSystemContainer]) {
        self.system_metadata.clear();

        // Construct scheduling data for systems.
        for container in systems.iter() {
            let system = container.system();
            let (finish_sender, finish_receiver) = finish_channel();
            self.system_metadata.push(SystemSchedulingMetadata {
                finish_sender,
                finish_receiver,
                dependancies: vec![],
                is_send: system.is_send(),
                dependants_total: 0,
                archetype_component_access: Default::default(),
            });
        }
        // Populate the dependants lists in the scheduling metadata.
        for (dependant, container) in systems.iter().enumerate() {
            for dependency in container.dependencies() {
                self.system_metadata[*dependency].dependants_total += 1;

                let finish_receiver = self.system_metadata[*dependency].finish_receiver.clone();
                self.system_metadata[dependant]
                    .dependancies
                    .push(finish_receiver);
            }
        }
    }

    fn run_systems(&mut self, systems: &mut [ParallelSystemContainer], world: &mut World) {
        self.update_archetypes(systems, world);

        let compute_pool = world
            .get_resource_or_insert_with(|| ComputeTaskPool(TaskPool::default()))
            .clone();
        compute_pool.scope(|mut scope| {
            self.prepare_systems(&mut scope, systems, world);
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
    ) {
        #[cfg(feature = "trace")]
        let span = bevy_utils::tracing::info_span!("prepare_systems");
        #[cfg(feature = "trace")]
        let _guard = span.enter();
        for (index, (system_data, system)) in
            self.system_metadata.iter_mut().zip(systems).enumerate()
        {
            if !system.should_run() {
                // let dependants run if will not run
                system_data
                    .finish_sender.finish();
                break;
            }

            // span needs to be defined before get_system_future to get the name
            #[cfg(feature = "trace")]
            let system_overhead_span = bevy_utils::tracing::info_span!("system_overhead", name = &*system.name());

            let task = Self::get_system_future(
                system_data,
                system,
                index,
                world,
                self.shared_access.clone(),
            );

            #[cfg(feature = "trace")]
            let task = task.instrument(system_overhead_span);

            if system_data.is_send {
                scope.spawn(task);
            } else {
                scope.spawn_local(task);
            }
        }
    }

    fn get_system_future<'scope>(
        system_data: &mut SystemSchedulingMetadata,
        system_container: &'scope mut ParallelSystemContainer,
        index: usize,
        world: &'scope World,
        mut shared_access: SharedSystemAccess,
    ) -> impl Future<Output = ()> + 'scope {
        let dependants_total = system_data.dependants_total;
        let finish_senders = if dependants_total > 0 {
            // it should be ok to reset here because systems are topologically sorted
            system_data.finish_sender.reset();
            Some(
                system_data.finish_sender.clone(),
            )
        } else {
            None
        };
        let mut dependants = system_data.dependancies.to_owned();

        let system = system_container.system_mut();
        let archetype_component_access = system_data.archetype_component_access.clone();
        #[cfg(feature = "trace")]
        let system_span = bevy_utils::tracing::info_span!("system", name = &*system.name());
        let future = async move {
            // wait for all dependencies to complete
            let mut dependancies = dependants.iter_mut();
            while let Some(receiver) = dependancies.next() {
                receiver
                    .finished()
                    .await
                    ;
            }

            shared_access
                .wait_for_access(&archetype_component_access, index)
                .await;

            #[cfg(feature = "trace")]
            let system_guard = system_span.enter();
            unsafe { system.run_unsafe((), world) };
            #[cfg(feature = "trace")]
            drop(system_guard);

            shared_access.remove_access(index).await;

            // only await for these if there are dependants
            if let Some(mut dependants_finish_sender) = finish_senders {
                // greater than 1 because the 1 is the receiver store in system metadata
                dependants_finish_sender.finish();
            }
        };

        future
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
