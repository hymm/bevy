use crate::{
    archetype::ArchetypeComponentId,
    query::Access,
    schedule::{
        graph_utils::{self, DependencyGraphError},
        BoxedSystemLabel, GraphNode, IntoSystemDescriptor, ParallelSystemContainer,
        SystemContainer, SystemDescriptor,
    },
    schedule_v2::{
        one_shot_notify::{one_shot_notify, Receiver as NotifyReceiver, Sender as NotifySender},
        shared_system_access::SharedSystemAccess,
    },
    world::World,
};
use bevy_tasks::Scope;
use bevy_utils::HashSet;
use std::fmt::Debug;
use std::future::Future;

struct SystemSchedulingMetadata {
    /// Indices of systems that depend on this one, used to decrement their
    /// dependency counters when this system finishes.
    dependancies: Vec<NotifyReceiver>,
    /// send channel to
    finish_sender: NotifySender,
    finish_receiver: NotifyReceiver,
    /// Archetype-component access information.
    archetype_component_access: Access<ArchetypeComponentId>,
    /// Whether or not this system is send-able
    is_send: bool,
    /// number of dependants a system has
    dependants_total: usize,
}

pub struct Schedule {
    systems_modified: bool,
    uninitialized_parallel: Vec<usize>,
    systems: Vec<ParallelSystemContainer>,
    /// Cached metadata of systems
    system_metadata: Vec<SystemSchedulingMetadata>,
}

impl Schedule {
    pub fn new() -> Self {
        Self {
            systems_modified: false,
            uninitialized_parallel: vec![],
            systems: vec![],
            system_metadata: vec![],
        }
    }

    pub fn add_system<Params>(&mut self, system: impl IntoSystemDescriptor<Params>) -> &mut Self {
        self.add_system_inner(system.into_descriptor());
        self
    }

    fn add_system_inner(&mut self, system: SystemDescriptor) {
        self.systems_modified = true;
        match system {
            SystemDescriptor::Exclusive(_descriptor) => {}
            SystemDescriptor::Parallel(descriptor) => {
                let container = ParallelSystemContainer::from_descriptor(descriptor);

                self.uninitialized_parallel.push(self.systems.len());
                self.systems.push(container);
            }
        }
    }

    pub unsafe fn run_unsafe<'scope>(
        &'scope mut self,
        world: *mut World,
        scope: &Scope<'scope, ()>,
        shared_access: &'scope SharedSystemAccess,
    ) -> impl Future<Output = Vec<()>> + 'scope {
        let world = world.as_mut().unwrap();
        if self.systems_modified {
            self.initialize_systems(world);
            self.rebuild_orders_and_dependencies();
            self.rebuild_cached_data();
            self.systems_modified = false;
        }

        self.update_archetypes(world);
        
        // put async_scope in an async block to throw away value
        scope.async_scope(move |scope| self.spawn_systems(scope, world, shared_access))
    }

    fn initialize_systems(&mut self, world: &mut World) {
        for index in self.uninitialized_parallel.drain(..) {
            let container = &mut self.systems[index];
            container.system_mut().initialize(world);
        }
    }

    /// Rearranges all systems in topological orders. Systems must be initialized.
    fn rebuild_orders_and_dependencies(&mut self) {
        // This assertion is there to document that a maximum of `u32::MAX / 8` systems should be
        // added to a stage to guarantee that change detection has no false positive, but it
        // can be circumvented using exclusive or chained systems
        assert!(self.systems.len() < (u32::MAX / 8) as usize);
        fn unwrap_dependency_cycle_error<Node: GraphNode, Output, Labels: Debug>(
            result: Result<Output, DependencyGraphError<Labels>>,
            nodes: &[Node],
            nodes_description: &'static str,
        ) -> Output {
            match result {
                Ok(output) => output,
                Err(DependencyGraphError::GraphCycles(cycle)) => {
                    use std::fmt::Write;
                    let mut message = format!("Found a dependency cycle in {}:", nodes_description);
                    writeln!(message).unwrap();
                    for (index, labels) in &cycle {
                        writeln!(message, " - {}", nodes[*index].name()).unwrap();
                        writeln!(
                            message,
                            "    wants to be after (because of labels: {:?})",
                            labels,
                        )
                        .unwrap();
                    }
                    writeln!(message, " - {}", cycle[0].0).unwrap();
                    panic!("{}", message);
                }
            }
        }
        unwrap_dependency_cycle_error(
            process_systems(&mut self.systems),
            &self.systems,
            "parallel systems",
        );
    }

    fn rebuild_cached_data(&mut self) {
        self.system_metadata.clear();

        // Construct scheduling data for systems.
        for container in self.systems.iter() {
            let system = container.system();
            let (finish_sender, finish_receiver) = one_shot_notify();
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
        for (dependant, container) in self.systems.iter().enumerate() {
            for dependency in container.dependencies() {
                self.system_metadata[*dependency].dependants_total += 1;

                let finish_receiver = self.system_metadata[*dependency].finish_receiver.clone();
                self.system_metadata[dependant]
                    .dependancies
                    .push(finish_receiver);
            }
        }
    }

    fn spawn_systems<'scope>(
        &'scope mut self,
        scope: &mut Scope<'scope, ()>,
        world: &'scope World,
        shared_access: &'scope SharedSystemAccess,
    ) {
        #[cfg(feature = "trace")]
        let span = bevy_utils::tracing::info_span!("prepare_systems");
        #[cfg(feature = "trace")]
        let _guard = span.enter();
        for (index, (system_data, system)) in self
            .system_metadata
            .iter_mut()
            .zip(&mut self.systems)
            .enumerate()
        {
            // span needs to be defined before get_system_future to get the name
            #[cfg(feature = "trace")]
            let system_overhead_span =
                bevy_utils::tracing::info_span!("system_overhead", name = &*system.name());
            let task =
                Self::get_system_future(system_data, system, index, world, shared_access.clone());

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
        let finish_senders = if system_data.dependants_total > 0 {
            // it should be ok to reset here because systems are topologically sorted
            // so systems that need to wait on the finish sender have not been spawned yet
            system_data.finish_sender.reset();
            Some(system_data.finish_sender.clone())
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
                receiver.notified().await;
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
                dependants_finish_sender.notify();
            }
        };

        future
    }
}

/// Sorts given system containers topologically, populates their resolved dependencies
/// and run criteria.
fn process_systems(
    systems: &mut Vec<impl SystemContainer>,
) -> Result<(), DependencyGraphError<HashSet<BoxedSystemLabel>>> {
    let mut graph = graph_utils::build_dependency_graph(systems);
    let order = graph_utils::topological_order(&graph)?;
    let mut order_inverted = order.iter().enumerate().collect::<Vec<_>>();
    order_inverted.sort_unstable_by_key(|(_, &key)| key);
    for (index, container) in systems.iter_mut().enumerate() {
        container.set_dependencies(
            graph
                .get_mut(&index)
                .unwrap()
                .drain()
                .map(|(index, _)| order_inverted[index].0),
        );
    }
    let mut temp = systems.drain(..).map(Some).collect::<Vec<_>>();
    for index in order {
        systems.push(temp[index].take().unwrap());
    }
    Ok(())
}
