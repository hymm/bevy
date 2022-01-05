use crate::{
    schedule::{
        graph_utils::{self, DependencyGraphError},
        BoxedRunCriteriaLabel, BoxedSystemLabel, GraphNode, IntoSystemDescriptor,
        ParallelSystemContainer, SystemContainer, SystemDescriptor,
    },
    world::World,
};
use bevy_tasks::Scope;
use bevy_utils::{tracing::info, HashMap, HashSet};
use std::future::Future;
use std::fmt::Debug;

pub struct Schedule {
    systems_modified: bool,
    uninitialized_parallel: Vec<usize>,
    systems: Vec<ParallelSystemContainer>,
}

impl Schedule {
    pub fn new() -> Self {
        Self {
            systems_modified: false,
            uninitialized_parallel: vec![],
            systems: vec![],
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

    pub unsafe fn run_unsafe<'scope>(&'scope mut self, world: *mut World, scope: & Scope<'scope, ()>) -> impl Future<Output=Vec<()>> + 'scope {
        let world = world.as_mut().unwrap();
        if self.systems_modified {
            self.initialize_systems(world);
            self.rebuild_orders_and_dependencies();
            self.systems_modified = false;
        }

        let systems = &mut self.systems;

        // put async_scope in an async block to throw away value
        scope.async_scope(move |scope| {
            let system = &mut systems[0];
            let system = system.system_mut();
            let task = async move {
                unsafe { system.run_unsafe((), world) };
            };

            scope.spawn(task);
        })
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
