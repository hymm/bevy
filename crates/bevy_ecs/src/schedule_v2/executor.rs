use crate::{
    archetype::ArchetypeComponentId,
    query::Access,
    schedule::{
        graph_utils::{self, DependencyGraphError},
        BoxedSystemLabel, IntoSystemDescriptor, SystemContainer,
    },
    schedule_v2::schedule::Schedule,
    world::World,
};
use async_broadcast::{broadcast, Receiver, Sender};
use async_mutex::Mutex;
use bevy_tasks::{ComputeTaskPool, TaskPool};
use bevy_utils::HashSet;
use dashmap::DashMap;
use std::sync::Arc;

// use std::pin::Pin;
// use futures_lite::pin;

pub struct SharedSystemAccess {
    access: Arc<Mutex<Access<ArchetypeComponentId>>>,
    active_access: Arc<DashMap<usize, Access<ArchetypeComponentId>>>,
    access_updated_recv: Receiver<()>,
    access_updated_send: Sender<()>,
}

impl SharedSystemAccess {
    pub async fn wait_for_access(&mut self, other: &Access<ArchetypeComponentId>, index: usize) {
        loop {
            {
                let mut access = self.access.lock().await;
                if access.is_compatible(other) {
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
        Self {
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

        Self {
            access: Default::default(),
            active_access: Default::default(),
            access_updated_recv,
            access_updated_send,
        }
    }
}

struct ExecutorParallel {
    pub(crate) base_schedule: Schedule,
    shared_access: SharedSystemAccess,
}

impl ExecutorParallel {
    pub fn new() -> Self {
        Self {
            base_schedule: Schedule::new(),
            shared_access: Default::default(),
        }
    }

    pub fn run(&mut self, world: &mut World) {
        let compute_pool = world
            .get_resource_or_insert_with(|| ComputeTaskPool(TaskPool::default()))
            .clone();

        compute_pool.scope(|scope| {
            let task = unsafe {
                self.base_schedule
                    .run_unsafe(world, scope, &self.shared_access)
            };
            scope.spawn_local(async move {
                task.await;
            });
        });
    }

    pub fn add_system<Params>(&mut self, system: impl IntoSystemDescriptor<Params>) -> &mut Self {
        self.base_schedule.add_system(system);
        self
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        entity::Entity,
        query::{ChangeTrackers, Changed},
        schedule::{
            BoxedSystemLabel, ExclusiveSystemDescriptorCoercion, ParallelSystemDescriptorCoercion,
            RunCriteria, RunCriteriaDescriptorCoercion, RunCriteriaPiping, ShouldRun,
            SingleThreadedExecutor, Stage, SystemSet, SystemStage,
        },
        system::{In, IntoExclusiveSystem, IntoSystem, Local, Query, ResMut},
        world::World,
    };

    fn make_exclusive(tag: usize) -> impl FnMut(&mut World) {
        move |world| world.get_resource_mut::<Vec<usize>>().unwrap().push(tag)
    }

    fn make_parallel(tag: usize) -> impl FnMut(ResMut<Vec<usize>>) {
        move |mut resource: ResMut<Vec<usize>>| resource.push(tag)
    }

    #[test]
    fn run_parallel_system() {
        let mut world = World::new();
        world.insert_resource(Vec::<usize>::new());

        let mut executor = ExecutorParallel::new();
        executor.add_system(make_parallel(0));

        executor.run(&mut world);

        assert_eq!(*world.get_resource::<Vec<usize>>().unwrap(), vec![0]);
    }

    #[test]
    fn run_exclusive_system() {
        let mut world = World::new();
        world.insert_resource(Vec::<usize>::new());

        let mut executor = ExecutorParallel::new();
        executor.add_system(make_exclusive(0));

        executor.run(&mut world);

        assert_eq!(*world.get_resource::<Vec<usize>>().unwrap(), vec![0]);
    }
}
