use crate::{world::World, schedule::{ParallelSystemContainer, SystemDescriptor}};
use bevy_tasks::{ComputeTaskPool, Scope, TaskPool};

// use std::pin::Pin;
use futures_lite::pin;
struct ExecutorParallel {
  systems_modified: bool,
  uninitialized_parallel: Vec<usize>,
  systems: Vec<ParallelSystemContainer>,
}

impl ExecutorParallel {
    fn run(&mut self, world: &mut World) {
        let compute_pool = world
            .get_resource_or_insert_with(|| ComputeTaskPool(TaskPool::default()))
            .clone();

        pin!(world);

        compute_pool.scope(|scope| {
            let system = unsafe { system.system_mut_unsafe() };
            let task = async move {
                unsafe { system.run_unsafe((), world )};
            };
        });
    }

    fn add_system(&mut self, system: SystemDescriptor, default_run_criteria: Option<usize>) {
      self.systems_modified = true;
      match system {
          SystemDescriptor::Exclusive(mut descriptor) => {
              let insertion_point = descriptor.insertion_point;
              let criteria = descriptor.run_criteria.take();
              let mut container = ParallelSystemContainer::from_exclusive_descriptor(descriptor);
              self.uninitialized_parallel.push(self.systems.len());
              self.systems.push(container);
          }
          SystemDescriptor::Parallel(mut descriptor) => {
              let criteria = descriptor.run_criteria.take();
              let mut container = ParallelSystemContainer::from_descriptor(descriptor);

              self.uninitialized_parallel.push(self.systems.len());
              self.systems.push(container);
          }
      }
  }
}
