use crate as bevy_ecs;
use crate::change_detection::{Mut, MutUntyped};
use crate::component::{ComponentId, Tick};
use crate::entity::{Entities, Entity};
use crate::prelude::{Component, QueryState};
use crate::system::{CommandQueue, Commands, Deferred, SystemMeta, SystemParam, Local};
use crate::world::unsafe_world_cell::UnsafeWorldCell;
use crate::world::World;
use bevy_ecs_macros::Resource;

#[derive(Resource)]
pub struct ScheduleWorld {
    world: World,
    num_systems: usize,
    system_map: Vec<Entity>,
}

impl ScheduleWorld {
    pub fn new(num_systems: usize) -> Self {
        let mut world = World::default();
        let mut system_map = Vec::with_capacity(num_systems);

        // prespawn an entity for every system
        for _ in 0..num_systems {
            system_map.push(world.spawn(()).id());
        }

        ScheduleWorld {
            world,
            num_systems,
            system_map,
        }
    }

    pub fn new_schedule_data<T: Default + Component>(&mut self, system_index: usize) {
        let entity = self.system_map[system_index];

        self.world
            .get_entity_mut(entity)
            .unwrap()
            .insert(T::default());
    }

    /// # Safety
    /// TODO: make this safete comment more exact
    /// Caller must ensure this is no simultaneous access to the same data
    pub unsafe fn get_mut_by_id(&self, system_index: usize, id: ComponentId) -> MutUntyped {
        let entity = self.system_map[system_index];

        let entity_mut = self
            .world
            .as_unsafe_world_cell_migration_internal()
            .get_entity(entity)
            .unwrap();
        let data_mut = entity_mut.get_mut_by_id(id).unwrap();
        data_mut
    }

    pub fn world_mut(&mut self) -> &mut World {
        &mut self.world
    }
}

#[derive(Component, Default)]
pub struct ComponentCommandQueue(pub CommandQueue);

pub struct ScheduleCommandQueue<'a> {
    pub queue: Mut<'a, ComponentCommandQueue>,
}

pub struct ScheduleCommandQueueState {
    schedule_data_id: ComponentId,
    command_queue_id: ComponentId,
}

#[derive(SystemParam)]
pub struct CommandsV2<'w> {
    pub queue: ScheduleCommandQueue<'w>,
    pub entities: &'w Entities,
}

impl<'w> CommandsV2<'w> {
    pub fn into_commands(&mut self) -> Commands {
        Commands::new_from_entities(&mut self.queue.queue.0, self.entities)
    }
}

// SAFETY: TODO
unsafe impl<'a> SystemParam for ScheduleCommandQueue<'a> {
    type State = ScheduleCommandQueueState;
    type Item<'w, 's> = ScheduleCommandQueue<'w>;

    fn init_state(world: &mut World, system_meta: &mut SystemMeta) -> Self::State {
        let schedule_data_id = world.initialize_resource::<ScheduleWorld>();

        let mut schedule_world = world.get_resource_mut::<ScheduleWorld>().unwrap();
        let schedule_world = schedule_world.as_mut();
        let command_queue_id = schedule_world
            .world
            .init_component::<ComponentCommandQueue>();
        schedule_world
            .new_schedule_data::<ComponentCommandQueue>(system_meta.schedule_index.unwrap()); //TODO: get the system index from somewhere

        ScheduleCommandQueueState {
            schedule_data_id,
            command_queue_id,
        }
    }

    unsafe fn get_param<'w, 's>(
        state: &'s mut Self::State,
        system_meta: &SystemMeta,
        world: UnsafeWorldCell<'w>,
        _change_tick: Tick,
    ) -> Self::Item<'w, 's> {
        let schedule_world = world
            .get_resource_mut_by_id(state.schedule_data_id)
            .unwrap();
        let schedule_world: &mut ScheduleWorld = schedule_world.value.deref_mut();
        let queue = schedule_world
            .get_mut_by_id(system_meta.schedule_index.unwrap(), state.command_queue_id); // TODO: get the systems index from somewhere                                                              // let queue = queue2.as_mut();

        ScheduleCommandQueue {
            queue: queue.with_type(),
        }
    }
}

pub fn apply_commands(world: &mut World) {
    world.resource_scope(|world, mut schedule_world: Mut<ScheduleWorld>| {
        // TODO: figure out a place to cache this data
        let mut state = QueryState::<&mut ComponentCommandQueue>::new(schedule_world.world_mut());
        for mut queue in state.iter_mut(schedule_world.world_mut()) {
            queue.0.apply(world);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::apply_commands;
    use crate as bevy_ecs;
    use crate::prelude::{Resource, World};
    use crate::schedule::schedule_world::ScheduleWorld;
    use crate::schedule::{schedule_world::CommandsV2, IntoSystemConfigs, Schedule};

    #[test]
    fn it_can_apply_buffers() {
        let mut schedule = Schedule::new();
        let mut world = World::new();
        world.insert_resource(ScheduleWorld::new(2));

        #[derive(Resource)]
        struct TestResource;

        fn do_some_commands(mut commands: CommandsV2) {
            commands.into_commands().insert_resource(TestResource);
        }
        schedule.add_systems((do_some_commands, apply_commands).chain());

        schedule.run(&mut world);

        assert!(world.get_resource::<TestResource>().is_some());
    }
}
