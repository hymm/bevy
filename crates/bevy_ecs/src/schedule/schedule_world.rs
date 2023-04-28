use crate as bevy_ecs;
use crate::change_detection::{Mut, MutUntyped};
use crate::component::{ComponentId, Tick};
use crate::entity::{Entities, Entity};
use crate::prelude::{Component, QueryState};
use crate::system::{CommandQueue, Commands, SystemMeta, SystemParam};
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

#[derive(SystemParam)]
pub struct CommandsV2<'w> {
    pub queue: ScheduleData<'w, ComponentCommandQueue>,
    pub entities: &'w Entities,
}

impl<'w> CommandsV2<'w> {
    pub fn into_commands(&mut self) -> Commands {
        Commands::new_from_entities(&mut self.queue.data.0, self.entities)
    }
}

pub struct ScheduleData<'a, T>
where
    T: Component + Default,
{
    data: &'a mut T,
}

pub struct ScheduleDataState {
    schedule_data_id: ComponentId,
    command_queue_id: ComponentId,
}

// SAFETY: TODO.
unsafe impl<'a, T> SystemParam for ScheduleData<'a, T>
where
    T: Component + Default,
{
    type State = ScheduleDataState;
    type Item<'w, 's> = ScheduleData<'w, T>;

    fn init_state(world: &mut World, system_meta: &mut SystemMeta) -> Self::State {
        let schedule_data_id = world.initialize_resource::<ScheduleWorld>();

        let mut schedule_world = world.get_resource_mut::<ScheduleWorld>().unwrap();
        let schedule_world = schedule_world.as_mut();
        let command_queue_id = schedule_world.world.init_component::<T>();
        schedule_world.new_schedule_data::<T>(system_meta.schedule_index.unwrap()); //TODO: get the system index from somewhere

        ScheduleDataState {
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
        let data = schedule_world
            .get_mut_by_id(system_meta.schedule_index.unwrap(), state.command_queue_id); // TODO: get the systems index from somewhere                                                              // let queue = queue2.as_mut();

        // TODO: this will always trigger change detection. might be better to have a into_inner_bypass and
        // reimplement change detection on ScheduleData
        ScheduleData {
            data: data.with_type::<T>().into_inner(),
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
    use crate::schedule::{schedule_world::CommandsV2, IntoSystemConfigs, Schedule};

    #[test]
    fn it_can_apply_buffers() {
        let mut schedule = Schedule::new();
        let mut world = World::new();

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
