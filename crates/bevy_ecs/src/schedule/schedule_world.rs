use std::ops::{Deref, DerefMut};

use crate as bevy_ecs;
use crate::change_detection::{Mut, MutUntyped};
use crate::component::{ComponentId, Tick};
use crate::entity::Entity;
use crate::prelude::{Component, QueryState};
use crate::system::{CommandQueue, SystemMeta, SystemParam};
use crate::world::unsafe_world_cell::UnsafeWorldCell;
use crate::world::{World, WorldId};
use bevy_ecs_macros::Resource;

#[derive(Resource)]
pub struct ScheduleWorld {
    world: World,
}

impl ScheduleWorld {
    pub fn new() -> Self {
        let world = World::default();

        ScheduleWorld { world }
    }

    pub fn new_schedule_data<T: Default + Component + SystemBufferV2>(&mut self) -> Entity {
        self.world.spawn(T::default()).id()
    }

    /// # Safety
    /// TODO: make this safete comment more exact
    /// Caller must ensure this is no simultaneous access to the same data
    pub unsafe fn get_mut_by_entity(&self, entity: Entity, id: ComponentId) -> MutUntyped {
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

impl SystemBufferV2 for ComponentCommandQueue {
    fn apply(&mut self, world: &mut World) {
        self.0.apply(world);
    }
}

pub struct ScheduleData<'a, T>
where
    T: Component + Default,
{
    data: &'a mut T,
}

impl<'a, T> ScheduleData<'a, T>
where
    T: Component + Default,
{
    pub fn new(data: &'a mut T) -> Self {
        ScheduleData { data }
    }
}

impl<'a, T> Deref for ScheduleData<'a, T>
where
    T: Component + Default,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a, T> DerefMut for ScheduleData<'a, T>
where
    T: Component + Default,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

pub struct ScheduleDataState {
    entity: Entity,
    schedule_data_id: ComponentId,
    command_queue_id: ComponentId,
    world_id: WorldId,
}

// SAFETY: TODO.
unsafe impl<'a, T> SystemParam for ScheduleData<'a, T>
where
    T: Component + Default + SystemBufferV2,
{
    type State = ScheduleDataState;
    type Item<'w, 's> = ScheduleData<'w, T>;

    fn init_state(world: &mut World, _system_meta: &mut SystemMeta) -> Self::State {
        let schedule_data_id = world.initialize_resource::<ScheduleWorld>();

        let mut schedule_world =
            world.get_resource_or_insert_with::<ScheduleWorld>(ScheduleWorld::new);
        let schedule_world = schedule_world.as_mut();
        let command_queue_id = schedule_world.world.init_component::<T>();
        let entity = schedule_world.new_schedule_data::<T>();

        ScheduleDataState {
            entity,
            schedule_data_id,
            command_queue_id,
            world_id: schedule_world.world.id(),
        }
    }

    unsafe fn get_param<'w, 's>(
        state: &'s mut Self::State,
        _system_meta: &SystemMeta,
        world: UnsafeWorldCell<'w>,
        _change_tick: Tick,
    ) -> Self::Item<'w, 's> {
        let schedule_world = world
            .get_resource_mut_by_id(state.schedule_data_id)
            .unwrap();
        let schedule_world: &mut ScheduleWorld = schedule_world.value.deref_mut();

        if schedule_world.world.id() != state.world_id {
            panic!(
                "schedule world {:?} does not match what param was initialized with {:?}",
                state.world_id,
                schedule_world.world.id()
            );
        }

        let data = schedule_world.get_mut_by_entity(state.entity, state.command_queue_id); // TODO: get the systems index from somewhere                                                              // let queue = queue2.as_mut();

        // TODO: this will always trigger change detection. might be better to have a into_inner_bypass and
        // reimplement change detection on ScheduleData
        ScheduleData {
            data: data.with_type::<T>().into_inner(),
        }
    }

    // TODO: think about if the apply method should be removed from SystemParam.
    fn apply(state: &mut Self::State, _system_meta: &SystemMeta, world: &mut World) {
        world.resource_scope(|world, mut schedule_world: Mut<ScheduleWorld>| {
            if schedule_world.world.id() != state.world_id {
                panic!(
                    "schedule world {:?} does not match what param was initialized with {:?}",
                    state.world_id,
                    schedule_world.world.id()
                );
            }

            // TODO: could this be done unsafely for perf?
            let mut data = schedule_world.world.get_mut::<T>(state.entity).unwrap();

            data.apply(world);
        });
    }
}

pub trait SystemBufferV2 {
    fn apply(&mut self, world: &mut World);
}

pub fn apply_schedule_data<T>(world: &mut World)
where
    T: Component + SystemBufferV2,
{
    world.resource_scope(|world, mut schedule_world: Mut<ScheduleWorld>| {
        // TODO: figure out a place to cache this data
        let mut state = QueryState::<&mut T>::new(schedule_world.world_mut());
        for mut queue in state.iter_mut(schedule_world.world_mut()) {
            queue.apply(world);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::apply_schedule_data;
    use crate as bevy_ecs;
    use crate::prelude::{Resource, World};
    use crate::schedule::{IntoSystemConfigs, Schedule};
    use crate::system::{CommandQueue, Commands};

    #[test]
    fn it_can_apply_buffers() {
        let mut schedule = Schedule::new();
        let mut world = World::new();

        #[derive(Resource)]
        struct TestResource;

        fn do_some_commands(mut commands: Commands) {
            commands.insert_resource(TestResource);
        }

        schedule.add_systems((do_some_commands, apply_schedule_data::<CommandQueue>).chain());

        schedule.run(&mut world);

        assert!(world.get_resource::<TestResource>().is_some());
    }
}
