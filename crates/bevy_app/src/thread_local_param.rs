use core::marker::PhantomData;
use std::thread::LocalKey;

use bevy_ecs::system::SystemParam;

/// System Param wrapper around [`LocalKey`]
pub struct Tls<T: 'static>(LocalKey<T>);

// SAFETY: TODO
#[expect(unsafe_code, reason = "We make system param")]
unsafe impl<T> SystemParam for Tls<T> {
    type State = ();

    type Item<'world, 'state> = Tls<T>;

    fn init_state(_world: &mut bevy_ecs::world::World) -> Self::State {}

    fn init_access(
        _state: &Self::State,
        _system_meta: &mut bevy_ecs::system::SystemMeta,
        _component_access_set: &mut bevy_ecs::query::FilteredAccessSet,
        _world: &mut bevy_ecs::world::World,
    ) {
        // only one type of `T` is allowed per thread
    }

    unsafe fn get_param<'world, 'state>(
        _state: &'state mut Self::State,
        _system_meta: &bevy_ecs::system::SystemMeta,
        _world: bevy_ecs::world::unsafe_world_cell::UnsafeWorldCell<'world>,
        _change_tick: bevy_ecs::change_detection::Tick,
    ) -> Self::Item<'world, 'state> {
        Self(LocalKey<T>::new())
    }
}
