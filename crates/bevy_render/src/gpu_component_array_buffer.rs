use crate::{
    render_resource::{GpuArrayBuffer, GpuArrayBufferable},
    renderer::{RenderDevice, RenderQueue},
    Render, RenderApp, RenderSet,
};
use bevy_app::{AppLabel, WorldPlugin, WorldAppExt};
use bevy_ecs::{
    prelude::{Component, Entity},
    schedule::IntoSystemConfigs,
    system::{Commands, Query, Res, ResMut}, world::World,
};
use std::marker::PhantomData;

/// This plugin prepares the components of the corresponding type for the GPU
/// by storing them in a [`GpuArrayBuffer`].
pub struct GpuComponentArrayBufferPlugin<C: Component + GpuArrayBufferable>(PhantomData<C>);

impl<C: Component + GpuArrayBufferable> WorldPlugin for GpuComponentArrayBufferPlugin<C> {
    fn world(&self) -> Option<bevy_utils::intern::Interned<dyn AppLabel>> {
        Some(RenderApp.intern())
    }

    fn build(&self, world: &mut World) {
        world.add_systems(
            Render,
            prepare_gpu_component_array_buffers::<C>.in_set(RenderSet::PrepareResources),
        );

        world.insert_resource(GpuArrayBuffer::<C>::new(
            world.resource::<RenderDevice>(),
        ));
    }
}

impl<C: Component + GpuArrayBufferable> Default for GpuComponentArrayBufferPlugin<C> {
    fn default() -> Self {
        Self(PhantomData::<C>)
    }
}

fn prepare_gpu_component_array_buffers<C: Component + GpuArrayBufferable>(
    mut commands: Commands,
    render_device: Res<RenderDevice>,
    render_queue: Res<RenderQueue>,
    mut gpu_array_buffer: ResMut<GpuArrayBuffer<C>>,
    components: Query<(Entity, &C)>,
) {
    gpu_array_buffer.clear();

    let entities = components
        .iter()
        .map(|(entity, component)| (entity, gpu_array_buffer.push(component.clone())))
        .collect::<Vec<_>>();
    commands.insert_or_spawn_batch(entities);

    gpu_array_buffer.write_buffer(&render_device, &render_queue);
}
