#[allow(clippy::module_inception)]
mod mesh;
pub mod morph;
/// Generation for some primitive shape meshes.
pub mod shape;

pub use mesh::*;

use crate::{prelude::Image, render_asset::RenderAssetPlugin, RenderApp};
use bevy_app::{AppLabel, PluginGroup, WorldAppExt, WorldPlugin, WorldPluginExt, WorldPluginHolder};
use bevy_asset::{AssetApp, Handle};
use bevy_ecs::{entity::Entity, world::World};

/// Adds the [`Mesh`] as an asset and makes sure that they are extracted and prepared for the GPU.
pub struct MeshPlugin;

impl WorldPlugin for MeshPlugin {
    fn build(&self, world: &mut World) {
        world.init_asset::<Mesh>()
            .init_asset::<skinning::SkinnedMeshInverseBindposes>()
            .register_asset_reflect::<Mesh>()
            .register_type::<Option<Handle<Image>>>()
            .register_type::<Option<Vec<String>>>()
            .register_type::<Option<Indices>>()
            .register_type::<Indices>()
            .register_type::<skinning::SkinnedMesh>()
            .register_type::<Vec<Entity>>()
            // 'Mesh' must be prepared after 'Image' as meshes rely on the morph target image being ready
            ;
    }
}

pub struct MeshRenderPlugin;
impl WorldPlugin for MeshRenderPlugin {
    fn world(&self) -> Option<bevy_utils::intern::Interned<dyn AppLabel>> {
        Some(RenderApp.intern())
    }

    fn build(&self, world: &mut bevy_ecs::world::World) {
        world.add_plugin(RenderAssetPlugin::<Mesh, Image>::default());
    }
}

pub struct MeshPlugins;
impl PluginGroup for MeshPlugins {
    fn build(self) -> bevy_app::PluginGroupBuilder {
        self.build()
            .add(WorldPluginHolder::from(MeshPlugin))
            .add(WorldPluginHolder::from(MeshRenderPlugin))
    }
}
