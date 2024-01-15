#[allow(clippy::module_inception)]
mod camera;
mod camera_driver_node;
mod clear_color;
mod manual_texture_view;
mod projection;

pub use camera::*;
pub use camera_driver_node::*;
pub use clear_color::*;
pub use manual_texture_view::*;
pub use projection::*;

use crate::{
    extract_resource::ExtractResourcePlugin, render_graph::RenderGraph, ExtractSchedule, Render,
    RenderApp, RenderSet,
};
use bevy_app::{
    AppLabel, PluginGroup, WorldAppExt, WorldPlugin, WorldPluginExt, WorldPluginHolder,
};
use bevy_ecs::{schedule::IntoSystemConfigs, world::World};

#[derive(Default)]
pub struct CameraPlugin;

impl WorldPlugin for CameraPlugin {
    fn build(&self, world: &mut World) {
        world
            .register_type::<Camera>()
            .register_type::<Viewport>()
            .register_type::<Option<Viewport>>()
            .register_type::<ScalingMode>()
            .register_type::<CameraRenderGraph>()
            .register_type::<RenderTarget>()
            .register_type::<ClearColor>()
            .register_type::<ClearColorConfig>()
            .init_resource::<ManualTextureViews>()
            .init_resource::<ClearColor>();
        world
            .add_plugin(CameraProjectionPlugin::<Projection>::default())
            .add_plugin(CameraProjectionPlugin::<OrthographicProjection>::default())
            .add_plugin(CameraProjectionPlugin::<PerspectiveProjection>::default());
    }
}

#[derive(Default)]
pub struct CameraRenderPlugin;

impl WorldPlugin for CameraRenderPlugin {
    fn world(&self) -> Option<bevy_utils::intern::Interned<dyn bevy_app::AppLabel>> {
        Some(RenderApp.intern())
    }

    fn build(&self, world: &mut World) {
        world
            .add_plugin(ExtractResourcePlugin::<ManualTextureViews>::default())
            .add_plugin(ExtractResourcePlugin::<ClearColor>::default())
            .init_resource::<SortedCameras>()
            .add_systems(ExtractSchedule, extract_cameras)
            .add_systems(Render, sort_cameras.in_set(RenderSet::ManageViews));
        let camera_driver_node = CameraDriverNode::new(world);
        let mut render_graph = world.resource_mut::<RenderGraph>();
        render_graph.add_node(crate::main_graph::node::CAMERA_DRIVER, camera_driver_node);
    }
}

pub struct CameraPlugins;
impl PluginGroup for CameraPlugins {
    fn build(self) -> bevy_app::PluginGroupBuilder {
        self.build()
            .add(WorldPluginHolder::from(CameraPlugin))
            .add(WorldPluginHolder::from(CameraRenderPlugin))
    }
}
