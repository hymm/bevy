use bevy_asset::{AssetServer, UntypedAssetId};
use bevy_ecs::{
    resource::Resource,
    system::{Commands, Local, Res},
    world::World,
};

/// Some shaders should be ready before the render world runs.
/// Register them with this resource.
#[derive(Resource, Default)]
pub struct RequiredRenderAssets {
    asset_ids: Vec<UntypedAssetId>,
}

impl RequiredRenderAssets {
    pub fn add(&mut self, id: UntypedAssetId) {
        self.asset_ids.push(id);
    }
}

// run condition to run render schedule
pub fn required_assets_ready(
    asset_server: Res<AssetServer>,
    required_assets: Res<RequiredRenderAssets>,
    mut cached: Local<Option<bool>>,
) -> bool {
    if let Some(cached) = *cached
        && cached
    {
        true
    } else {
        let mut ready = true;
        for id in &required_assets.asset_ids {
            if !asset_server.is_loaded_with_dependencies(*id) {
                ready = false;
            }
        }
        if ready {
            *cached = Some(true);
        }
        ready
    }
}

pub trait RegisterRequiredRenderAssets {
    fn add_required_asset(&mut self, id: impl Into<UntypedAssetId>);
}

impl RegisterRequiredRenderAssets for Commands<'_, '_> {
    fn add_required_asset(&mut self, id: impl Into<UntypedAssetId>) {
        let untyped_id = id.into();
        self.queue(move |world: &mut World| {
            world.resource_mut::<RequiredRenderAssets>().add(untyped_id);
        });
    }
}
