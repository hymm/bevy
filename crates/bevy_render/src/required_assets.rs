use bevy_asset::{AssetServer, UntypedAssetId};
use bevy_ecs::{
    resource::Resource,
    system::{Local, Res},
};

/// Some shaders should be ready before the render world runs.
/// Register them with this resource.
#[derive(Resource, Default)]
pub struct RequiredRenderAssets {
    asset_ids: Vec<UntypedAssetId>,
}

impl RequiredRenderAssets {
    pub fn add(&mut self, id: impl Into<UntypedAssetId>) {
        self.asset_ids.push(id.into());
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
