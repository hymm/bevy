use bevy_ecs::{
    schedule::{ScheduleLabel, SystemConfigs},
    world::World,
};
use bevy_utils::intern::Interned;
use downcast_rs::{impl_downcast, Downcast};

use crate::{App, AppLabel};
use std::{any::Any, sync::Mutex};

/// A collection of Bevy app logic and configuration.
///
/// Plugins configure an [`App`]. When an [`App`] registers a plugin,
/// the plugin's [`Plugin::build`] function is run. By default, a plugin
/// can only be added once to an [`App`].
///
/// If the plugin may need to be added twice or more, the function [`is_unique()`](Self::is_unique)
/// should be overridden to return `false`. Plugins are considered duplicate if they have the same
/// [`name()`](Self::name). The default `name()` implementation returns the type name, which means
/// generic plugins with different type parameters will not be considered duplicates.
///
/// ## Lifecycle of a plugin
///
/// When adding a plugin to an [`App`]:
/// * the app calls [`Plugin::build`] immediately, and register the plugin
/// * once the app started, it will wait for all registered [`Plugin::ready`] to return `true`
/// * it will then call all registered [`Plugin::finish`]
/// * and call all registered [`Plugin::cleanup`]
pub trait Plugin: Downcast + Any + Send + Sync {
    /// Configures the [`App`] to which this plugin is added.
    fn build(&self, app: &mut App);

    /// Has the plugin finished its setup? This can be useful for plugins that need something
    /// asynchronous to happen before they can finish their setup, like the initialization of a renderer.
    /// Once the plugin is ready, [`finish`](Plugin::finish) should be called.
    fn ready(&self, _app: &App) -> bool {
        true
    }

    /// Finish adding this plugin to the [`App`], once all plugins registered are ready. This can
    /// be useful for plugins that depends on another plugin asynchronous setup, like the renderer.
    fn finish(&self, _app: &mut App) {
        // do nothing
    }

    /// Runs after all plugins are built and finished, but before the app schedule is executed.
    /// This can be useful if you have some resource that other plugins need during their build step,
    /// but after build you want to remove it and send it to another thread.
    fn cleanup(&self, _app: &mut App) {
        // do nothing
    }

    /// Configures a name for the [`Plugin`] which is primarily used for checking plugin
    /// uniqueness and debugging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// If the plugin can be meaningfully instantiated several times in an [`App`],
    /// override this method to return `false`.
    fn is_unique(&self) -> bool {
        true
    }
}

impl_downcast!(Plugin);

/// [`WorldPlugin`] is a plugin that only acts on one world. Using this is preferred
/// over [`AppPlugin`] when possible.
pub trait WorldPlugin: Downcast + Send + Sync + 'static {
    /// world to add the plugin to. If None it defaults to the main world.
    fn world(&self) -> Option<Interned<dyn AppLabel>> {
        None
    }

    /// add dependencies between systems.
    fn dependencies(&self, _system: SystemConfigs) {}

    /// run methods on world to initialize plugin
    /// this is run the first time `app.update()` is called.
    fn build(&self, world: &mut World);
}

impl<T> WorldPlugin for Box<T>
where
    T: WorldPlugin + ?Sized,
{
    fn build(&self, world: &mut World) {
        (**self).build(world);
    }
}

/// Add methods to world to add `WorldPlugins`
pub trait WorldPluginExt {
    /// add a [`WorldPlugin`] to the [`World`]
    fn add_plugin(&mut self, plugin: impl WorldPlugin + Send + Sync + 'static) -> &mut Self;
}

impl WorldPluginExt for World {
    fn add_plugin(&mut self, plugin: impl WorldPlugin) -> &mut Self {
        let system = move |world: &mut World| {
            plugin.build(world);
        };

        // TODO: We can add initialization ordering between different plugins by
        // adding dependencies between the different systems.
        // plugin.dependencies(system);

        self.schedule_scope(PluginInit, |_world, schedule| {
            schedule.add_systems(system);
        });

        self
    }
}

pub struct WorldPluginHolder {
    world_plugin: Mutex<Option<Box<dyn WorldPlugin>>>,
}

impl WorldPluginHolder {
    fn take_plugin(&self) -> Box<dyn WorldPlugin> {
        let mut guard = self.world_plugin.lock().unwrap();
        guard.take().unwrap()
    }
}

impl Plugin for WorldPluginHolder {
    fn build(&self, app: &mut App) {
        let plugin = self.take_plugin();
        // add the plugin to the world in the build step
        app.world.add_plugin(plugin);
    }
}

impl<T> From<T> for WorldPluginHolder
where
    T: WorldPlugin,
{
    fn from(value: T) -> Self {
        WorldPluginHolder {
            world_plugin: Mutex::new(Some(Box::new(value))),
        }
    }
}

#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash)]
struct PluginInit;

/// A type representing an unsafe function that returns a mutable pointer to a [`Plugin`].
/// It is used for dynamically loading plugins.
///
/// See `bevy_dynamic_plugin/src/loader.rs#dynamically_load_plugin`.
pub type CreatePlugin = unsafe fn() -> *mut dyn Plugin;

/// Types that represent a set of [`Plugin`]s.
///
/// This is implemented for all types which implement [`Plugin`],
/// [`PluginGroup`](super::PluginGroup), and tuples over [`Plugins`].
pub trait Plugins<Marker>: sealed::Plugins<Marker> {}

impl<Marker, T> Plugins<Marker> for T where T: sealed::Plugins<Marker> {}

mod sealed {

    use bevy_ecs::all_tuples;

    use crate::{App, AppError, Plugin, PluginGroup};

    pub trait Plugins<Marker> {
        fn add_to_app(self, app: &mut App);
    }

    pub struct PluginMarker;
    pub struct PluginGroupMarker;
    pub struct PluginsTupleMarker;

    impl<P: Plugin> Plugins<PluginMarker> for P {
        #[track_caller]
        fn add_to_app(self, app: &mut App) {
            if let Err(AppError::DuplicatePlugin { plugin_name }) =
                app.add_boxed_plugin(Box::new(self))
            {
                panic!(
                    "Error adding plugin {plugin_name}: : plugin was already added in application"
                )
            }
        }
    }

    impl<P: PluginGroup> Plugins<PluginGroupMarker> for P {
        #[track_caller]
        fn add_to_app(self, app: &mut App) {
            self.build().finish(app);
        }
    }

    macro_rules! impl_plugins_tuples {
        ($(($param: ident, $plugins: ident)),*) => {
            impl<$($param, $plugins),*> Plugins<(PluginsTupleMarker, $($param,)*)> for ($($plugins,)*)
            where
                $($plugins: Plugins<$param>),*
            {
                #[allow(non_snake_case, unused_variables)]
                #[track_caller]
                fn add_to_app(self, app: &mut App) {
                    let ($($plugins,)*) = self;
                    $($plugins.add_to_app(app);)*
                }
            }
        }
    }

    all_tuples!(impl_plugins_tuples, 0, 15, P, S);
}
