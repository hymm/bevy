use bevy_ecs::{schedule::{InternedSystemSet, InternedScheduleLabel, ScheduleLabel, SystemConfigs, IntoSystemConfigs}, world::World};
use downcast_rs::{impl_downcast, Downcast};

use crate::{App, Update};
use std::any::Any;

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

// /// A plugin that only acts an a certain world.
// pub trait WorldPlugin {
//     /// The sub app label of teh world that this plugin acts on. If this returns None it acts on the main schedule.
//     fn world() -> Option<InternedAppLabel> {
//         None
//     }

//     /// Configures the `World` this plugin belongs to.
//     fn build(&self, _app: &mut World);
// }

// impl<T> Plugin for T
// where
//     T: WorldPlugin + Send + Sync + 'static,
// {
//     fn build(&self, app: &mut App) {
//         self.build(&mut app.world);
//     }
// }

/// a plugin that returns a system set. Scheduled into the configured
/// sets by default, but can change that.
pub trait SchedulablePlugin {
    /// this is the default schedule the plugin is added to.
    /// If None it is added to Update
    fn default_schedule() -> Option<InternedScheduleLabel> {
        None
    }

    /// this is the default system set the plugin is added to.
    /// If None it is not added to any system set
    fn default_system_set() -> Option<InternedSystemSet> {
        None
    }

    /// set this to false if you only provide systems
    fn is_unique(&self) -> bool {
        true
    }

    fn build(&self, app: &mut App) -> SystemConfigs;
}

impl <T> Plugin for T
where
    T: SchedulablePlugin + Send + Sync + 'static,
{
    fn build(&self, app: &mut App) {
        let mut systems = self.build(app);
        if let Some(system_set) = Self::default_system_set() {
            systems = systems.in_set(system_set);
        }
        let schedule = Self::default_schedule().unwrap_or(Update.intern());
        app.add_systems(schedule, systems);
    }
}

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

#[cfg(test)]
mod tests {
    use bevy_ecs::schedule::{SystemSet, common_conditions::run_once};

    use super::*;

    #[test]
    fn test_schedule_plugin() {
        struct MyPlugin;
        impl SchedulablePlugin for MyPlugin {
            fn build(&self, _app: &mut crate::App) -> bevy_ecs::schedule::SystemConfigs {
                // my_func.into_configs()
                (|| {}).into_configs()
            }
        }

        #[derive(SystemSet, Hash, PartialEq, Eq, Debug, Clone)]
        struct MySystemSet;

        let mut app = App::new();
        // can add plugin normally
        app.add_plugins(MyPlugin);

        // can get the systems
        let systems = SchedulablePlugin::build(&MyPlugin, &mut app);
        app.add_systems(Update, systems.in_set(MySystemSet));

        // can call it multiple times
        // TODO: this is bad because we bypassed is_unique.
        let systems = SchedulablePlugin::build(&MyPlugin, &mut app);
        app.add_systems(Update, systems.run_if(run_once()));
    }
}
