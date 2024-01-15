use bevy_ecs::{
    prelude::*,
    schedule::{
        common_conditions::run_once as run_once_condition, run_enter_schedule, ScheduleLabel,
    },
};

use crate::prelude::*;

pub trait WorldAppExt {
    /// Setup the application to manage events of type `T`.
    ///
    /// This is done by adding a [`Resource`] of type [`Events::<T>`],
    /// and inserting an [`event_update_system`] into [`First`].
    ///
    /// See [`Events`] for defining events.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bevy_app::prelude::*;
    /// # use bevy_ecs::prelude::*;
    /// #
    /// # #[derive(Event)]
    /// # struct MyEvent;
    /// # let mut app = App::new();
    /// #
    /// app.add_event::<MyEvent>();
    /// ```
    ///
    /// [`event_update_system`]: bevy_ecs::event::event_update_system
    fn add_event<T>(&mut self) -> &mut Self
    where
        T: Event;

    /// Adds a system to the given schedule in this app's [`Schedules`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use bevy_app::prelude::*;
    /// # use bevy_ecs::prelude::*;
    /// #
    /// # let mut app = App::new();
    /// # fn system_a() {}
    /// # fn system_b() {}
    /// # fn system_c() {}
    /// # fn should_run() -> bool { true }
    /// #
    /// app.add_systems(Update, (system_a, system_b, system_c));
    /// app.add_systems(Update, (system_a, system_b).run_if(should_run));
    /// ```
    fn add_systems<M>(
        &mut self,
        schedule: impl ScheduleLabel,
        systems: impl IntoSystemConfigs<M>,
    ) -> &mut Self;

    /// Initializes a [`State`] with standard starting values.
    ///
    /// If the [`State`] already exists, nothing happens.
    ///
    /// Adds [`State<S>`] and [`NextState<S>`] resources, [`OnEnter`] and [`OnExit`] schedules
    /// for each state variant (if they don't already exist), an instance of [`apply_state_transition::<S>`] in
    /// [`StateTransition`] so that transitions happen before [`Update`](crate::Update) and
    /// a instance of [`run_enter_schedule::<S>`] in [`StateTransition`] with a
    /// [`run_once`](`run_once_condition`) condition to run the on enter schedule of the
    /// initial state.
    ///
    /// If you would like to control how other systems run based on the current state,
    /// you can emulate this behavior using the [`in_state`] [`Condition`].
    ///
    /// Note that you can also apply state transitions at other points in the schedule
    /// by adding the [`apply_state_transition`] system manually.
    fn init_state<S: States + FromWorld>(&mut self) -> &mut Self;

    /// Inserts a specific [`State`] to the current [`App`] and
    /// overrides any [`State`] previously added of the same type.
    ///
    /// Adds [`State<S>`] and [`NextState<S>`] resources, [`OnEnter`] and [`OnExit`] schedules
    /// for each state variant (if they don't already exist), an instance of [`apply_state_transition::<S>`] in
    /// [`StateTransition`] so that transitions happen before [`Update`](crate::Update) and
    /// a instance of [`run_enter_schedule::<S>`] in [`StateTransition`] with a
    /// [`run_once`](`run_once_condition`) condition to run the on enter schedule of the
    /// initial state.
    ///
    /// If you would like to control how other systems run based on the current state,
    /// you can emulate this behavior using the [`in_state`] [`Condition`].
    ///
    /// Note that you can also apply state transitions at other points in the schedule
    /// by adding the [`apply_state_transition`] system manually.
    fn insert_state<S: States>(&mut self, state: S) -> &mut Self;

    /// Initializes a new empty `schedule` to the [`App`] under the provided `label` if it does not exists.
    ///
    /// See [`App::add_schedule`] to pass in a pre-constructed schedule.
    fn init_schedule(&mut self, label: impl ScheduleLabel) -> &mut Self;

    /// Configures a collection of system sets in the provided schedule, adding any sets that do not exist.
    #[track_caller]
    fn configure_sets(
        &mut self,
        schedule: impl ScheduleLabel,
        sets: impl IntoSystemSetConfigs,
    ) -> &mut Self;

    /// Registers the type `T` in the [`TypeRegistry`](bevy_reflect::TypeRegistry) resource,
    /// adding reflect data as specified in the [`Reflect`](bevy_reflect::Reflect) derive:
    /// ```ignore (No serde "derive" feature)
    /// #[derive(Component, Serialize, Deserialize, Reflect)]
    /// #[reflect(Component, Serialize, Deserialize)] // will register ReflectComponent, ReflectSerialize, ReflectDeserialize
    /// ```
    ///
    /// See [`bevy_reflect::TypeRegistry::register`].
    #[cfg(feature = "bevy_reflect")]
    fn register_type<T: bevy_reflect::GetTypeRegistration>(&mut self) -> &mut Self;

    /// Adds the type data `D` to type `T` in the [`TypeRegistry`](bevy_reflect::TypeRegistry) resource.
    ///
    /// Most of the time [`App::register_type`] can be used instead to register a type you derived [`Reflect`](bevy_reflect::Reflect) for.
    /// However, in cases where you want to add a piece of type data that was not included in the list of `#[reflect(...)]` type data in the derive,
    /// or where the type is generic and cannot register e.g. `ReflectSerialize` unconditionally without knowing the specific type parameters,
    /// this method can be used to insert additional type data.
    ///
    /// # Example
    /// ```
    /// use bevy_app::App;
    /// use bevy_reflect::{ReflectSerialize, ReflectDeserialize};
    ///
    /// App::new()
    ///     .register_type::<Option<String>>()
    ///     .register_type_data::<Option<String>, ReflectSerialize>()
    ///     .register_type_data::<Option<String>, ReflectDeserialize>();
    /// ```
    ///
    /// See [`bevy_reflect::TypeRegistry::register_type_data`].
    #[cfg(feature = "bevy_reflect")]
    fn register_type_data<
        T: bevy_reflect::Reflect + bevy_reflect::TypePath,
        D: bevy_reflect::TypeData + bevy_reflect::FromType<T>,
    >(
        &mut self,
    ) -> &mut Self;
}

impl WorldAppExt for World {
    fn add_event<T>(&mut self) -> &mut Self
    where
        T: Event,
    {
        if !self.contains_resource::<Events<T>>() {
            self.init_resource::<Events<T>>().add_systems(
                First,
                bevy_ecs::event::event_update_system::<T>
                    .run_if(bevy_ecs::event::event_update_condition::<T>),
            );
        }
        self
    }

    fn add_systems<M>(
        &mut self,
        schedule: impl ScheduleLabel,
        systems: impl IntoSystemConfigs<M>,
    ) -> &mut Self {
        let schedule = schedule.intern();
        let mut schedules = self.resource_mut::<Schedules>();

        if let Some(schedule) = schedules.get_mut(schedule) {
            schedule.add_systems(systems);
        } else {
            let mut new_schedule = Schedule::new(schedule);
            new_schedule.add_systems(systems);
            schedules.insert(new_schedule);
        }

        self
    }

    fn init_state<S: States + FromWorld>(&mut self) -> &mut Self {
        if !self.contains_resource::<State<S>>() {
            self.init_resource::<State<S>>()
                .init_resource::<NextState<S>>()
                .add_event::<StateTransitionEvent<S>>()
                .add_systems(
                    StateTransition,
                    (
                        run_enter_schedule::<S>.run_if(run_once_condition()),
                        apply_state_transition::<S>,
                    )
                        .chain(),
                );
        }

        // The OnEnter, OnExit, and OnTransition schedules are lazily initialized
        // (i.e. when the first system is added to them), and World::try_run_schedule is used to fail
        // gracefully if they aren't present.

        self
    }

    fn insert_state<S: States>(&mut self, state: S) -> &mut Self {
        self.insert_resource(State::new(state))
            .init_resource::<NextState<S>>()
            .add_event::<StateTransitionEvent<S>>()
            .add_systems(
                StateTransition,
                (
                    run_enter_schedule::<S>.run_if(run_once_condition()),
                    apply_state_transition::<S>,
                )
                    .chain(),
            );

        // The OnEnter, OnExit, and OnTransition schedules are lazily initialized
        // (i.e. when the first system is added to them), and World::try_run_schedule is used to fail
        // gracefully if they aren't present.

        self
    }

    fn init_schedule(&mut self, label: impl ScheduleLabel) -> &mut Self {
        let label = label.intern();
        let mut schedules = self.resource_mut::<Schedules>();
        if !schedules.contains(label) {
            schedules.insert(Schedule::new(label));
        }
        self
    }

    #[track_caller]
    fn configure_sets(
        &mut self,
        schedule: impl ScheduleLabel,
        sets: impl IntoSystemSetConfigs,
    ) -> &mut Self {
        let schedule = schedule.intern();
        let mut schedules = self.resource_mut::<Schedules>();
        if let Some(schedule) = schedules.get_mut(schedule) {
            schedule.configure_sets(sets);
        } else {
            let mut new_schedule = Schedule::new(schedule);
            new_schedule.configure_sets(sets);
            schedules.insert(new_schedule);
        }
        self
    }

    #[cfg(feature = "bevy_reflect")]
    fn register_type<T: bevy_reflect::GetTypeRegistration>(&mut self) -> &mut Self {
        let registry = self.resource_mut::<AppTypeRegistry>();
        registry.write().register::<T>();
        self
    }

    #[cfg(feature = "bevy_reflect")]
    fn register_type_data<
        T: bevy_reflect::Reflect + bevy_reflect::TypePath,
        D: bevy_reflect::TypeData + bevy_reflect::FromType<T>,
    >(
        &mut self,
    ) -> &mut Self {
        let registry = self.resource_mut::<AppTypeRegistry>();
        registry.write().register_type_data::<T, D>();
        self
    }
}
