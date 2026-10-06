//! Ticks that can be stored in [`Mut`] to set and get component changes

use core::panic::Location;

use bevy_ptr::UnsafeCellDeref;

use crate::change_detection::{
    AtomicTick, ComponentTickCells, ComponentTicksRef, MaybeLocation, Tick,
};

/// Trait that must be implemented to use ticks with [`Mut`].
pub trait ChangeTicksMut<'w>: Into<ComponentTicksMutDynamic<'w>> {
    fn new(
        added: &'w mut Tick,
        changed: &'w mut Tick,
        summary_tick: Option<&'w AtomicTick>,
        last_run: Tick,
        this_run: Tick,
        caller: MaybeLocation<&'w mut &'static Location<'static>>,
    ) -> Self;

    fn added(&self) -> Tick;
    fn set_added(&mut self, new_tick: Tick);
    fn changed(&self) -> Tick;
    fn set_changed(&mut self, new_tick: Tick);
    fn changed_by(&self) -> MaybeLocation;
    fn set_changed_by(&mut self, changed_by: MaybeLocation);
    fn last_run(&self) -> Tick;
    fn set_last_run(&mut self, last_run: Tick);
    fn this_run(&self) -> Tick;
    fn set_this_run(&mut self, this_run: Tick);
    fn summary_tick(&self) -> Option<&AtomicTick>;
    unsafe fn from_tick_cells(
        cells: ComponentTickCells<'w>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self;
}

/// Used by mutable query parameters (such as [`Mut`] and [`ResMut`])
/// to store mutable access to the [`Tick`]s of a single component or resource.
pub struct ComponentTicksMut<'w> {
    pub(crate) added: &'w mut Tick,
    pub(crate) changed: &'w mut Tick,
    pub(crate) changed_by: MaybeLocation<&'w mut &'static Location<'static>>,
    pub(crate) last_run: Tick,
    pub(crate) this_run: Tick,
}
impl<'w> ChangeTicksMut<'w> for ComponentTicksMut<'w> {
    fn new(
        added: &'w mut Tick,
        changed: &'w mut Tick,
        _summary_tick: Option<&'w AtomicTick>,
        last_run: Tick,
        this_run: Tick,
        caller: MaybeLocation<&'w mut &'static Location<'static>>,
    ) -> Self {
        Self {
            added,
            changed,
            changed_by: caller,
            last_run,
            this_run,
        }
    }

    /// # Safety
    /// This should never alias the underlying ticks. All access must be unique.
    unsafe fn from_tick_cells(
        cells: ComponentTickCells<'w>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        Self {
            // SAFETY: Caller ensures there is no alias to the cell.
            added: unsafe { cells.added.deref_mut() },
            // SAFETY: Caller ensures there is no alias to the cell.
            changed: unsafe { cells.changed.deref_mut() },
            // SAFETY: Caller ensures there is no alias to the cell.
            changed_by: unsafe { cells.changed_by.map(|changed_by| changed_by.deref_mut()) },
            last_run,
            this_run,
        }
    }

    fn set_added(&mut self, new_tick: Tick) {
        *self.added = new_tick;
    }

    fn set_changed(&mut self, new_tick: Tick) {
        *self.changed = new_tick;
    }

    fn changed_by(&self) -> MaybeLocation<&'static Location<'static>> {
        self.changed_by.copied()
    }

    fn set_changed_by(&mut self, changed_by: MaybeLocation) {
        self.changed_by.assign(changed_by);
    }

    fn set_last_run(&mut self, last_run: Tick) {
        self.last_run = last_run;
    }

    fn set_this_run(&mut self, this_run: Tick) {
        self.this_run = this_run;
    }

    fn added(&self) -> Tick {
        *self.added
    }

    fn changed(&self) -> Tick {
        *self.changed
    }

    fn last_run(&self) -> Tick {
        self.last_run
    }

    fn this_run(&self) -> Tick {
        self.this_run
    }

    fn summary_tick(&self) -> Option<&AtomicTick> {
        None
    }
}

impl<'w> From<ComponentTicksMut<'w>> for ComponentTicksRef<'w> {
    fn from(ticks: ComponentTicksMut<'w>) -> Self {
        ComponentTicksRef {
            added: ticks.added,
            changed: ticks.changed,
            changed_by: ticks.changed_by.map(|changed_by| &*changed_by),
            last_run: ticks.last_run,
            this_run: ticks.this_run,
        }
    }
}
impl<'w> From<ComponentTicksMut<'w>> for ComponentTicksMutDynamic<'w> {
    fn from(value: ComponentTicksMut<'w>) -> Self {
        ComponentTicksMutDynamic {
            refs: TickRefs::Ticks {
                added: value.added,
                changed: value.changed,
                changed_by: value.changed_by,
            },
            last_run: value.last_run,
            this_run: value.this_run,
            summary_tick: None,
        }
    }
}

/// Used by mutable query parameters (such as [`Mut`] and [`ResMut`])
/// to store mutable access to the [`Tick`]s of a single component or resource.
pub struct ComponentTicksMutSumm<'w> {
    pub(crate) added: &'w mut Tick,
    pub(crate) changed: &'w mut Tick,
    pub(crate) changed_by: MaybeLocation<&'w mut &'static Location<'static>>,
    pub(crate) last_run: Tick,
    pub(crate) this_run: Tick,
    /// A reference to the summary tick for the component, if the component is
    /// dense and has a summary tick.
    pub(crate) summary_tick: Option<&'w AtomicTick>,
}
impl<'w> ChangeTicksMut<'w> for ComponentTicksMutSumm<'w> {
    fn new(
        added: &'w mut Tick,
        changed: &'w mut Tick,
        summary_tick: Option<&'w AtomicTick>,
        last_run: Tick,
        this_run: Tick,
        caller: MaybeLocation<&'w mut &'static Location<'static>>,
    ) -> Self {
        Self {
            added,
            changed,
            changed_by: caller,
            last_run,
            this_run,
            summary_tick,
        }
    }

    unsafe fn from_tick_cells(
        cells: ComponentTickCells<'w>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        Self {
            // SAFETY: Caller ensures there is no alias to the cell.
            added: unsafe { cells.added.deref_mut() },
            // SAFETY: Caller ensures there is no alias to the cell.
            changed: unsafe { cells.changed.deref_mut() },
            // SAFETY: Caller ensures there is no alias to the cell.
            summary_tick: cells.summary_tick,
            // SAFETY: Caller ensures there is no alias to the cell.
            changed_by: unsafe { cells.changed_by.map(|changed_by| changed_by.deref_mut()) },
            last_run,
            this_run,
        }
    }

    fn set_added(&mut self, new_tick: Tick) {
        *self.added = new_tick;
    }

    fn set_changed(&mut self, new_tick: Tick) {
        *self.changed = new_tick;
    }

    fn changed_by(&self) -> MaybeLocation<&'static Location<'static>> {
        self.changed_by.copied()
    }

    fn set_changed_by(&mut self, changed_by: MaybeLocation) {
        self.changed_by.assign(changed_by);
    }

    fn set_last_run(&mut self, last_run: Tick) {
        self.last_run = last_run;
    }

    fn set_this_run(&mut self, this_run: Tick) {
        self.this_run = this_run;
    }

    fn added(&self) -> Tick {
        *self.added
    }

    fn changed(&self) -> Tick {
        *self.changed
    }

    fn last_run(&self) -> Tick {
        self.last_run
    }

    fn this_run(&self) -> Tick {
        self.this_run
    }

    fn summary_tick(&self) -> Option<&AtomicTick> {
        self.summary_tick
    }
}

impl<'w> From<ComponentTicksMutSumm<'w>> for ComponentTicksRef<'w> {
    fn from(ticks: ComponentTicksMutSumm<'w>) -> Self {
        ComponentTicksRef {
            added: ticks.added,
            changed: ticks.changed,
            changed_by: ticks.changed_by.map(|changed_by| &*changed_by),
            last_run: ticks.last_run,
            this_run: ticks.this_run,
        }
    }
}
impl<'w> From<ComponentTicksMutSumm<'w>> for ComponentTicksMut<'w> {
    fn from(ticks: ComponentTicksMutSumm<'w>) -> Self {
        ComponentTicksMut {
            added: ticks.added,
            changed: ticks.changed,
            changed_by: ticks.changed_by,
            last_run: ticks.last_run,
            this_run: ticks.this_run,
        }
    }
}
impl<'w> From<ComponentTicksMutSumm<'w>> for ComponentTicksMutDynamic<'w> {
    fn from(ticks: ComponentTicksMutSumm<'w>) -> Self {
        ComponentTicksMutDynamic {
            refs: TickRefs::Ticks {
                added: ticks.added,
                changed: ticks.changed,
                changed_by: ticks.changed_by,
            },
            summary_tick: ticks.summary_tick,
            last_run: ticks.last_run,
            this_run: ticks.this_run,
        }
    }
}

pub(crate) enum TickRefs<'w> {
    Ticks {
        added: &'w mut Tick,
        changed: &'w mut Tick,
        changed_by: MaybeLocation<&'w mut &'static Location<'static>>,
    },
    NoTicks,
}

impl TickRefs<'_> {
    fn reborrow(&mut self) -> TickRefs<'_> {
        match self {
            TickRefs::Ticks {
                added,
                changed,
                changed_by,
            } => TickRefs::Ticks {
                added,
                changed,
                changed_by: changed_by.as_deref_mut(),
            },
            TickRefs::NoTicks => TickRefs::NoTicks,
        }
    }
}

/// Component ticks that are used when the existence of ticks is not statically known.
pub struct ComponentTicksMutDynamic<'w> {
    pub(crate) refs: TickRefs<'w>,
    pub(crate) summary_tick: Option<&'w AtomicTick>,
    pub(crate) last_run: Tick,
    pub(crate) this_run: Tick,
}

impl<'w> ComponentTicksMutDynamic<'w> {
    pub fn reborrow(&mut self) -> ComponentTicksMutDynamic<'_> {
        ComponentTicksMutDynamic {
            refs: self.refs.reborrow(),
            summary_tick: self.summary_tick,
            last_run: self.last_run,
            this_run: self.this_run,
        }
    }
}

impl<'w> ChangeTicksMut<'w> for ComponentTicksMutDynamic<'w> {
    fn new(
        added: &'w mut Tick,
        changed: &'w mut Tick,
        summary_tick: Option<&'w AtomicTick>,
        last_run: Tick,
        this_run: Tick,
        caller: MaybeLocation<&'w mut &'static Location<'static>>,
    ) -> Self {
        ComponentTicksMutDynamic {
            refs: TickRefs::Ticks {
                added,
                changed,
                changed_by: caller,
            },
            summary_tick,
            last_run,
            this_run,
        }
    }

    fn added(&self) -> Tick {
        if let TickRefs::Ticks { ref added, .. } = self.refs {
            **added
        } else {
            Tick::default()
        }
    }

    fn set_added(&mut self, new_tick: Tick) {
        if let TickRefs::Ticks { ref mut added, .. } = self.refs {
            **added = new_tick;
        }
    }

    fn changed(&self) -> Tick {
        if let TickRefs::Ticks { ref changed, .. } = self.refs {
            **changed
        } else {
            Tick::default()
        }
    }

    fn set_changed(&mut self, new_tick: Tick) {
        if let TickRefs::Ticks {
            ref mut changed, ..
        } = self.refs
        {
            **changed = new_tick;
        }
    }

    fn changed_by(&self) -> MaybeLocation {
        if let TickRefs::Ticks { ref changed_by, .. } = self.refs {
            changed_by.copied()
        } else {
            MaybeLocation::caller()
        }
    }

    fn set_changed_by(&mut self, new_changed_by: MaybeLocation) {
        if let TickRefs::Ticks {
            ref mut changed_by, ..
        } = self.refs
        {
            changed_by.assign(new_changed_by);
        }
    }

    fn last_run(&self) -> Tick {
        self.last_run
    }

    fn set_last_run(&mut self, last_run: Tick) {
        self.last_run = last_run;
    }

    fn this_run(&self) -> Tick {
        self.this_run
    }

    fn set_this_run(&mut self, this_run: Tick) {
        self.this_run = this_run;
    }

    fn summary_tick(&self) -> Option<&AtomicTick> {
        self.summary_tick
    }

    unsafe fn from_tick_cells(
        cells: ComponentTickCells<'w>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        ComponentTicksMutDynamic {
            refs: TickRefs::Ticks {
                // SAFETY: Caller ensures there is no alias to the cell.
                added: unsafe { cells.added.deref_mut() },
                // SAFETY: Caller ensures there is no alias to the cell.
                changed: unsafe { cells.changed.deref_mut() },
                // SAFETY: Caller ensures there is no alias to the cell.
                changed_by: unsafe { cells.changed_by.map(|changed_by| changed_by.deref_mut()) },
            },
            summary_tick: cells.summary_tick,
            last_run,
            this_run,
        }
    }
}
impl<'w> From<ComponentTicksMutDynamic<'w>> for ComponentTicksRef<'w> {
    fn from(value: ComponentTicksMutDynamic<'w>) -> Self {
        if let ComponentTicksMutDynamic {
            refs:
                TickRefs::Ticks {
                    added,
                    changed,
                    changed_by,
                },
            summary_tick: _,
            last_run,
            this_run,
        } = value
        {
            ComponentTicksRef {
                added,
                changed,
                changed_by: changed_by.map(|changed_by| &*changed_by),
                last_run,
                this_run,
            }
        } else {
            panic!("Cannot convert NoTicks to Ref");
        }
    }
}
impl<'w> From<ComponentTicksMutDynamic<'w>> for ComponentTicksMut<'w> {
    fn from(value: ComponentTicksMutDynamic<'w>) -> Self {
        if value.summary_tick.is_none()
            && let ComponentTicksMutDynamic {
                refs:
                    TickRefs::Ticks {
                        added,
                        changed,
                        changed_by,
                    },
                summary_tick: _,
                last_run,
                this_run,
            } = value
        {
            ComponentTicksMut {
                added,
                changed,
                changed_by: changed_by.map(|changed_by| &mut *changed_by),
                last_run,
                this_run,
            }
        } else {
            panic!("Cannot convert NoTicks to Mut");
        }
    }
}

// struct NoChangeDetection;

// impl<'w> ChangeTicksMut<'w> for NoChangeDetection {
//     fn new(
//         _added: &'w mut Tick,
//         _changed: &'w mut Tick,
//         _summary_tick: Option<&'w AtomicTick>,
//         _last_run: Tick,
//         _this_run: Tick,
//         _caller: MaybeLocation<&'w mut &'static Location<'static>>,
//     ) -> Self {
//         NoChangeDetection
//     }

//     fn added(&self) -> Tick {
//         Tick::default()
//     }

//     fn set_added(&mut self, _new_tick: Tick) {}

//     fn changed(&self) -> Tick {
//         Tick::default()
//     }

//     fn set_changed(&mut self, _new_tick: Tick) {}

//     fn changed_by(&self) -> MaybeLocation {
//         // TODO: not sure this is correct. Ideally it'd be more of a null value. or explain that it has never changed
//         MaybeLocation::caller()
//     }

//     fn set_changed_by(&mut self, _changed_by: MaybeLocation) {}

//     fn last_run(&self) -> Tick {
//         Tick::default()
//     }

//     fn set_last_run(&mut self, _last_run: Tick) {}

//     fn this_run(&self) -> Tick {
//         Tick::default()
//     }

//     fn set_this_run(&mut self, _this_run: Tick) {}

//     fn summary_tick(&self) -> Option<&AtomicTick> {
//         None
//     }

//     unsafe fn from_tick_cells(
//         _cells: ComponentTickCells<'w>,
//         _last_run: Tick,
//         _this_run: Tick,
//     ) -> Self {
//         NoChangeDetection
//     }
// }
// impl From<NoChangeDetection> for ComponentTicksMut<'_> {
//     fn from(_: NoChangeDetection) -> Self {
//         ComponentTicksMut {
//             added: &mut Tick::default(),
//             changed: &mut Tick::default(),
//             changed_by: MaybeLocation::caller(),
//             last_run: Tick::default(),
//             this_run: Tick::default(),
//         }
//     }
// }
// impl From<NoChangeDetection> for ComponentTicksMutSumm<'_> {
//     fn from(_: NoChangeDetection) -> Self {
//         ComponentTicksMutSumm {
//             added: &mut Tick::default(),
//             changed: &mut Tick::default(),
//             last_run: Tick::default(),
//             this_run: Tick::default(),
//             changed_by: MaybeLocation::caller(),
//             summary_tick: None,
//         }
//     }
// }
