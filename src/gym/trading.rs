use crate::{
    error::ChapatyResult,
    gym::{Reward, StepOutcome},
};

pub mod action;
pub mod action_space;
pub mod agent;
pub mod config;
pub(crate) mod context;
pub mod env;
pub mod factory;
pub(crate) mod ledger;
pub mod observation;
pub mod state;
pub mod types;

pub use action::*;
pub use action_space::*;
pub use agent::*;
pub use config::*;
pub use env::*;
pub use factory::*;
pub use observation::*;
pub use state::*;
pub use types::*;

pub trait Env {
    fn reset(&mut self) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)>;
    fn step(&mut self, actions: Actions) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)>;
}
