use crate::{
    error::ChapatyResult,
    gym::{
        Reward, StepOutcome,
        trading::{action::Actions, observation::Observation},
    },
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

pub use factory::{load, make};

pub trait Env {
    fn reset(&mut self) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)>;
    fn step(&mut self, actions: Actions) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)>;
}
