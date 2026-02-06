use crate::{
    error::ChapatyResult,
    gym::{
        Reward, StepOutcome,
        flow::{action::Actions, observation::Observation},
    },
};

pub mod action;
pub mod action_space;
pub mod context;
pub mod domain;
pub mod env;
pub mod fill;
pub mod generator;
pub mod ledger;
pub mod observation;
pub mod scheduler;
pub mod state;

pub trait Env {
    fn reset(&mut self) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)>;
    fn step(&mut self, actions: Actions) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)>;
}
