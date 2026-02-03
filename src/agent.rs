pub mod crossover;
pub mod news;

use ndarray::Array;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, str::FromStr, sync::Arc};
use strum::{Display, EnumString};

use crate::{
    error::{ChapatyResult, DataError},
    gym::trading::{action::Actions, observation::Observation},
};

// ============================================================================
//  Shared Utilities
// ============================================================================

/// A utility for defining search space axes in grid searches.
/// It parses explicit string parameters to avoid floating point ambiguity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GridAxis {
    start: f64,
    end: f64,
    step: f64,
    /// Number of decimal places to round to, inferred from the `step` string.
    precision: u32,
}

impl GridAxis {
    /// Create a new axis from string parameters.
    /// Returns a Result instead of panicking.
    pub fn new(start: &str, end: &str, step: &str) -> ChapatyResult<Self> {
        let start_f = f64::from_str(start).map_err(DataError::from)?;
        let end_f = f64::from_str(end).map_err(DataError::from)?;
        let step_f = f64::from_str(step).map_err(DataError::from)?;

        let precision = step.split('.').nth(1).map(|s| s.len() as u32).unwrap_or(0);

        Ok(Self {
            start: start_f,
            end: end_f,
            step: step_f,
            precision,
        })
    }

    pub fn generate(&self) -> Vec<f64> {
        let factor = 10_f64.powi(self.precision as i32);

        Array::range(self.start, self.end, self.step)
            .iter()
            .map(|val| (val * factor).round() / factor)
            .collect()
    }
}

// ============================================================================
//  Core Agent Definitions
// ============================================================================

/// Represents the unique identifier of an agent, used for tracking actions in reports.
/// This enum is designed to help identify which agent performed a specific action during
/// the backtesting or trading process. Each variant contains a `String` that uniquely
/// identifies the agent for reporting purposes.
///
/// The `String` can represent custom agent names or predefined types (e.g., "NewsCounter").
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Display,
    Default,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    EnumString,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentIdentifier {
    /// A custom user-defined agent.
    #[strum(to_string = "{0}")]
    Named(Arc<String>),

    #[default]
    Random,
}

pub trait Agent {
    /// Decide on actions based on the current observation.
    fn act(&mut self, obs: Observation) -> ChapatyResult<Actions>;

    /// Optional agent name for logging/debugging.
    fn identifier(&self) -> AgentIdentifier {
        AgentIdentifier::Named(Arc::new(
            "UnnamedAgent: override Agent::identifier()".to_string(),
        ))
    }

    /// Reset internal state at the end of an episode. Default is no-op.
    fn reset(&mut self) {}
}

impl Agent for Box<dyn Agent> {
    fn act(&mut self, obs: Observation) -> ChapatyResult<Actions> {
        (**self).act(obs)
    }

    fn identifier(&self) -> AgentIdentifier {
        (**self).identifier()
    }

    fn reset(&mut self) {
        (**self).reset()
    }
}
