use std::{str::FromStr, sync::Arc};

use ndarray::Array;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use crate::{
    error::{ChapatyResult, DataError},
    impl_add_sub_mul_div_primitive, impl_from_primitive,
};

pub mod trading;

/// Represents a reward value in whole dollars.
///
/// This struct wraps an `i64` to avoid floating-point precision issues, ensuring
/// exact comparisons and efficient operations in financial calculations.
///
/// # Rationale
///
/// - Using `i64` avoids floating-point inaccuracies (e.g., `0.1 + 0.2 != 0.3` in `f64`).
/// - `i64` ensures deterministic ordering and equality comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Reward(pub i64);
impl_from_primitive!(Reward, i64);
impl_add_sub_mul_div_primitive!(Reward, i64);

/// Configuration parameter for penalizing invalid actions.
///
/// This is a Newtype wrapper around [`Reward`] to distinguish it
/// from standard step rewards and allow for specific default values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InvalidActionPenalty(pub Reward);

// Defines the sensible default for this specific parameter
impl Default for InvalidActionPenalty {
    fn default() -> Self {
        Self(Reward(0))
    }
}

// Allow seamless conversion to the underlying Reward when doing math
impl From<InvalidActionPenalty> for Reward {
    fn from(penalty: InvalidActionPenalty) -> Self {
        penalty.0
    }
}

/// Represents the lifecycle status of the trading environment.
///
/// This enum tracks the state of the simulation, guiding the flow from the
/// initial start, through sequential episodes, to the final completion.
///
/// # Lifecycle
///
/// The environment follows a finite state machine (FSM) with the following valid transitions. Other transitions return an error.
///
/// ```md
/// Current State (optional step context)           | Action  | Next State  | Notes
/// ------------------------------------------------|---------|-------------|-------------------------------------------
/// `Running` (end of episode)                      | step()  | EpisodeDone | Episode terminates
/// `Running` (no simulation data left)             | step()  | Done        | Epoch terminates
/// `Running`                                       | step()  | Running     | Continue within episode
/// `EpisodeDone` (simulation data left)            | reset() | Running     | Proceed to next episode
/// `Ready` / `Running` / `EpisodeDone` / `Done`    | reset() | Running     | Restart entire run. Start at first episode
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvStatus {
    /// Initial state. The environment is waiting for `reset()` to be called.
    Ready,

    /// An episode is active and the environment is ready for `step()` calls.
    ///
    /// The attached `Episode` value tracks the current episode number, starting from 0.
    Running,

    /// The active episode has reached a terminal state.
    ///
    /// A call to `reset()` is required to start the next episode.
    EpisodeDone,

    /// The simulation is complete and has run out of data.
    Done,
}

impl EnvStatus {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    pub fn is_episode_done(&self) -> bool {
        matches!(self, Self::EpisodeDone)
    }

    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepOutcome {
    InProgress,
    /// agent closed position / success / fail
    Terminated,
    /// episode boundary due to time
    Truncated,
    /// end of data / end of epoch
    Done,
}

impl StepOutcome {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    pub fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }

    pub fn is_truncated(&self) -> bool {
        matches!(self, Self::Truncated)
    }

    pub fn is_terminal(&self) -> bool {
        self.is_terminated() || self.is_truncated()
    }
}

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
