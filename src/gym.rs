use std::{str::FromStr, sync::Arc};

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use crate::error::{AgentError, ChapatyResult, DataError};

pub mod trading;

/// Represents a reward value in whole dollars.
///
/// This struct wraps an `i64` to avoid floating-point precision issues,
/// ensuring exact comparisons and efficient operations in financial
/// calculations.
///
/// # Rationale
///
/// - Using `i64` avoids floating-point inaccuracies (e.g., `0.1 + 0.2 != 0.3`
///   in `f64`).
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
/// The environment follows a finite state machine (FSM) with the following
/// valid transitions. Other transitions return an error.
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
    /// The attached `Episode` value tracks the current episode number, starting
    /// from 0.
    Running,

    /// The active episode has reached a terminal state.
    ///
    /// A call to `reset()` is required to start the next episode.
    EpisodeDone,

    /// The simulation is complete and has run out of data.
    Done,
}

impl EnvStatus {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    #[must_use]
    pub const fn is_episode_done(&self) -> bool {
        matches!(self, Self::EpisodeDone)
    }

    #[must_use]
    pub const fn is_done(&self) -> bool {
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
    #[must_use]
    pub const fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    #[must_use]
    pub const fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }

    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        matches!(self, Self::Truncated)
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.is_terminated() || self.is_truncated()
    }
}

// ============================================================================
//  Shared Utilities
// ============================================================================

/// A utility for defining search space axes in grid searches.
///
/// An axis runs from `start` to `end`, both inclusive, in steps of `step`. The
/// values are passed as strings so the number of decimal places is known
/// exactly, and generated values are rounded to it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GridAxis {
    start: f64,
    end: f64,
    step: f64,
    /// Number of decimal places to round to, the most of `start` and `step`.
    precision: i32,
}

impl GridAxis {
    /// Creates an axis from `start` to `end`, both inclusive, in steps of
    /// `step`.
    ///
    /// # Errors
    /// Returns an error when a value cannot be parsed as `f64`, when a value
    /// or the number of steps is not finite, when `step <= 0`, or when
    /// `end < start`.
    pub fn new(start: &str, end: &str, step: &str) -> ChapatyResult<Self> {
        let start_f = f64::from_str(start).map_err(DataError::from)?;
        let end_f = f64::from_str(end).map_err(DataError::from)?;
        let step_f = f64::from_str(step).map_err(DataError::from)?;

        if !(start_f.is_finite() && end_f.is_finite() && step_f.is_finite()) {
            return Err(AgentError::InvalidInput(format!(
                "grid axis values must be finite, got start={start}, end={end}, step={step}"
            ))
            .into());
        }
        if step_f <= 0.0 {
            return Err(AgentError::InvalidInput(format!(
                "grid axis step must be > 0, got step={step}"
            ))
            .into());
        }
        if end_f < start_f {
            return Err(AgentError::InvalidInput(format!(
                "grid axis end must be >= start, got start={start}, end={end}"
            ))
            .into());
        }
        if !((end_f - start_f) / step_f).is_finite() {
            return Err(AgentError::InvalidInput(format!(
                "grid axis has too many steps, got start={start}, end={end}, step={step}"
            ))
            .into());
        }

        Ok(Self {
            start: start_f,
            end: end_f,
            step: step_f,
            precision: decimal_places(start)?.max(decimal_places(step)?),
        })
    }

    /// Returns every value from `start` to `end`, both inclusive.
    ///
    /// The last value is the largest `start + i * step` that does not exceed
    /// `end`. A tiny tolerance absorbs floating point noise, so an `end` that
    /// lies on the grid is always included.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "`new` guarantees a finite, non-negative step count, and a grid axis has far fewer values than f64 counts exactly"
    )]
    pub fn generate(&self) -> Vec<f64> {
        const TOLERANCE: f64 = 1e-9;

        let factor = 10_f64.powi(self.precision);
        let last_index = ((self.end - self.start) / self.step + TOLERANCE).floor() as usize;

        (0..=last_index)
            .map(|i| {
                let value = self.step.mul_add(i as f64, self.start);
                (value * factor).round() / factor
            })
            .collect()
    }
}

/// Number of digits after the decimal point in `value`, for example 2 for
/// `"0.25"` and 0 for `"5"`.
fn decimal_places(value: &str) -> ChapatyResult<i32> {
    let places = value
        .split('.')
        .nth(1)
        .map(|digits| i32::try_from(digits.len()).map_err(DataError::from))
        .transpose()?
        .unwrap_or(0);
    Ok(places)
}

// ============================================================================
//  Core Agent Definitions
// ============================================================================

/// Represents the unique identifier of an agent, used for tracking actions in
/// reports.
///
/// This enum is designed to help identify which agent performed a specific
/// action during the backtesting or trading process. Each variant contains a
/// `String` that uniquely identifies the agent for reporting purposes.
///
/// The `String` can represent custom agent names or predefined types (e.g.,
/// "`NewsCounter`").
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

#[cfg(test)]
mod tests {
    use super::*;

    fn axis(start: &str, end: &str, step: &str) -> ChapatyResult<Vec<f64>> {
        GridAxis::new(start, end, step).map(|axis| axis.generate())
    }

    #[test]
    fn end_is_inclusive() -> ChapatyResult<()> {
        let values = axis("0.8", "2.0", "0.1")?;
        assert_eq!(values.len(), 13);
        assert_eq!(values.first(), Some(&0.8));
        assert_eq!(values.last(), Some(&2.0));
        Ok(())
    }

    #[test]
    fn end_on_grid_survives_float_noise() -> ChapatyResult<()> {
        // (0.8 - 0.2) / 0.2 is 3.0000000000000004 in f64.
        assert_eq!(axis("0.2", "0.8", "0.2")?, vec![0.2, 0.4, 0.6, 0.8]);
        // (0.7 - 0.0) / 0.1 is 6.999999999999999 in f64.
        assert_eq!(axis("0.0", "0.7", "0.1")?.last(), Some(&0.7));
        Ok(())
    }

    #[test]
    fn end_off_grid_is_never_exceeded() -> ChapatyResult<()> {
        assert_eq!(axis("0.0", "1.0", "0.4")?, vec![0.0, 0.4, 0.8]);
        Ok(())
    }

    #[test]
    fn integer_steps_and_single_value() -> ChapatyResult<()> {
        assert_eq!(axis("1.0", "3.0", "1.0")?, vec![1.0, 2.0, 3.0]);
        assert_eq!(axis("25", "35", "5")?, vec![25.0, 30.0, 35.0]);
        assert_eq!(axis("5", "5", "1")?, vec![5.0]);
        Ok(())
    }

    #[test]
    fn precision_covers_start_and_step() -> ChapatyResult<()> {
        assert_eq!(axis("0.125", "0.625", "0.25")?, vec![0.125, 0.375, 0.625]);
        Ok(())
    }

    #[test]
    fn invalid_axes_are_rejected() {
        assert!(GridAxis::new("0.0", "1.0", "0").is_err());
        assert!(GridAxis::new("0.0", "1.0", "-0.1").is_err());
        assert!(GridAxis::new("1.0", "0.0", "0.1").is_err());
        assert!(GridAxis::new("0.0", "inf", "0.1").is_err());
        assert!(GridAxis::new("NaN", "1.0", "0.1").is_err());
        assert!(GridAxis::new("abc", "1.0", "0.1").is_err());
    }
}
