use std::sync::Arc;

use crate::{
    error::ChapatyResult,
    gym::{
        AgentIdentifier,
        trading::{action::Actions, observation::Observation},
    },
};

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
