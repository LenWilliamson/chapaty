// ================================================================================================
// Command Trait
// ================================================================================================

use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString};

use crate::{data::domain::Price, error::ChapatyResult, gym::flow::domain::RfqId};

/// Represents an executable instruction from an agent.
pub trait Command {
    /// Performs intrinsic validation (stateless checks).
    /// Returns `Ok(())` if the command parameters are self-consistent.
    fn validate(&self) -> ChapatyResult<()>;
}

// ================================================================================================
// The Action Enum (The Command Wrapper)
// ================================================================================================

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    EnumIter,
    EnumCount,
    Display,
)]
#[strum(serialize_all = "lowercase")]
pub enum ActionKind {
    Quote,
    Ignore,
    Accept,
}

impl From<&Action> for ActionKind {
    fn from(action: &Action) -> Self {
        match action {
            Action::Quote(_) => Self::Quote,
            Action::Ignore(_) => Self::Ignore,
            Action::Accept(_) => Self::Accept,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Action {
    Quote(QuoteCmd),
    Ignore(IgnoreCmd),
    Accept(AcceptCmd),
}

impl Command for Action {
    fn validate(&self) -> ChapatyResult<()> {
        match self {
            Action::Quote(cmd) => cmd.validate(),
            Action::Ignore(cmd) => cmd.validate(),
            Action::Accept(cmd) => cmd.validate(),
        }
    }
}

// ================================================================================================
// The Commands (PODs - Plain Old Data)
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuoteCmd {
    pub rfq_id: RfqId,
    pub price: Price,

}

impl Command for QuoteCmd {
    fn validate(&self) -> ChapatyResult<()> { unimplemented!() }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IgnoreCmd {
    pub rfq_id: RfqId,
    
}

impl Command for IgnoreCmd {
    fn validate(&self) -> ChapatyResult<()> { unimplemented!() }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AcceptCmd {
    pub rfq_id: RfqId,
    // Optional: Safety check, accept if price >= X
    pub min_price: Option<Price>, 
}

impl Command for AcceptCmd {
    fn validate(&self) -> ChapatyResult<()> { unimplemented!() }
}

// ================================================================================================
// Action Sorting & Batching Logic
// ================================================================================================

/// A batch of trading actions to be applied to the environment.
#[derive(Debug, Clone, Default)]
pub struct Actions(pub Vec<Action>);