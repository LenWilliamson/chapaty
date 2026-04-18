// ================================================================================================
// Command Trait
// ================================================================================================

use chrono::Duration;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString};

use crate::{
    data::domain::Price,
    error::ChapatyResult,
    gym::{flow::domain::RfqId, trading::action::Actions as StreetActions},
};

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
    Reject,
    Accept,
}

impl From<&Action> for ActionKind {
    fn from(action: &Action) -> Self {
        match action {
            Action::Quote(_) => Self::Quote,
            Action::Ignore(_) => Self::Ignore,
            Action::Reject(_) => Self::Reject,
            Action::Accept(_) => Self::Accept,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Action {
    Quote(QuoteCmd),
    Ignore(IgnoreCmd),
    Reject(RejectCmd),
    Accept(AcceptCmd),
}

impl Command for Action {
    fn validate(&self) -> ChapatyResult<()> {
        match self {
            Action::Quote(cmd) => cmd.validate(),
            Action::Ignore(cmd) => cmd.validate(),
            Action::Reject(cmd) => cmd.validate(),
            Action::Accept(cmd) => cmd.validate(),
        }
    }
}

impl Action {
    pub fn rfq_id(&self) -> RfqId {
        match self {
            Action::Quote(cmd) => cmd.rfq_id,
            Action::Ignore(cmd) => cmd.rfq_id,
            Action::Reject(cmd) => cmd.rfq_id,
            Action::Accept(cmd) => cmd.rfq_id,
        }
    }

    pub fn is_quote(&self) -> bool {
        matches!(self, Action::Quote(_))
    }

    pub fn is_ignore(&self) -> bool {
        matches!(self, Action::Ignore(_))
    }

    pub fn is_reject(&self) -> bool {
        matches!(self, Action::Reject(_))
    }

    pub fn is_accept(&self) -> bool {
        matches!(self, Action::Accept(_))
    }

    pub fn requires_client_reply(&self) -> bool {
        self.is_quote()
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
    fn validate(&self) -> ChapatyResult<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IgnoreCmd {
    pub rfq_id: RfqId,
}

impl Command for IgnoreCmd {
    fn validate(&self) -> ChapatyResult<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RejectCmd {
    pub rfq_id: RfqId,
}

impl Command for RejectCmd {
    fn validate(&self) -> ChapatyResult<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AcceptCmd {
    pub rfq_id: RfqId,
    // Optional: Safety check, accept if price >= X
    pub min_price: Option<Price>,
}

impl Command for AcceptCmd {
    fn validate(&self) -> ChapatyResult<()> {
        unimplemented!()
    }
}

// ================================================================================================
// Action Sorting & Batching Logic
// ================================================================================================

/// A batch of trading actions to be applied to the environment.
/// The complete set of instructions the agent wants to execute in this step.
/// Structured explicitly to separate concerns (Flow vs. Street vs. Time).
#[derive(Debug, Clone, Default)]
pub struct Actions {
    /// Aktionen im Kunden-Geschäft (Quotes, Rejects).
    /// Wir nutzen hier einen einfachen Vec, da die Reihenfolge oft egal ist
    /// oder durch die RfQ-ID eindeutig zugeordnet wird.
    pub flow: Vec<Action>,

    /// Aktionen an der Börse (Hedges).
    /// Wir nutzen das hoch-optimierte Struct aus dem Trading-Modul.
    pub street: StreetActions,

    /// Zeit-Management (Wann soll der Agent wieder geweckt werden?).
    pub wait: Vec<Duration>,
}

// Optional: Ein Builder oder Helper für leere Actions
impl Actions {
    pub fn no_op() -> Self {
        Self::default()
    }
}
