use crate::{
    agent::AgentIdentifier,
    data::{
        domain::{Price, Quantity, TradeId},
        event::MarketId,
    },
    error::{AgentError, ChapatyResult},
    gym::trading::types::TradeType,
    sorted_vec_map::SortedVecMap,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString};

// ================================================================================================
// Command Trait
// ================================================================================================

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
    Open,
    Modify,
    MarketClose,
    Cancel,
}

impl From<&Action> for ActionKind {
    fn from(action: &Action) -> Self {
        match action {
            Action::Open(_) => ActionKind::Open,
            Action::Modify(_) => ActionKind::Modify,
            Action::MarketClose(_) => ActionKind::MarketClose,
            Action::Cancel(_) => ActionKind::Cancel,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Action {
    Open(OpenCmd),
    Modify(ModifyCmd),
    MarketClose(MarketCloseCmd),
    Cancel(CancelCmd),
}

impl Command for Action {
    fn validate(&self) -> ChapatyResult<()> {
        match self {
            Action::Open(cmd) => cmd.validate(),
            Action::Modify(cmd) => cmd.validate(),
            Action::MarketClose(cmd) => cmd.validate(),
            Action::Cancel(cmd) => cmd.validate(),
        }
    }
}

impl Action {
    /// Helper to identify "Open" intent for filtering/sorting optimization.
    pub fn is_open(&self) -> bool {
        matches!(self, Action::Open(_))
    }

    /// Returns the execution priority rank for deterministic sorting.
    ///
    /// Priority Order (Safe Margin Management):
    /// 1. Cancel (0) - Remove pending commitments first.
    /// 2. Close (1) - Exit active risk and free up margin.
    /// 3. Modify (2) - Adjust existing trades.
    /// 4. Open (3) - Enter new positions last (using freed resources).
    pub fn execution_priority(&self) -> u8 {
        match self {
            Action::Cancel(_) => 0,
            Action::MarketClose(_) => 1,
            Action::Modify(_) => 2,
            Action::Open(_) => 3,
        }
    }

    pub fn kind(&self) -> ActionKind {
        self.into()
    }

    /// Extracts the Trade ID associated with this action.
    /// Useful for logging and routing without matching on the specific variant.
    pub fn trade_id(&self) -> TradeId {
        match self {
            Action::Open(cmd) => cmd.trade_id,
            Action::Modify(cmd) => cmd.trade_id,
            Action::MarketClose(cmd) => cmd.trade_id,
            Action::Cancel(cmd) => cmd.trade_id,
        }
    }

    pub fn as_command(&self) -> &dyn Command {
        match self {
            Action::Open(cmd) => cmd,
            Action::Modify(cmd) => cmd,
            Action::MarketClose(cmd) => cmd,
            Action::Cancel(cmd) => cmd,
        }
    }

    /// Extracts the Agent ID associated with this action.
    pub fn agent_id(&self) -> AgentIdentifier {
        match self {
            Action::Open(cmd) => cmd.agent_id.clone(),
            Action::Modify(cmd) => cmd.agent_id.clone(),
            Action::MarketClose(cmd) => cmd.agent_id.clone(),
            Action::Cancel(cmd) => cmd.agent_id.clone(),
        }
    }
}

// ================================================================================================
// The Commands (PODs - Plain Old Data)
// ================================================================================================

/// Command to open a new trade (Market or Limit).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenCmd {
    pub agent_id: AgentIdentifier,
    /// The unique ID assigned by the agent for this trade.
    pub trade_id: TradeId,
    pub trade_type: TradeType,
    pub quantity: Quantity,

    // Optional Parameters
    pub entry_price: Option<Price>,
    pub stop_loss: Option<Price>,
    pub take_profit: Option<Price>,
}

impl Command for OpenCmd {
    fn validate(&self) -> ChapatyResult<()> {
        if self.quantity <= Quantity(0.0) {
            return Err(AgentError::InvalidInput(format!(
                "Open quantity must be positive. Got: {:?}",
                self.quantity
            ))
            .into());
        }

        // Validate price ordering (SL < Entry < TP, etc.)
        self.trade_type.price_ordering_validation(
            self.stop_loss,
            self.entry_price,
            self.take_profit,
        )?;

        Ok(())
    }
}

/// Command to modify an existing trade.
///
/// - For **Pending** orders: Can modify Entry (Limit) Price, Stop Loss, and Take Profit.
/// - For **Active** trades: Can ONLY modify Stop Loss and Take Profit. Attempting to modify
///   Entry Price on an active trade will result in an error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModifyCmd {
    pub agent_id: AgentIdentifier,
    pub trade_id: TradeId,
    pub new_entry_price: Option<Price>,
    pub new_stop_loss: Option<Price>,
    pub new_take_profit: Option<Price>,
}

impl Command for ModifyCmd {
    fn validate(&self) -> ChapatyResult<()> {
        // Guard against crossing SL/TP if both are provided in the same modification
        if let (Some(sl), Some(tp)) = (self.new_stop_loss, self.new_take_profit)
            && (sl.0 - tp.0).abs() < f64::EPSILON
        {
            return Err(AgentError::InvalidInput(
                "Stop Loss and Take Profit cannot be equal.".to_string(),
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketCloseCmd {
    pub agent_id: AgentIdentifier,
    pub trade_id: TradeId,
    pub quantity: Option<Quantity>,
}

impl Command for MarketCloseCmd {
    fn validate(&self) -> ChapatyResult<()> {
        if let Some(qty) = self.quantity
            && qty <= Quantity(0.0)
        {
            return Err(AgentError::InvalidInput(format!(
                "Close quantity must be positive. Got: {:?}",
                qty
            ))
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelCmd {
    pub agent_id: AgentIdentifier,
    pub trade_id: TradeId,
}

impl Command for CancelCmd {
    fn validate(&self) -> ChapatyResult<()> {
        // Trivial validation: A cancel command has no parameters to validate.
        // It's always valid structurally.
        Ok(())
    }
}

// ================================================================================================
// Action Sorting & Batching Logic
// ================================================================================================

/// A batch of trading actions to be applied to the environment.
#[derive(Debug, Clone)]
pub struct Actions(pub SortedVecMap<MarketId, Vec<Action>>);

impl Default for Actions {
    fn default() -> Self {
        Self::new()
    }
}

impl Actions {
    /// Returns an [`Actions`] instance that represents **no operations**.
    pub fn no_op() -> Self {
        Actions(SortedVecMap::new())
    }

    pub fn new() -> Self {
        Self::no_op()
    }

    pub fn add(&mut self, spec: MarketId, action: Action) {
        self.0.entry(spec).or_default().push(action);
    }

    pub fn with_action(mut self, spec: MarketId, action: Action) -> Self {
        self.add(spec, action);
        self
    }

    pub fn any_open_action(&self, spec: &MarketId) -> bool {
        self.0
            .get(spec)
            .map(|actions| actions.iter().any(|action| action.is_open()))
            .unwrap_or(false)
    }

    /// Consumes the batch and returns an iterator yielding actions sorted by execution priority.
    pub fn into_sorted_iter(self) -> impl Iterator<Item = (MarketId, Action)> {
        self.0
            .into_iter()
            .map(|(spec, mut actions)| {
                actions.sort_by_key(Action::execution_priority);
                (spec, actions)
            })
            .flat_map(|(spec, actions)| actions.into_iter().map(move |action| (spec, action)))
    }
}

impl From<(MarketId, Action)> for Actions {
    fn from((spec, action): (MarketId, Action)) -> Self {
        Actions::new().with_action(spec, action)
    }
}

impl From<Vec<(MarketId, Action)>> for Actions {
    fn from(vec: Vec<(MarketId, Action)>) -> Self {
        vec.into_iter().collect()
    }
}

impl FromIterator<(MarketId, Action)> for Actions {
    fn from_iter<T: IntoIterator<Item = (MarketId, Action)>>(iter: T) -> Self {
        iter.into_iter()
            .fold(Actions::new(), |mut acc, (market, action)| {
                acc.add(market, action);
                acc
            })
    }
}
