pub mod rassler_with_confirmation;

use crate::{dfa::states::ActivationEvent, enums::strategy::StrategyKind};

pub trait DecisionPolicy {
    /// market_trend as argument to function?
    fn choose_strategy(&self, activation_events: &Vec<ActivationEvent>) -> Option<StrategyKind>;
}
