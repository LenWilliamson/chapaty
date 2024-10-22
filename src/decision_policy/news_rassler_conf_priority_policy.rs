use crate::enums::strategy::StrategyKind;

use super::DecisionPolicy;

pub struct NewsRasslerConfPriorityPolicy;

impl DecisionPolicy for NewsRasslerConfPriorityPolicy {
    fn choose_strategy(
        &self,
        activation_events: &Vec<crate::dfa::states::ActivationEvent>,
    ) -> Option<crate::enums::strategy::StrategyKind> {
        if activation_events.len() == 0 {
            None
        } else if activation_events.len() == 1 {
            Some(activation_events[0].strategy.get_strategy_kind())
        } else if activation_events.len() == 2 {
            Some(StrategyKind::NewsRasslerConf)
        } else {
            panic!("The NewsRasslerConfPriorityPolicy can only choose between two strategies: NewsCounter and NewsRasslerConf")
        }
    }
}
