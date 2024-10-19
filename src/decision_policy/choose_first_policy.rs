use super::DecisionPolicy;

pub struct ChooseFirstPolicy;

impl DecisionPolicy for ChooseFirstPolicy {
    fn choose_strategy(
        &self,
        activation_events: &Vec<crate::dfa::states::ActivationEvent>,
    ) -> Option<crate::enums::strategy::StrategyKind> {
        None
    }
}
