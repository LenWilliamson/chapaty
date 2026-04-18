use crate::{
    data::view::MarketView,
    gym::{
        flow::{action_space::ActionSpace, state::States as FlowStates},
        trading::state::States as StreetStates,
    },
};

#[derive(Debug, Clone)]
pub struct Observation<'env> {
    pub market_view: MarketView<'env>,
    pub flow_states: &'env FlowStates,
    pub street_states: &'env StreetStates,
}

impl<'env> Observation<'env> {
    pub fn action_space(self) -> ActionSpace<'env> {
        unimplemented!()
    }
}
