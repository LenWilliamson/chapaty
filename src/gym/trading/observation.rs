use crate::{
    data::view::MarketView,
    gym::trading::{action_space::ActionSpace, state::States},
};

#[derive(Clone, Copy)]
pub struct Observation<'env> {
    pub market_view: MarketView<'env>,
    pub states: &'env States,
}

impl<'env> Observation<'env> {
    #[must_use]
    pub fn action_space(self) -> ActionSpace<'env> {
        ActionSpace::new(self.states, self.market_view)
    }
}
