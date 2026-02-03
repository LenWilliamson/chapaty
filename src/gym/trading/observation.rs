use std::fmt::Debug;

use crate::{
    data::view::MarketView,
    gym::trading::{action_space::ActionSpace, state::States},
};

#[derive(Debug, Clone)]
pub struct Observation<'env> {
    pub market_view: MarketView<'env>,
    pub states: &'env States,
}

impl<'env> Observation<'env> {
    pub fn action_space(self) -> ActionSpace<'env> {
        ActionSpace::new(self.states, self.market_view)
    }
}
