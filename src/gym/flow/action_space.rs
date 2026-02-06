use rand::rngs::ThreadRng;

use crate::{data::view::MarketView, error::ChapatyResult, gym::flow::{action::Actions, state::States}};


pub struct ActionSpace<'env> {
    states: &'env States,
    view: MarketView<'env>,
    rng: ThreadRng,
}

impl<'env> ActionSpace<'env> {
    pub fn new(states: &'env States, view: MarketView<'env>) -> Self {
        Self {
            states,
            view,
            rng: rand::rng(),
        }
    }

    pub fn sample(&mut self) -> ChapatyResult<Actions> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {}
