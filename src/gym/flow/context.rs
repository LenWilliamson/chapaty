use crate::{
    data::view::MarketView,
    gym::flow::{action::Actions},
};

/// Carries all necessary data to validate and apply agent actions.
///
/// Decoupled from `Env` to allow `Ledger` to consume it directly.
pub struct ActionCtx<'env> {
    pub actions: Actions,
    pub market: MarketView<'env>,
}

/// Carries market data needed to update open positions (Mark-to-Market).
pub struct UpdateCtx<'a, 'env> {
    pub market: &'a MarketView<'env>,
}

/// A summary of the results from applying a batch of actions.
#[derive(Default, Debug, Clone, Copy)]
pub struct ActionSummary {
    pub rejected: u32,
    pub executed: u32,
}
