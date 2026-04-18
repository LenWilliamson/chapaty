use crate::{data::view::MarketView, gym::flow::action::Action};

/*

TODO

context.rs aus trading und flow zusamnelegen und Action-zeugs in action.rs verschieben
*/

/// Carries all necessary data to validate and apply agent actions.
///
/// Decoupled from `Env` to allow `Ledger` to consume it directly.
pub struct ActionCtx<'env> {
    pub actions: Vec<Action>,
    pub market: MarketView<'env>,
}

/// A summary of the results from applying a batch of actions.
#[derive(Default, Debug, Clone)]
pub struct ActionSummary {
    pub rejected: Vec<Action>,
    pub executed: Vec<Action>,
}
