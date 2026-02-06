use crate::{data::view::MarketView, gym::flow::action_space::ActionSpace};

#[derive(Debug, Clone)]
pub struct Observation<'env> {
    pub market_view: MarketView<'env>,
    pub states: &'env States,
}

impl<'env> Observation<'env> {
    pub fn action_space(self) -> ActionSpace<'env> {
        ActionSpace::new(self.states, self.market_view)
    }
    /// Alle RFQs, die eine Antwort erfordern (Open, Countered)
    pub fn incoming(&self) -> impl Iterator<Item = &State> {
        self.states.incoming_index.iter()
            .filter_map(|id| self.states.get_current_state(id))
    }

    /// Alle RFQs, wo wir auf den Kunden warten
    pub fn pending(&self) -> impl Iterator<Item = &State> {
        self.states.pending_index.iter()
            .filter_map(|id| self.states.get_current_state(id))
    }
    
    /// Zugriff auf Historie für ein spezifisches RFQ (für Features)
    pub fn history_of(&self, id: &RfqId) -> &[State] {
        self.states.get_transcript(id).unwrap_or(&[])
    }
}
