// Aktionen, die nur möglich sind, wenn der RFQ "Quoted" ist
impl Rfq<Quoted> {
    /// Die Simulation entscheidet: Der Kunde akzeptiert
    pub fn customer_accepts(self) -> Rfq<Finalized> {
        let price = self.state.my_price;
        let qty = self.quantity;
        self.finalize(RfqOutcome::Filled { price, quantity: qty })
    }

    /// Die Simulation entscheidet: Der Kunde lehnt ab
    pub fn customer_rejects(self) -> Rfq<Finalized> {
        self.finalize(RfqOutcome::Rejected)
    }

    /// Die Simulation entscheidet: Der Kunde verhandelt (Counter Offer)
    pub fn customer_counters(self, counter_price: f64) -> Rfq<Countered> {
        Rfq {
            id: self.id, symbol: self.symbol, side: self.side, quantity: self.quantity,
            created_at: self.created_at,
            state: Countered { customer_price: counter_price },
        }
    }
    
    fn finalize(self, outcome: RfqOutcome) -> Rfq<Finalized> {
         Rfq {
            id: self.id, symbol: self.symbol, side: self.side, quantity: self.quantity,
            created_at: self.created_at,
            state: Finalized { outcome },
        }
    }
}