// Aktionen für Countered (Negotiation Loop)
impl Rfq<Countered> {
    /// Agent akzeptiert den Preis des Kunden
    pub fn accept_counter(self) -> Rfq<Finalized> {
        let price = self.state.customer_price;
        let qty = self.quantity;
        Rfq {
             id: self.id, symbol: self.symbol, side: self.side, quantity: self.quantity,
             created_at: self.created_at,
             state: Finalized { outcome: RfqOutcome::Filled { price, quantity: qty } },
        }
    }
    
    /// Agent schlägt wieder einen neuen Preis vor (Loop zurück zu Quoted!)
    pub fn requote(self, new_price: f64) -> Rfq<Quoted> {
        Rfq {
            id: self.id, symbol: self.symbol, side: self.side, quantity: self.quantity,
            created_at: self.created_at,
            state: Quoted { my_price: new_price },
        }
    }
}