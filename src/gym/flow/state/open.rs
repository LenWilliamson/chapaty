impl Rfq<Open> {
    pub fn quote(self, price: f64, now: DateTime<Utc>) -> Rfq<Quoted> {
        Rfq {
            // ... copy fields ...
            revision_id: self.revision_id + 1, // Revision hochzählen
            state: Quoted { 
                my_price: price,
                quoted_at: now 
            }
        }
    }

        /// Der Agent sendet ein Quote -> Übergang zu "Quoted"
    /// Beachte: Wir konsumieren "self" (move), der alte Zustand existiert nicht mehr.
    pub fn quote(self, price: f64) -> Rfq<Quoted> {
        Rfq {
            id: self.id,
            symbol: self.symbol,
            side: self.side,
            quantity: self.quantity,
            created_at: self.created_at,
            state: Quoted { my_price: price },
        }
    }

    /// Der Agent ignoriert die Anfrage -> Expired
    pub fn ignore(self) -> Rfq<Finalized> {
        self.finalize(RfqOutcome::Expired)
    }
    
    // Hilfsfunktion für den Abschluss
    fn finalize(self, outcome: RfqOutcome) -> Rfq<Finalized> {
         Rfq {
            id: self.id, symbol: self.symbol, side: self.side, quantity: self.quantity,
            created_at: self.created_at,
            state: Finalized { outcome },
        }
    }
}