use crate::enums::trades::TradeKind;

#[derive(Debug, Clone)]
pub struct Trade {
    pub entry_price: f64,
    pub stop_loss: f64,
    pub take_prift: f64,
    pub trade_kind: TradeKind,
}

impl Trade {
    pub fn profit(&self, exit_px: f64) -> f64 {
        let entry_px = self.entry_price;
        match self.trade_kind {
            TradeKind::Short => entry_px - exit_px,
            TradeKind::Long => exit_px - entry_px,
            TradeKind::None => 0.0,
        }
    }
}
