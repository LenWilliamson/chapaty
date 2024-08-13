use crate::{enums::trade_and_pre_trade::TradeDirectionKind, MarketKind};

#[derive(Debug, Clone)]
pub struct Trade {
    pub entry_price: f64,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub trade_kind: TradeDirectionKind,
    pub is_valid: bool,
}

impl Trade {
    pub fn profit(&self, exit_px: f64) -> f64 {
        let entry_px = self.entry_price;
        match self.trade_kind {
            TradeDirectionKind::Short => entry_px - exit_px,
            TradeDirectionKind::Long => exit_px - entry_px,
            TradeDirectionKind::None => 0.0,
        }
    }

    pub fn curate_precision(self, market: &MarketKind) -> Trade {
        Self {
            entry_price: market.round_float_to_correct_decimal_place(self.entry_price),
            stop_loss: self
                .stop_loss
                .and_then(|px| Some(market.round_float_to_correct_decimal_place(px))),
            take_profit: self
                .take_profit
                .and_then(|px| Some(market.round_float_to_correct_decimal_place(px))),
            trade_kind: self.trade_kind,
            ..self
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_compute_trade_values() {
        let trade_long = Trade {
            entry_price: 100.0,
            stop_loss: Some(-1.0),
            take_profit: Some(-1.0),
            trade_kind: TradeDirectionKind::Long,
            is_valid: true
        };

        assert_eq!(1.0, trade_long.profit(101.0));
        assert_eq!(0.0, trade_long.profit(100.0));
        assert_eq!(-1.0, trade_long.profit(99.0));

        let trade_short = Trade {
            entry_price: 100.0,
            stop_loss: Some(-1.0),
            take_profit: Some(-1.0),
            trade_kind: TradeDirectionKind::Short,
            is_valid: true
        };

        assert_eq!(-1.0, trade_short.profit(101.0));
        assert_eq!(0.0, trade_short.profit(100.0));
        assert_eq!(1.0, trade_short.profit(99.0));
    }
}
