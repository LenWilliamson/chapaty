use crate::enums::trade_and_pre_trade::TradeDirectionKind;

#[derive(Debug, Clone)]
pub struct Trade {
    pub entry_price: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub trade_kind: TradeDirectionKind,
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
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_compute_trade_values() {
        let trade_long = Trade {
            entry_price: 100.0,
            stop_loss: -1.0,
            take_profit: -1.0,
            trade_kind: TradeDirectionKind::Long,
        };

        assert_eq!(1.0, trade_long.profit(101.0));
        assert_eq!(0.0, trade_long.profit(100.0));
        assert_eq!(-1.0, trade_long.profit(99.0));

        let trade_short = Trade {
            entry_price: 100.0,
            stop_loss: -1.0,
            take_profit: -1.0,
            trade_kind: TradeDirectionKind::Short,
        };

        assert_eq!(-1.0, trade_short.profit(101.0));
        assert_eq!(0.0, trade_short.profit(100.0));
        assert_eq!(1.0, trade_short.profit(99.0));
    }
}
