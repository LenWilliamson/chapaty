use super::pnl_report_data_row_calculator::TradeAndPreTradeValuesWithData;
use crate::{bot::trade::Trade, lazy_frame_operations::trait_extensions::MyLazyFrameOperations};
use polars::prelude::LazyFrame;
use std::convert::identity;

pub struct TradePnLCalculator {
    entry_ts: i64,
    trade: Trade,
    market_sim_data_since_entry: LazyFrame,
    trade_and_pre_trade_values: TradeAndPreTradeValuesWithData,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradePnL {
    pub trade_entry_ts: i64,
    pub stop_loss: Option<PnL>,
    pub take_profit: Option<PnL>,
    pub timeout: Option<PnL>,
}

impl TradePnL {
    pub fn trade_outcome(&self) -> String {
        if self.is_trade_timeout() {
            self.handle_timeout_trade()
        } else {
            self.handle_regular_trade_outcome()
        }
    }

    pub fn exit_price(&self) -> f64 {
        if self.is_trade_timeout() {
            self.timeout.clone().unwrap().price
        } else {
            self.handle_regular_trade_exit()
        }
    }

    pub fn profit(&self) -> f64 {
        if self.is_trade_timeout() {
            self.timeout.clone().unwrap().profit.clone().unwrap()
        } else {
            self.handle_regular_profit()
        }
    }

    fn handle_regular_trade_exit(&self) -> f64 {
        if self.is_stop_loss_entry_before_take_profit_entry() {
            self.stop_loss.clone().unwrap().price
        } else if self.is_stop_loss_entry_after_take_profit_entry() {
            self.take_profit.clone().unwrap().price
        } else {
            // If trade outcome not clear, be conservative and assume loser trade
            self.stop_loss.clone().unwrap().price
        }
    }

    fn handle_regular_profit(&self) -> f64 {
        if self.is_stop_loss_entry_before_take_profit_entry() {
            self.stop_loss.clone().unwrap().profit.clone().unwrap()
        } else if self.is_stop_loss_entry_after_take_profit_entry() {
            self.take_profit.clone().unwrap().profit.clone().unwrap()
        } else {
            // If trade outcome not clear, be conservative and assume loser trade
            self.stop_loss.clone().unwrap().profit.clone().unwrap()
        }
    }

    fn is_trade_timeout(&self) -> bool {
        let is_stop_loss_timeout = self.stop_loss.is_none();
        let is_take_profit_timeout = self.take_profit.is_none();

        is_stop_loss_timeout && is_take_profit_timeout
    }

    fn handle_timeout_trade(&self) -> String {
        if self.is_timeout_trade_winner() {
            "Winner".to_string()
        } else {
            "Loser".to_string()
        }
    }

    fn handle_regular_trade_outcome(&self) -> String {
        if self.is_stop_loss_entry_before_take_profit_entry() {
            "Loser".to_string()
        } else if self.is_stop_loss_entry_after_take_profit_entry() {
            "Winner".to_string()
        } else {
            "Not Clear".to_string()
        }
    }

    fn is_timeout_trade_winner(&self) -> bool {
        self.timeout.clone().unwrap().profit.clone().unwrap() > 0.0
    }

    fn is_stop_loss_entry_before_take_profit_entry(&self) -> bool {
        let sl_ts = get_entry_ts(&self.stop_loss);
        let tp_ts = get_entry_ts(&self.take_profit);
        sl_ts < tp_ts
    }

    fn is_stop_loss_entry_after_take_profit_entry(&self) -> bool {
        let sl_ts = get_entry_ts(&self.stop_loss);
        let tp_ts = get_entry_ts(&self.take_profit);
        sl_ts > tp_ts
    }
}

fn get_entry_ts(trade_pnl: &Option<PnL>) -> i64 {
    trade_pnl.clone().map_or_else(no_entry_timestamp, |pnl| {
        pnl.ts.map_or_else(no_entry_timestamp, identity)
    })
}

fn no_entry_timestamp() -> i64 {
    i64::MAX
}

#[derive(Debug, Clone, PartialEq)]
pub struct PnL {
    pub price: f64,
    pub ts: Option<i64>,
    pub profit: Option<f64>,
}

impl PnL {
    fn or_none(self) -> Option<Self> {
        if is_order_open(self.ts) {
            None
        } else {
            Some(self)
        }
    }
}

impl TradePnLCalculator {
    pub fn compute(&self) -> TradePnL {
        let stop_loss = self.handle_exit(self.trade.stop_loss);
        let take_profit = self.handle_exit(self.trade.take_profit);
        let timeout = if is_limit_order_open(stop_loss.clone(), take_profit.clone()) {
            Some(self.handle_timeout())
        } else {
            None
        };

        TradePnL {
            trade_entry_ts: self.entry_ts,
            stop_loss: stop_loss.or_none(),
            take_profit: take_profit.or_none(),
            timeout,
        }
    }

    fn handle_exit(&self, exit_px: f64) -> PnL {
        let ts = self.trade_exit_ts(exit_px);
        let profit = ts.map_or_else(|| None, |_| Some(self.trade.profit(exit_px)));

        PnL {
            price: exit_px,
            ts,
            profit,
        }
    }

    fn handle_timeout(&self) -> PnL {
        let exit_px = self.trade_and_pre_trade_values.trade.last_trade_price();

        PnL {
            price: exit_px,
            ts: Some(0),
            profit: Some(self.trade.profit(exit_px)),
        }
    }

    fn trade_exit_ts(&self, exit_px: f64) -> Option<i64> {
        self.market_sim_data_since_entry
            .clone()
            .find_timestamp_when_price_reached(exit_px)
    }
}

fn is_limit_order_open(sl: PnL, tp: PnL) -> bool {
    let is_sl_order_open = is_order_open(sl.ts);
    let is_tp_order_open = is_order_open(tp.ts);

    is_sl_order_open && is_tp_order_open
}

fn is_order_open(timestamp: Option<i64>) -> bool {
    timestamp.is_none()
}

pub struct TradePnLCalculatorBuilder {
    entry_ts: Option<i64>,
    trade: Option<Trade>,
    market_sim_data_since_entry: Option<LazyFrame>,
    trade_and_pre_trade_values: Option<TradeAndPreTradeValuesWithData>,
}

impl TradePnLCalculatorBuilder {
    pub fn new() -> Self {
        Self {
            entry_ts: None,
            trade: None,
            market_sim_data_since_entry: None,
            trade_and_pre_trade_values: None,
        }
    }

    pub fn with_market_sim_data_since_entry(self, market_sim_data_since_entry: LazyFrame) -> Self {
        Self {
            market_sim_data_since_entry: Some(market_sim_data_since_entry),
            ..self
        }
    }

    pub fn with_entry_ts(self, ts: i64) -> Self {
        Self {
            entry_ts: Some(ts),
            ..self
        }
    }

    pub fn with_trade(self, trade: Trade) -> Self {
        Self {
            trade: Some(trade),
            ..self
        }
    }

    pub fn with_trade_and_pre_trade_values(
        self,
        trade_and_pre_trade_values: TradeAndPreTradeValuesWithData,
    ) -> Self {
        Self {
            trade_and_pre_trade_values: Some(trade_and_pre_trade_values),
            ..self
        }
    }

    pub fn build(self) -> TradePnLCalculator {
        TradePnLCalculator {
            entry_ts: self.entry_ts.clone().unwrap(),
            trade: self.trade.clone().unwrap(),
            market_sim_data_since_entry: self.market_sim_data_since_entry.clone().unwrap(),
            trade_and_pre_trade_values: self.trade_and_pre_trade_values.clone().unwrap(),
        }
    }

    pub fn build_and_compute(self) -> TradePnL {
        self.build().compute()
    }
}

#[cfg(test)]
mod test {
    /**
     *
     * Testfall 1:
     *  - Zeitraum 2022-03:
     *      - Schlusskurs Freitag (2022-03-04 23:00): 39_004.73USDT
     *      - Wähle POC: 42_100.00USDT (POC so wählen das Trade nicht getriggert wird)
     *      - Short Trade findet statt (2022-03-09 09:00)
     *      - Höchster Kurs in der Woche vom Short Trade (ab Zeitpunkt des ENTRY): (2022-03-09 16:00): 42_594.06USDT => TESTEN
     *      - Niedrigster Kurs in der Woche vom Short Trade (ab Zeiptunkt des ENTRY): (2022-03-11 03:00): 38_223.60USDT => TESTEN
     *      - Schlusskurs Freitag (2022-03-11 23:00): 38_916.69USDT
     *      - Prüfe anhand der Daten der Vorwoche ob korrekterweise ein Shorttrade vorliegt
     *  - Prüfe:
     *      - Wird der Entry beim POC ausgelöst
     *      - SL1:
     *          - Wähle X = 494.07USDT => Trade läuft weiter
     *          - Wähle X = 500.00USDT => Trade läuft weiter
     *          - Wähle X = 494.06USDT => Ausgestopped, da die Bedingung "<" nicht erfüllt
     *          - Wähle X = 400.00USDT => Ausgestopped
     *      - SL2:
     *          - Berechne High & Low der Vorwoche => Testen, da Daten erst ab Dienstag vorliegen
     *              - HIGH (2022-03-02 16:00): 44_819.39USDT => Wähle HIGH da Shorttrade
     *              - LOW ist nicht am 2022-02-28 um 00-00-00 bei 37_330.23USDT da Außerhalb Zeitinterval => Testen
     *              - LOW ist 37_450.17USDT  2022-02-28 um 01-00-00
     *          - Trade läuft weiter
     *          - Setze High der Vorwoche künstlich auf 42_200.00USDT => Ausgestopped
     *      - TP1:
     *          - Wähle X = 1_000.00USDT => Gewinn = 42_100.00 - (39_004.73USDT + 1_000.00USDT)
     *          - ......nonsense: Wähle X = 4_000.00USDT => TIMEOUT mit Gewinn (macht kein Sinn, da oberhalb von POC)
     *      - TP2: Wähle X = 0.00USDT => Gewinn
     *      - TP3:
     *          - Wähle X = 500.00USDT => Gewinn = 42_100.00 - (39_004.73USDT - 500.00USDT)
     *          - Wähle X = 1_000.00USDT => TIMEOUT mit Gewinn
     *      - TIMEOUT mit Verlust fehlt => künstlich erzeugen
     *
     * Testfall 2:
     *  - Zeitraum 2022-02:
     *      - Schlusskurs Freitag (2022-02-25 23:00): 39_424.14USDT
     *      - Wähle POC: 38_100.00USDT (POC so wählen das Trade nicht getriggert wird)
     *      - Long Trade findet nicht am (2022-02-27 20:00) statt (da Wochenende) => Testen ob der Filter funktioniert, dass wir nur (Mo ab 01:00 - Fr bis 23:00) prüfen
     *      - Long Trade findet statt am (2022-02-28 01:00)
     *      - Höchster Kurs in der Woche vom Short Trade (ab Zeitpunkt des ENTRY):(2022-03-02 16:00): 45_400.39USDT => TESTEN
     *      - Niedrigster Kurs in der Woche vom Short Trade (ab Zeiptunkt des ENTRY, hier sogar gleich dem Zeitpunkt des ENTRY): (2022-02-28 01:00): 37_450.17USDT => TESTEN
     *      - Schlusskurs Freitag (2022-03-04 23:00): 39_004.73USDT
     *      - Prüfe anhand der Daten der Vorwoche ob korrekterweise ein Longtrade vorliegt
     *  - Prüfe:
     *      - Wird der Entry beim POC ausgelöst
     *      - SL1:
     *          - Wähle X = 1_000.00USDT => Trade läuft weiter
     *          - Wähle X = 649.84USDT => Trade läuft weiter
     *          - Wähle X = 649.83USDT => Ausgestopped, da die Bedingung ">" nicht erfüllt
     *          - Wähle X = 400.00USDT => Ausgestopped
     *      - SL2:
     *          - Berechne High & Low der Vorwoche => Wähle LOW da Long Trade
     *              - HIGH (2022-02-24 21:00): 39_843.00USDT
     *              - LOW ist 34_322.28USDT  2022-02-24 um 06-00-00
     *          - Trade läuft weiter
     *          - Setze Low der Vorwoche künstlich auf 37_800.00USDT => Ausgestopped
     *      - TP1:
     *          - Wähle X = 400.00USDT => GEWINN = (39_424.14 - 400) - 38_100.00
     *          - Keine weiteren Fälle hier, da TP1 die risikoaverse Version ist
     *      - TP2: Wähle X = 0.00USDT => Gewinn
     *      - TP3:
     *          - Wähle X = 5_000.00USDT => GEWINN = (39_424.14 + 5_000.00) - 38_000.00
     *          - Wähle X = 10_000.00USDT => TIMEOUT mit Gewinn
     *      - TIMEOUT mit Verlust fehlt => künstlich erzeugen
     *
     * Weiterer Testfall:
     *  - Short Trade registriert, aber kein Eintritt
     *  - Long Trade registriert, aber kein Eintritt
     *  - Jede Woche liefert resulstat mit Metadaten
     *
     * Aktuell nimmst du nur den ersten entry, was ist wenn es zwei entries gibt? Wir wirkt das auf die Margin?
     *
     * Wenn die Daten in 1Monatstabellen vorliegen muss korrekt der nächste Monat geladen werden
     *
     * Test ob höchster und niedrigster Kurs ab Zeiptunkt des ENTRY richtig bestimmt wird
     *
     * Wir brauchen einen Fall in dem der Trade nicht closed und TIMEOUT mit Gewinn / Verlust hat
     *
     * Wir müssen testen ob anhand der Daten der Vorwoche korrekt bestimmt wird ob wir einen Long oder
     * Short Trade erwarten
     *  
     * Unabhängig von Short oder Long soll es eine Funktion geben: Trade Triggerd der als Argument übergeben wird
     * ob es ein Long oder Short Trade ist. Diese prüft ob der Trade valide ist für die Kerze in der der Trade getriggerd wird.
     *  - Wir müssen Shorttrade prüfen, wenn der Eröffnungskurs unter dem POC ist
     *  - Was passiert wenn in der ersten Kerze der Trade ausgelöst wird, da POC in [Low, High], aber Low bzw. High schon ausstoppen
     */
    use super::*;
    use crate::{
        bot::trade::Trade,
        calculator::{pre_trade_values_calculator::RequiredPreTradeValuesWithData, trade_values_calculator::TradeValuesWithData},
        cloud_api::api_for_unit_tests::download_df,
        enums::{
            indicator::{PriceHistogramKind, TradingIndicatorKind},
            my_any_value::MyAnyValueKind,
            trade_and_pre_trade::{PreTradeDataKind, TradeDataKind, TradeDirectionKind},
        },
    };
    use polars::prelude::IntoLazy;
    use std::collections::HashMap;

    fn set_up_pre_trade_indicator_values_ppp_long() -> HashMap<TradingIndicatorKind, f64> {
        HashMap::from([(
            TradingIndicatorKind::Poc(PriceHistogramKind::VolAggTrades),
            38_100.0,
        )])
    }

    fn set_up_pre_trade_market_values_ppp_long() -> HashMap<PreTradeDataKind, f64> {
        HashMap::from([
            (PreTradeDataKind::LastTradePrice, 39_424.14),
            (PreTradeDataKind::LowestTradePrice, 36_220.54),
            (PreTradeDataKind::HighestTradePrice, 39_843.0),
        ])
    }

    fn set_up_trade_data_map_ppp_long(entry_ts: i64) -> TradeValuesWithData {
        let trade = HashMap::from([
            (
                TradeDataKind::EntryTimestamp,
                MyAnyValueKind::Int64(entry_ts),
            ),
            (
                TradeDataKind::LastTradePrice,
                MyAnyValueKind::Float64(43_160.0),
            ),
            (
                TradeDataKind::LowestTradePriceSinceEntry,
                MyAnyValueKind::Float64(37_451.56),
            ),
            (
                TradeDataKind::HighestTradePriceSinceEntry,
                MyAnyValueKind::Float64(44_225.84),
            ),
            (
                TradeDataKind::LowestTradePriceSinceEntryTimestamp,
                MyAnyValueKind::Int64(1646028000000),
            ),
            (
                TradeDataKind::HighestTradePriceSinceEntryTimestamp,
                MyAnyValueKind::Int64(1646085600000),
            ),
        ]);

        TradeValuesWithData { trade }
    }

    fn set_up_pre_trade_values_ppp_long() -> RequiredPreTradeValuesWithData {
        RequiredPreTradeValuesWithData {
            market_valeus: set_up_pre_trade_market_values_ppp_long(),
            indicator_values: set_up_pre_trade_indicator_values_ppp_long(),
        }
    }

    fn set_up_trade_ppp_long(entry_price: f64, stop_loss: f64, take_profit: f64) -> Trade {
        Trade {
            entry_price,
            stop_loss,
            take_profit,
            trade_kind: TradeDirectionKind::Long,
        }
    }

    fn set_up_trade_and_pre_trade_values_ppp_long(entry_ts: i64) -> TradeAndPreTradeValuesWithData {
        TradeAndPreTradeValuesWithData {
            trade: set_up_trade_data_map_ppp_long(entry_ts),
            pre_trade: set_up_pre_trade_values_ppp_long(),
        }
    }

    fn set_up_trade_pnl_calculator_ppp_long(
        entry_ts: i64,
        entry_price: f64,
        stop_loss: f64,
        take_profit: f64,
        market_sim_data_since_entry: LazyFrame,
    ) -> TradePnLCalculator {
        TradePnLCalculator {
            entry_ts,
            trade: set_up_trade_ppp_long(entry_price, stop_loss, take_profit),
            market_sim_data_since_entry,
            trade_and_pre_trade_values: set_up_trade_and_pre_trade_values_ppp_long(entry_ts),
        }
    }

    fn set_up_target_trade_pnl_ppp_long_case_1a(entry_ts: i64) -> TradePnL {
        let stop_loss = Some(PnL {
            price: 38_100.0,
            ts: Some(1646010000000),
            profit: Some(0.0),
        });
        let take_profit = Some(PnL {
            price: 39_424.14,
            ts: Some(1646056800000),
            profit: Some(1_324.1399999999994),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss,
            take_profit,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_long_case_1b(entry_ts: i64) -> TradePnL {
        let stop_loss = Some(PnL {
            price: 37_451.56,
            ts: Some(1646028000000),
            profit: Some(-648.4400000000023),
        });
        let take_profit = Some(PnL {
            price: 39_324.14,
            ts: Some(1646056800000),
            profit: Some(1224.1399999999994),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss,
            take_profit,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_long_case_2(entry_ts: i64) -> TradePnL {
        let stop_loss = Some(PnL {
            price: 37_451.56,
            ts: Some(1646028000000),
            profit: Some(-648.4400000000023),
        });
        // No timeout, as "is_limit_order_open == false"
        // let timeout = Some(PnL {
        //     price:  43_160.0,
        //     ts: Some(0),
        //     profit: Some(5060.0),
        // });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss,
            take_profit: None,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_long_case_3(entry_ts: i64) -> TradePnL {
        let take_profit = Some(PnL {
            price: 39_324.14,
            ts: Some(1646056800000),
            profit: Some(1224.1399999999994),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss: None,
            take_profit,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_long_case_4(entry_ts: i64) -> TradePnL {
        let timeout = Some(PnL {
            price: 43_160.0,
            ts: Some(0),
            profit: Some(5060.0),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss: None,
            take_profit: None,
            timeout,
        }
    }

    /// This unit test computes the `TradePnL` consiting of
    /// * `price` - the stop loss or take profit price
    /// * `timestamp` - when the condition is taken, otherwise `None`
    /// * `profit` - the loss, i.e. `profit < 0` we made when the condition is taken, otherwise `None`
    ///
    /// # Test data
    /// The test data files are stored inside `chapaty-ai-test/ppp/_test_data_files/*.csv`
    /// * `8_vol.csv` contains the volume profile for `9_long.csv`
    /// * `9_vol.csv` contains the volume profile for `10_short.csv`
    ///
    /// We test against all possible cases at least once. For Case (1) we test two cases: a) and b).
    ///
    /// |   |SL |TP |TO |
    /// |---|---|---|---|
    /// |1) | T | T | - |
    /// |2) | T | F | - |
    /// |3) | F | T | - |
    /// |4) | F | F | T |
    #[tokio::test]
    async fn test_trade_pnl_ppp_long() {
        let df_long = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/9_long_curr.csv".to_string(),
        )
        .await;

        // BEGIN: Case 1
        let entry_ts = 1646010000000;
        let poc = 38_100.0;
        let price_upon_entry = poc; // triggered
        let prev_close = 39_424.14; // triggered
        let market_sim_data_since_entry =
            df_long.clone().lazy().drop_rows_before_entry_ts(entry_ts);

        let calculator = set_up_trade_pnl_calculator_ppp_long(
            entry_ts,
            poc,
            price_upon_entry,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_long_case_1a(entry_ts);
        assert_eq!(target, calculator.compute());

        let price_upon_entry = poc - 648.44; // triggered
        let prev_close = 39_424.14 - 100.0; // triggered

        let calculator = set_up_trade_pnl_calculator_ppp_long(
            entry_ts,
            poc,
            price_upon_entry,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_long_case_1b(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 1

        // BEGIN: Case 2
        let prev_low = 36_220.54 + 1_231.02; // triggered
        let prev_close = 39_424.14 + 10_000.0; // Timeout

        let calculator = set_up_trade_pnl_calculator_ppp_long(
            entry_ts,
            poc,
            prev_low,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_long_case_2(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 2

        // BEGIN: Case 3
        let price_upon_entry = poc - 648.45; // not triggered => Timeout
        let prev_close = 39_424.14 - 100.0; // triggered

        let calculator = set_up_trade_pnl_calculator_ppp_long(
            entry_ts,
            poc,
            price_upon_entry,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_long_case_3(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 3

        // BEGIN: Case 4
        let prev_low = 36_220.54; // not triggered => Timeout
        let prev_close = 39_424.14 + 10_000.0; // Timeout

        let calculator = set_up_trade_pnl_calculator_ppp_long(
            entry_ts,
            poc,
            prev_low,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_long_case_4(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 4
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // #####################################################################################################################
    // ---------------------------------------------------------------------------------------------------------------------
    // ---------------------------------------------------------------------------------------------------------------------
    // #####################################################################################################################
    // ---------------------------------------------------------------------------------------------------------------------
    // ---------------------------------------------------------------------------------------------------------------------
    // #####################################################################################################################
    // ---------------------------------------------------------------------------------------------------------------------

    fn set_up_pre_trade_indicator_values_ppp_short() -> HashMap<TradingIndicatorKind, f64> {
        HashMap::from([(
            TradingIndicatorKind::Poc(PriceHistogramKind::VolAggTrades),
            42_100.0,
        )])
    }

    fn set_up_pre_trade_market_values_ppp_short() -> HashMap<PreTradeDataKind, f64> {
        HashMap::from([
            (PreTradeDataKind::LastTradePrice, 39_004.73),
            (PreTradeDataKind::LowestTradePrice, 38_550.0),
            (PreTradeDataKind::HighestTradePrice, 44_101.12),
        ])
    }

    fn set_up_trade_data_map_ppp_short(entry_ts: i64) -> TradeValuesWithData {
        let trade = HashMap::from([
            (
                TradeDataKind::EntryTimestamp,
                MyAnyValueKind::Int64(entry_ts),
            ),
            (
                TradeDataKind::LastTradePrice,
                MyAnyValueKind::Float64(39_385.01),
            ),
            (
                TradeDataKind::LowestTradePriceSinceEntry,
                MyAnyValueKind::Float64(38_848.48),
            ),
            (
                TradeDataKind::HighestTradePriceSinceEntry,
                MyAnyValueKind::Float64(42_594.06),
            ),
            (
                TradeDataKind::LowestTradePriceSinceEntryTimestamp,
                MyAnyValueKind::Int64(1646888400000),
            ),
            (
                TradeDataKind::HighestTradePriceSinceEntryTimestamp,
                MyAnyValueKind::Int64(1646838000000),
            ),
        ]);

        TradeValuesWithData { trade }
    }

    fn set_up_pre_trade_values_ppp_short() -> RequiredPreTradeValuesWithData {
        RequiredPreTradeValuesWithData {
            market_valeus: set_up_pre_trade_market_values_ppp_short(),
            indicator_values: set_up_pre_trade_indicator_values_ppp_short(),
        }
    }

    fn set_up_trade_ppp_short(entry_price: f64, stop_loss: f64, take_profit: f64) -> Trade {
        Trade {
            entry_price,
            stop_loss,
            take_profit,
            trade_kind: TradeDirectionKind::Short,
        }
    }

    fn set_up_trade_and_pre_trade_values_ppp_short(
        entry_ts: i64,
    ) -> TradeAndPreTradeValuesWithData {
        TradeAndPreTradeValuesWithData {
            trade: set_up_trade_data_map_ppp_short(entry_ts),
            pre_trade: set_up_pre_trade_values_ppp_short(),
        }
    }

    fn set_up_trade_pnl_calculator_ppp_short(
        entry_ts: i64,
        entry_price: f64,
        stop_loss: f64,
        take_profit: f64,
        market_sim_data_since_entry: LazyFrame,
    ) -> TradePnLCalculator {
        TradePnLCalculator {
            entry_ts,
            trade: set_up_trade_ppp_short(entry_price, stop_loss, take_profit),
            market_sim_data_since_entry,
            trade_and_pre_trade_values: set_up_trade_and_pre_trade_values_ppp_short(entry_ts),
        }
    }

    fn set_up_target_trade_pnl_ppp_short_case_1a(entry_ts: i64) -> TradePnL {
        let stop_loss = Some(PnL {
            price: 42_100.0,
            ts: Some(1646812800000),
            profit: Some(0.0),
        });
        let take_profit = Some(PnL {
            price: 39_004.73,
            ts: Some(1646888400000),
            profit: Some(3_095.269999999997),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss,
            take_profit,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_short_case_1b(entry_ts: i64) -> TradePnL {
        let stop_loss = Some(PnL {
            price: 42_594.06,
            ts: Some(1646838000000),
            profit: Some(-494.0599999999977),
        });
        let take_profit = Some(PnL {
            price: 39_104.73,
            ts: Some(1646888400000),
            profit: Some(2_995.269999999997),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss,
            take_profit,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_short_case_2(entry_ts: i64) -> TradePnL {
        let stop_loss = Some(PnL {
            price: 42_594.06,
            ts: Some(1646838000000),
            profit: Some(-494.0599999999977),
        });
        // No timeout, as "is_limit_order_open == false"
        // let timeout = Some(PnL {
        //     price: 39_385.01,
        //     ts: Some(0),
        //     profit: Some(2714.989999999998),
        // });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss,
            take_profit: None,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_short_case_3(entry_ts: i64) -> TradePnL {
        let take_profit = Some(PnL {
            price: 39_104.73,
            ts: Some(1646888400000),
            profit: Some(2_995.269999999997),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss: None,
            take_profit,
            timeout: None,
        }
    }

    fn set_up_target_trade_pnl_ppp_short_case_4(entry_ts: i64) -> TradePnL {
        let timeout = Some(PnL {
            price: 39_385.01,
            ts: Some(0),
            profit: Some(2714.989999999998),
        });
        TradePnL {
            trade_entry_ts: entry_ts,
            stop_loss: None,
            take_profit: None,
            timeout,
        }
    }

    /// This unit test computes the `TradePnL` consiting of
    /// * `price` - the stop loss or take profit price
    /// * `timestamp` - when the condition is taken, otherwise `None`
    /// * `profit` - the loss, i.e. `profit < 0` we made when the condition is taken, otherwise `None`
    ///
    /// # Test data
    /// The test data files are stored inside `chapaty-ai-test/ppp/_test_data_files/*.csv`
    /// * `8_vol.csv` contains the volume profile for `9_long.csv`
    /// * `9_vol.csv` contains the volume profile for `10_short.csv`
    ///
    /// We test against all possible cases at least once. For Case (1) we test two cases: a) and b).
    ///
    /// |   |SL |TP |TO |
    /// |---|---|---|---|
    /// |1) | T | T | - |
    /// |2) | T | F | - |
    /// |3) | F | T | - |
    /// |4) | F | F | T |
    #[tokio::test]
    async fn test_trade_pnl_ppp_short() {
        let df_long = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/10_short_curr.csv".to_string(),
        )
        .await;

        // BEGIN: Case 1
        let entry_ts = 1646812800000;
        let poc = 42_100.0;
        let price_upon_entry = poc; // triggered
        let prev_close = 39_004.73; // triggered
        let market_sim_data_since_entry =
            df_long.clone().lazy().drop_rows_before_entry_ts(entry_ts);

        let calculator = set_up_trade_pnl_calculator_ppp_short(
            entry_ts,
            poc,
            price_upon_entry,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_short_case_1a(entry_ts);
        assert_eq!(target, calculator.compute());

        let price_upon_entry = poc + 494.06; // triggered
        let prev_close = 39_004.73 + 100.0; // triggered

        let calculator = set_up_trade_pnl_calculator_ppp_short(
            entry_ts,
            poc,
            price_upon_entry,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_short_case_1b(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 1

        // BEGIN: Case 2
        // Floating point error: 44_101.12 - 1_507.06 = 42594.060000000005 > 42594.06
        let prev_high = 44_101.12 - 1_507.060000000005; // triggered
        let prev_close = 39_424.14 - 10_000.0; // Timeout

        let calculator = set_up_trade_pnl_calculator_ppp_short(
            entry_ts,
            poc,
            prev_high,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_short_case_2(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 2

        // BEGIN: Case 3
        let prev_high = 44_101.12 + 500.0; // not triggered => Timeout
        let prev_close = 39_004.73 + 100.0; // triggered

        let calculator = set_up_trade_pnl_calculator_ppp_short(
            entry_ts,
            poc,
            prev_high,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_short_case_3(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 3

        // BEGIN: Case 4
        let price_upon_entry = poc + 494.07; // not triggered => Timeout
        let prev_close = 39_004.7 - 156.26; // not triggered => Timeout

        let calculator = set_up_trade_pnl_calculator_ppp_short(
            entry_ts,
            poc,
            price_upon_entry,
            prev_close,
            market_sim_data_since_entry.clone(),
        );
        let target = set_up_target_trade_pnl_ppp_short_case_4(entry_ts);
        assert_eq!(target, calculator.compute());
        // END: Case 4
    }
}
