use crate::{converter::any_value::AnyValueConverter, enums::column_names::PnLReportColumnKind};
use polars::{
    datatypes::AnyValue,
    lazy::dsl::when,
    prelude::{col, lit, DataFrame, IntoLazy, SeriesMethods},
};


/// Percent Profitable = Trefferquote = Gewinner/Gesamttrades
pub fn percent_profitability(number_winner: u32, number_trades: u32) -> f64 {
    f64::try_from(number_winner).unwrap() / f64::try_from(number_trades).unwrap()
}

pub fn number_winner_trades(df: DataFrame) -> u32 {
    total_number_winner_trades(df.clone()) - number_timeout_winner_trades(df)
}
pub fn number_loser_trades(df: DataFrame) -> u32 {
    total_number_loser_trades(df.clone()) - number_timeout_loser_trades(df)
}

pub fn total_number_winner_trades(df: DataFrame) -> u32 {
    let summary = status_summary(df.clone());
    get_number_of_trades_from_summary(summary, "Winner")
}

pub fn total_number_loser_trades(df: DataFrame) -> u32 {
    let summary = status_summary(df.clone());
    get_number_of_trades_from_summary(summary, "Loser")
}

pub fn number_timeout_winner_trades(df: DataFrame) -> u32 {
    let timeout_trades_summary = timeout_summary(df);
    get_number_of_trades_from_summary(timeout_trades_summary, "Winner")
}

pub fn number_timeout_loser_trades(df: DataFrame) -> u32 {
    let timeout_trades_summary = timeout_summary(df);
    get_number_of_trades_from_summary(timeout_trades_summary, "Loser")
}

pub fn number_timeout_trades(df: DataFrame) -> u32 {
    number_timeout_winner_trades(df.clone()) + number_timeout_loser_trades(df)
}

pub fn number_no_entry(df: DataFrame) -> u32 {
    let summary = status_summary(df);
    get_number_of_trades_from_summary(summary, "NoEntry")
}

fn get_number_of_trades_from_summary(df: DataFrame, trade: &str) -> u32 {
    let status_col = PnLReportColumnKind::Status.to_string();

    let trades = df
        .lazy()
        .filter(col(&status_col).eq(lit(trade)))
        .collect()
        .unwrap();
    let (rows, _) = trades.shape();
    if rows == 0 {
        return 0
    }
    trades["counts"].get(0).unwrap().unwrap_uint32()
}

pub fn total_number_trades(df: DataFrame) -> u32 {
    let status_col = PnLReportColumnKind::Status.to_string();
    let counts = status_summary(df);
    let total = counts
        .lazy()
        .filter(
            col(&status_col)
                .neq(lit("NoEntry"))
                .and(col(&status_col).neq(lit("Not Clear"))),
        )
        .select(&[col("counts").sum()])
        .collect()
        .unwrap();
    total["counts"].get(0).unwrap().unwrap_uint32()
}

// Ratio Avg Win/Avg Lose = CRV
pub fn avg_win_by_avg_loose(avg_win: f64, avg_loss: f64) -> f64 {
    if avg_loss == 0.0 {
        return f64::INFINITY
    }
    (avg_win / avg_loss).abs()
}

pub fn avg_win(df: DataFrame) -> f64 {
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .select([col(&pl_dollar_col)])
        .filter(col(&pl_dollar_col).gt(0.0))
        .mean()
        .collect()
        .unwrap();

    if let AnyValue::Null = res[pl_dollar_col.as_str()].get(0).unwrap() {
        return 0.0
    }

    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}
pub fn avg_loss(df: DataFrame) -> f64 {
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .select([col(&pl_dollar_col)])
        .filter(col(&pl_dollar_col).lt(0.0))
        .mean()
        .collect()
        .unwrap();
    if let AnyValue::Null = res[pl_dollar_col.as_str()].get(0).unwrap() {
        return 0.0
    }
    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}
pub fn total_win(df: DataFrame) -> f64 {
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .select([col(&pl_dollar_col)])
        .filter(col(&pl_dollar_col).gt(0.0))
        .sum()
        .collect()
        .unwrap();

    if let AnyValue::Null = res[pl_dollar_col.as_str()].get(0).unwrap() {
        return 0.0
    }

    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}
pub fn total_loss(df: DataFrame) -> f64 {
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .select([col(&pl_dollar_col)])
        .filter(col(&pl_dollar_col).lt(0.0))
        .sum()
        .collect()
        .unwrap();

    if let AnyValue::Null = res[pl_dollar_col.as_str()].get(0).unwrap() {
        return 0.0
    }

    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}

pub fn timeout_win(df: DataFrame) -> f64 {
    let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
    let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .filter(
            col(&take_profit_ts)
                .eq(lit("Timeout"))
                .and(col(&stop_loss_ts).eq(lit("Timeout")))
                .and(col(&pl_dollar_col).gt(0.0)),
        )
        .select([col(&pl_dollar_col)])
        .sum()
        .collect()
        .unwrap();

    if let AnyValue::Null = res[pl_dollar_col.as_str()].get(0).unwrap() {
        return 0.0
    }

    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}

pub fn timeout_loss(df: DataFrame) -> f64 {
    let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
    let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .filter(
            col(&take_profit_ts)
                .eq(lit("Timeout"))
                .and(col(&stop_loss_ts).eq(lit("Timeout")))
                .and(col(&pl_dollar_col).lt(0.0)),
        )
        .select([col(&pl_dollar_col)])
        .sum()
        .collect()
        .unwrap();

    if let AnyValue::Null = res[pl_dollar_col.as_str()].get(0).unwrap() {
        return 0.0
    }

    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}

pub fn profit_factor(total_win: f64, total_loss: f64) -> f64 {
    if total_loss == 0.0 {
        return f64::INFINITY
    }
    (total_win / total_loss).abs()
}

pub fn status_summary(df: DataFrame) -> DataFrame {
    let status_col = PnLReportColumnKind::Status.to_string();

    df[status_col.as_str()].value_counts(true, false).unwrap()
}

pub fn timeout_summary(df: DataFrame) -> DataFrame {
    let status_col = PnLReportColumnKind::Status.to_string();
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();
    let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
    let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

    let filtered = df
        .lazy()
        .select(&[
            col(&take_profit_ts),
            col(&stop_loss_ts),
            col(&status_col),
            col(&pl_dollar_col),
        ])
        .filter(
            col(&take_profit_ts)
                .eq(lit("Timeout"))
                .and(col(&stop_loss_ts).eq(lit("Timeout"))),
        )
        .with_column(
            when(col(&pl_dollar_col).lt(0.0))
                .then(lit("Loser"))
                .otherwise(lit("Winner"))
                .alias("Timeout Status"),
        )
        .select(&[col("Timeout Status").alias("Status")])
        .collect()
        .unwrap();
    status_summary(filtered)
}

pub fn net_profit(df: DataFrame) -> f64 {
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();

    let res = df
        .lazy()
        .select(&[col(&pl_dollar_col)])
        .sum()
        .collect()
        .unwrap();
    res[pl_dollar_col.as_str()].get(0).unwrap().unwrap_float64()
}

pub fn avg_trade(net_profit: f64, number_of_trades: u32) -> f64 {
    net_profit / f64::try_from(number_of_trades).unwrap()
}

pub fn accumulated_profit(df: DataFrame, initial: f64) -> Vec<f64> {
    let pl_dollar_col = PnLReportColumnKind::PlDollar.to_string();
    let series = &df[pl_dollar_col.as_str()];

    series.rechunk().iter().fold(vec![initial], |mut acc, val| {
        acc.push(val.unwrap_float64() + acc.last().unwrap());
        acc
    })
}

pub fn max_draw_down_abs(accumulated_profit: &Vec<f64>) -> f64 {
    max_draw_down(accumulated_profit).1
}

pub fn max_draw_down_rel(accumulated_profit: &Vec<f64>) -> f64 {
    let (high, draw_down) = max_draw_down(accumulated_profit);
    if high == 0.0 {
        return f64::INFINITY
    }
    (high - draw_down) / high - 1_f64
}

fn max_draw_down(accumulated_profit: &Vec<f64>) -> (f64, f64) {
    accumulated_profit
        .iter()
        .fold((accumulated_profit[0], 0.0), |(mut high, acc), x| {
            high = high.max(*x);
            (high, acc.max(high - x))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::{df, prelude::NamedFrom};

    #[test]
    fn test_accumulated_profit() {
        let initial = 10_f64;
        let df = df!(
            "a" => &[1_i64, 2, 3],
            "PlDollar" => &[10_f64, 20.0, 30.0],
        )
        .unwrap();

        let out = df
            .clone()
            .lazy()
            .with_column(col("PlDollar").cumsum(false).alias("accumulated_profit"))
            .collect()
            .unwrap();

        let target: Vec<_> = out["accumulated_profit"]
            .f64()
            .unwrap()
            .into_iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap() + initial)
            .collect();

        let res = accumulated_profit(df, initial);
        assert_eq!(res[1..], target);
    }

    #[test]
    fn test_max_draw_down_abs() {
        let prices = [7.0, 1.0, 5.0, 3.0, 6.0, 4.0];
        assert_eq!(6.0, max_draw_down_abs(&prices.to_vec()));

        let prices = [6.0, 7.0, 6.0, 4.0, 3.0, 4.0];
        assert_eq!(4.0, max_draw_down_abs(&prices.to_vec()));
    }

    #[test]
    fn test_max_draw_down_rel() {
        let prices = [7.0, 1.0, 5.0, 3.0, 6.0, 4.0];
        assert_eq!(1.0 / 7.0 - 1.0, max_draw_down_rel(&prices.to_vec()));

        let prices = [6.0, 7.0, 6.0, 4.0, 3.0, 4.0];
        assert_eq!(3.0 / 7.0 - 1.0, max_draw_down_rel(&prices.to_vec()));
    }

    #[test]
    fn test_status_summary() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = status_summary(df);
        let target = df!(
            "Status" => &["Loser", "NoEntry", "Winner"],
            "counts" => &[3_u32, 1, 3],
        );

        assert_eq!(
            target.unwrap(),
            res.lazy()
                .sort("Status", Default::default())
                .collect()
                .unwrap()
        );
    }

    #[test]
    fn test_timeout_summary() {
        let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();
        let res = timeout_summary(df);
        let target = df!(
            "Status" => &["Loser", "Winner"],
            "counts" => &[2_u32, 1],
        );

        assert_eq!(
            target.unwrap(),
            res.lazy()
                .sort("Status", Default::default())
                .collect()
                .unwrap()
        );
    }

    #[test]
    fn test_avg_trade() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let net_profit = net_profit(df.clone());
        let number_of_trades = total_number_trades(df);

        let res = avg_trade(net_profit, number_of_trades);
        let target = (10_f64 + -20.0 + -30.0 + -20.0 + 30.0 + 10.0) / 6.0;
        assert_eq!(target, res);
    }

    #[test]
    fn test_net_profit() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = net_profit(df);
        let target = 10_f64 + -20.0 + -30.0 + -20.0 + 30.0 + 10.0;

        assert_eq!(target, res);
    }

    #[test]
    fn test_profit_factor() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let total_win = total_win(df.clone());
        let total_loss = total_loss(df.clone());
        let target_total_win = 30.0 + 10.0 + 10.0;
        let target_total_loss = -30.0 + -20.0 + -20.0;

        let target: f64 = target_total_win / target_total_loss;
        let res = profit_factor(total_win, total_loss);

        assert_eq!(target.abs(), res);
    }

    #[test]
    fn test_total_loss() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = total_loss(df);
        let target = -30.0 + -20.0 + -20.0;

        assert_eq!(target, res);
    }

    #[test]
    fn test_total_win() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = total_win(df);
        let target = 30.0 + 10.0 + 10.0;

        assert_eq!(target, res);
    }

    #[test]
    fn test_timeout_win() {
        let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = timeout_win(df);

        assert_eq!(10.0, res);
    }

    #[test]
    fn test_timeout_loss() {
        let take_profit_ts: String = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = timeout_loss(df);

        assert_eq!(-40.0, res);
    }

    #[test]
    fn test_avg_loss() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = avg_loss(df);
        let target = (-30.0 + -20.0 + -20.0) / 3.0;

        assert_eq!(target, res);
    }

    #[test]
    fn test_avg_win() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let res = avg_win(df);
        let target = (30.0 + 10.0 + 10.0) / 3.0;

        assert_eq!(target, res);
    }

    #[test]
    fn test_avg_win_by_avg_loose() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let avg_win = avg_win(df.clone());
        let target_avg_win = (30.0 + 10.0 + 10.0) / 3.0;

        let avg_loss = avg_loss(df);
        let target_avg_loss = (-30.0 + -20.0 + -20.0) / 3.0;

        let target: f64 = target_avg_win / target_avg_loss;
        let res = avg_win_by_avg_loose(avg_win, avg_loss);

        assert_eq!(target.abs(), res);
    }

    #[test]
    fn test_total_number_trades() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        assert_eq!(6, total_number_trades(df));
    }

    #[test]
    fn test_count_number_of_trades() {
        let df = df!(
            "Status" => &["Winner", "Timeout", "Loser", "NoEntry"],
            "counts" => &[2_u32, 3, 1, 1],
        )
        .unwrap();

        assert_eq!(2, get_number_of_trades_from_summary(df.clone(), "Winner"));
        assert_eq!(1, get_number_of_trades_from_summary(df.clone(), "Loser"));
        assert_eq!(3, get_number_of_trades_from_summary(df.clone(), "Timeout"));
        assert_eq!(1, get_number_of_trades_from_summary(df, "NoEntry"));
    }

    #[test]
    fn test_number_loser_trades() {
        let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        assert_eq!(3, total_number_loser_trades(df));
    }

    #[test]
    fn test_number_timeout_loser_trades() {
        let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        assert_eq!(2, number_timeout_loser_trades(df));
    }

    #[test]
    fn test_number_timeout_winner_trades() {
        let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        assert_eq!(1, number_timeout_winner_trades(df));
    }

    #[test]
    fn test_number_winner_trades() {
        let take_profit_ts = PnLReportColumnKind::TakeProfitTimestamp.to_string();
        let stop_loss_ts = PnLReportColumnKind::StopLossTimestamp.to_string();

        let df = df!(
            &take_profit_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            &stop_loss_ts => &[
                "2022-01-12",
                "Timeout",
                "2022-01-13",
                "Timeout",
                "2022-01-10",
                "Timeout",
                "2022-01-14",
            ],
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        assert_eq!(3, total_number_winner_trades(df));
    }

    #[test]
    fn test_percent_profitability() {
        let df = df!(
            "Status" => &["Winner", "Loser", "Loser", "Loser", "Winner", "Winner", "NoEntry"],
            "PlDollar" => &[10_f64, -20.0, -30.0, -20.0, 30.0, 10.0, 0.0],
        )
        .unwrap();

        let number_winner = total_number_winner_trades(df.clone());
        let number_trades = total_number_trades(df);

        assert_eq!(
            3.0 / 6.0,
            percent_profitability(number_winner, number_trades)
        );
    }
}
