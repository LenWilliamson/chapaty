use crate::enums::bots::BotKind;
use crate::enums::columns::PerformanceStatisticColumnNames;
use crate::enums::markets::MarketKind;
use crate::math::performance_statistics::{
    accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
    max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
    number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
    number_winner_trades, percent_profitability, profit_factor, total_loss,
    total_number_loser_trades, total_number_trades, total_number_winner_trades, total_win, timeout_win, timeout_loss,
};

use polars::df;
use polars::prelude::NamedFrom;
use polars::prelude::{DataFrame, Field};
pub fn schema() -> polars::prelude::Schema {
    polars::prelude::Schema::from_iter(
        vec![
            Field::new(
                &PerformanceStatisticColumnNames::Year.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::Strategy.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::Market.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NetProfit.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::AvgWinnByTrade.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::MaxDrawDownAbs.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::MaxDrawDownRel.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::PercentageProfitability.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::RatioAvgWinByAvgLoss.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::AvgWin.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::AvgLoss.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::ProfitFactor.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TotalWin.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TotalLoss.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::CleanWin.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TimeoutWin.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::CleanLoss.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TimeoutLoss.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TotalNumberWinnerTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TotalNumberLoserTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::TotalNumberTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NumberWinnerTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NumberLoserTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NumberTimeoutWinnerTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NumberTimeoutLoserTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NumberTimeoutTrades.to_string(),
                polars::prelude::DataType::UInt32,
            ),
            Field::new(
                &PerformanceStatisticColumnNames::NumberNoEntry.to_string(),
                polars::prelude::DataType::UInt32,
            ),
        ]
        .into_iter(),
    )
}

pub fn generate_performance_report(
    pl: DataFrame,
    year: u32,
    bot: BotKind,
    market: MarketKind,
) -> DataFrame {
    
    let net_profit = net_profit(pl.clone());
    let total_number_of_trades = total_number_trades(pl.clone());
    let accumulated_profit = accumulated_profit(pl.clone(), 0.0);
    let total_number_winner = total_number_winner_trades(pl.clone());
    let avg_win = avg_win(pl.clone());
    let avg_loss = avg_loss(pl.clone());
    let total_win = total_win(pl.clone());
    let total_loss = total_loss(pl.clone());
    let timeout_win = timeout_win(pl.clone());
    let timeout_loss = timeout_loss(pl.clone());
    let clean_win = total_win - timeout_win;
    let clean_loss = total_loss - timeout_loss;

    df!(
        &PerformanceStatisticColumnNames::Year.to_string() => &vec![year],
        &PerformanceStatisticColumnNames::Strategy.to_string() => &vec![bot.to_string()],
        &PerformanceStatisticColumnNames::Market.to_string() => &vec![market.to_string()],
        &PerformanceStatisticColumnNames::NetProfit.to_string() => &vec![net_profit],
        &PerformanceStatisticColumnNames::AvgWinnByTrade.to_string() => &vec![avg_trade(net_profit, total_number_of_trades)],
        &PerformanceStatisticColumnNames::MaxDrawDownAbs.to_string() => &vec![max_draw_down_abs(&accumulated_profit)],
        &PerformanceStatisticColumnNames::MaxDrawDownRel.to_string() => &vec![max_draw_down_rel(&accumulated_profit)],
        &PerformanceStatisticColumnNames::PercentageProfitability.to_string() => &vec![percent_profitability(total_number_winner, total_number_of_trades)],
        &PerformanceStatisticColumnNames::RatioAvgWinByAvgLoss.to_string() => &vec![avg_win_by_avg_loose(avg_win, avg_loss)],
        &PerformanceStatisticColumnNames::AvgWin.to_string() => &vec![avg_win],
        &PerformanceStatisticColumnNames::AvgLoss.to_string() => &vec![avg_loss],
        &PerformanceStatisticColumnNames::ProfitFactor.to_string() => &vec![profit_factor(total_win, total_loss)],
        &PerformanceStatisticColumnNames::TotalWin.to_string() => &vec![total_win],
        &PerformanceStatisticColumnNames::TotalLoss.to_string() => &vec![total_loss],
        &PerformanceStatisticColumnNames::CleanWin.to_string() => &vec![clean_win],
        &PerformanceStatisticColumnNames::TimeoutWin.to_string() => &vec![timeout_win],
        &PerformanceStatisticColumnNames::CleanLoss.to_string() => &vec![clean_loss],
        &PerformanceStatisticColumnNames::TimeoutLoss.to_string() => &vec![timeout_loss],
        &PerformanceStatisticColumnNames::TotalNumberWinnerTrades.to_string() => &vec![total_number_winner],
        &PerformanceStatisticColumnNames::TotalNumberLoserTrades.to_string() => &vec![total_number_loser_trades(pl.clone())],
        &PerformanceStatisticColumnNames::TotalNumberTrades.to_string() => &vec![total_number_of_trades],
        &PerformanceStatisticColumnNames::NumberWinnerTrades.to_string() => &vec![number_winner_trades(pl.clone())],
        &PerformanceStatisticColumnNames::NumberLoserTrades.to_string() => &vec![number_loser_trades(pl.clone())],
        &PerformanceStatisticColumnNames::NumberTimeoutWinnerTrades.to_string() => &vec![number_timeout_winner_trades(pl.clone())],
        &PerformanceStatisticColumnNames::NumberTimeoutLoserTrades.to_string() => &vec![number_timeout_loser_trades(pl.clone())],
        &PerformanceStatisticColumnNames::NumberTimeoutTrades.to_string() => &vec![number_timeout_trades(pl.clone())],
        &PerformanceStatisticColumnNames::NumberNoEntry.to_string() => &vec![number_no_entry(pl.clone())],
    ).unwrap()
}
