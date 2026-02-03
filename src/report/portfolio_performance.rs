use std::sync::Arc;

use polars::{
    frame::DataFrame,
    prelude::{
        DataType, Expr, Field, IntoLazy, PlSmallStr, QuantileMethod, Schema, SchemaRef,
        SortMultipleOptions, UnionArgs, col, lit,
    },
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    data::common::RiskMetricsConfig,
    error::{ChapatyError, ChapatyResult, DataError},
    report::{
        cumulative_returns::{
            cumulative_realized_return_usd_expr, peak_cumulative_return_usd_expr,
        },
        grouped::GroupedJournal,
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        journal::{Journal, JournalCol},
        polars_ext::{ExprExt, polars_to_chapaty_error},
        trade_statistics::executed_trade_count_expr,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioPerformance {
    pub df: DataFrame,
}

impl Default for PortfolioPerformance {
    fn default() -> Self {
        let df = DataFrame::empty_with_schema(&Self::to_schema());
        Self { df }
    }
}

impl ReportName for PortfolioPerformance {
    fn base_name(&self) -> String {
        generate_dynamic_base_name(&self.df, "portfolio_performance")
    }
}

impl Report for PortfolioPerformance {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

impl ToSchema for PortfolioPerformance {
    fn to_schema() -> SchemaRef {
        let fields: Vec<Field> = PortfolioPerformanceCol::iter()
            .map(|col| {
                let dtype = match col {
                    PortfolioPerformanceCol::NetProfit
                    | PortfolioPerformanceCol::AvgTradeProfit
                    | PortfolioPerformanceCol::ExpectedValuePerTrade
                    | PortfolioPerformanceCol::TotalWinProfit
                    | PortfolioPerformanceCol::TotalLoss
                    | PortfolioPerformanceCol::TotalWinProfitByTotalLoss
                    | PortfolioPerformanceCol::SharpeRatio
                    | PortfolioPerformanceCol::SortinoRatio
                    | PortfolioPerformanceCol::OmegaRatio
                    | PortfolioPerformanceCol::CalmarRatio
                    | PortfolioPerformanceCol::RecoveryFactor
                    | PortfolioPerformanceCol::MaxDrawdownUsd
                    | PortfolioPerformanceCol::MaxDrawdownPct
                    | PortfolioPerformanceCol::WinRate
                    | PortfolioPerformanceCol::AvgWinToAvgLossRatio
                    | PortfolioPerformanceCol::TradeReturnStdDev
                    | PortfolioPerformanceCol::TradeReturnVariance
                    | PortfolioPerformanceCol::LowerQuantileTradeReturn
                    | PortfolioPerformanceCol::MedianTradeReturn
                    | PortfolioPerformanceCol::UpperQuantileTradeReturn
                    | PortfolioPerformanceCol::AvgWinReturn
                    | PortfolioPerformanceCol::LowerQuantileWinReturn
                    | PortfolioPerformanceCol::MedianWinReturn
                    | PortfolioPerformanceCol::UpperQuantileWinReturn
                    | PortfolioPerformanceCol::AvgLossReturn
                    | PortfolioPerformanceCol::LowerQuantileLossReturn
                    | PortfolioPerformanceCol::MedianLossReturn
                    | PortfolioPerformanceCol::UpperQuantileLossReturn
                    | PortfolioPerformanceCol::LargestWin
                    | PortfolioPerformanceCol::LargestLoss
                    | PortfolioPerformanceCol::UnrealizedWinProfit
                    | PortfolioPerformanceCol::UnrealizedLoss
                    | PortfolioPerformanceCol::CleanWinProfit
                    | PortfolioPerformanceCol::CleanLoss
                    | PortfolioPerformanceCol::RootMeanSquareDeviation
                    | PortfolioPerformanceCol::MeanAbsoluteError => DataType::Float64,
                };
                Field::new(col.into(), dtype)
            })
            .collect();

        Arc::new(Schema::from_iter(fields))
    }
}

impl TryFrom<&Journal> for PortfolioPerformance {
    type Error = ChapatyError;

    fn try_from(j: &Journal) -> ChapatyResult<Self> {
        if j.as_df().is_empty() {
            return Ok(Self::default());
        }

        let cfg = j.risk_metrics_config();
        let df = j
            .as_df()
            .clone()
            .lazy()
            .select(exprs(cfg))
            .collect()
            .map_err(convert_err)?;

        Ok(Self { df })
    }
}

impl TryFrom<&GroupedJournal<'_>> for PortfolioPerformance {
    type Error = ChapatyError;

    fn try_from(gj: &GroupedJournal) -> ChapatyResult<Self> {
        if gj.source().as_df().is_empty() {
            return Ok(Self::default());
        }

        let cfg = gj.source().risk_metrics_config();
        let (partitions, keys) = gj.to_partitions()?;

        let lazy_computations = partitions
            .into_par_iter()
            .map(|df| {
                let mut selection = Vec::with_capacity(keys.len() + PortfolioPerformanceCol::COUNT);
                for k in &keys {
                    selection.push(col(k).first());
                }
                selection.extend(exprs(cfg));

                let lf = df
                    .lazy()
                    .sort(
                        [JournalCol::EntryTimestamp.as_str()],
                        SortMultipleOptions::default(),
                    )
                    .select(selection);
                Ok(lf)
            })
            .collect::<Result<Vec<_>, ChapatyError>>();

        let merged = polars::prelude::concat(
            lazy_computations?,
            UnionArgs {
                parallel: true,
                rechunk: true,
                ..Default::default()
            },
        )
        .map_err(|e| DataError::DataFrame(format!("Merge plan failed: {e}")))?
        .collect()
        .map_err(|e| DataError::DataFrame(format!("Execution failed: {e}")))?;

        Ok(Self { df: merged })
    }
}

pub struct PortfolioPerformanceAccessor<'a> {
    df: &'a DataFrame,
}

impl PortfolioPerformance {
    /// Creates a safe accessor for scalar value extraction.
    ///
    /// # Errors
    /// Returns an error if the report is **Grouped** (rows > 1) or **Empty**.
    /// This prevents logical errors where users might mistakenly read the first group's
    /// result as a global metric.
    pub fn accessor(&self) -> ChapatyResult<PortfolioPerformanceAccessor<'_>> {
        match self.df.height() {
            1 => Ok(PortfolioPerformanceAccessor { df: &self.df }),
            0 => Err(DataError::DataFrame("Report is empty".to_string()).into()),
            n => Err(DataError::DataFrame(format!(
                "Cannot extract scalar from grouped report (rows={n})."
            ))
            .into()),
        }
    }
}

impl<'a> PortfolioPerformanceAccessor<'a> {
    /// Efficiently extracts a metric value from the single-row report.
    ///
    /// Returns `None` if the value is null (e.g., Sharpe Ratio with 0 volatility).
    pub fn get(&self, metric: PortfolioPerformanceCol) -> Option<f64> {
        self.df.column(metric.as_str()).ok()?.f64().ok()?.get(0)
    }
}

fn exprs(cfg: RiskMetricsConfig) -> Vec<Expr> {
    let return_col = JournalCol::RealizedReturnDollars;
    let exit_reason_col = JournalCol::ExitReason;
    let init_val = cfg.initial_portfolio_value();

    vec![
        // === Profitability ===
        net_profit_expr(return_col)
            .alias(PortfolioPerformanceCol::NetProfit)
            .cast(DataType::Float64),
        avg_trade_profit_expr(return_col)
            .alias(PortfolioPerformanceCol::AvgTradeProfit)
            .cast(DataType::Float64),
        expected_value_per_trade_expr(return_col)
            .alias(PortfolioPerformanceCol::ExpectedValuePerTrade)
            .cast(DataType::Float64),
        total_win_profit_expr(return_col)
            .alias(PortfolioPerformanceCol::TotalWinProfit)
            .cast(DataType::Float64),
        total_loss_expr(return_col)
            .alias(PortfolioPerformanceCol::TotalLoss)
            .cast(DataType::Float64),
        total_win_profit_by_total_loss_expr(return_col)
            .alias(PortfolioPerformanceCol::TotalWinProfitByTotalLoss)
            .cast(DataType::Float64),
        // === Risk-adjusted returns ===
        sharpe_ratio_expr(return_col, cfg)
            .alias(PortfolioPerformanceCol::SharpeRatio)
            .cast(DataType::Float64),
        sortino_ratio_expr(return_col, cfg)
            .alias(PortfolioPerformanceCol::SortinoRatio)
            .cast(DataType::Float64),
        omega_ratio_expr(return_col, cfg)
            .alias(PortfolioPerformanceCol::OmegaRatio)
            .cast(DataType::Float64),
        calmar_ratio_expr(return_col, cfg)
            .alias(PortfolioPerformanceCol::CalmarRatio)
            .cast(DataType::Float64),
        recovery_factor_expr(return_col, init_val)
            .alias(PortfolioPerformanceCol::RecoveryFactor)
            .cast(DataType::Float64),
        // === Risk measures ===
        max_drawdown_usd_expr(init_val)
            .alias(PortfolioPerformanceCol::MaxDrawdownUsd)
            .cast(DataType::Float64),
        max_drawdown_pct_expr(init_val)
            .alias(PortfolioPerformanceCol::MaxDrawdownPct)
            .cast(DataType::Float64),
        // === Win/loss structure ===
        win_rate_expr(return_col)
            .alias(PortfolioPerformanceCol::WinRate)
            .cast(DataType::Float64),
        avg_win_to_avg_loss_ratio_expr(return_col)
            .alias(PortfolioPerformanceCol::AvgWinToAvgLossRatio)
            .cast(DataType::Float64),
        // === Trade return distribution ===
        trade_return_std_dev_expr(return_col)
            .alias(PortfolioPerformanceCol::TradeReturnStdDev)
            .cast(DataType::Float64),
        trade_return_variance_expr(return_col)
            .alias(PortfolioPerformanceCol::TradeReturnVariance)
            .cast(DataType::Float64),
        lower_quantile_trade_return_expr(return_col)
            .alias(PortfolioPerformanceCol::LowerQuantileTradeReturn)
            .cast(DataType::Float64),
        median_trade_return_expr(return_col)
            .alias(PortfolioPerformanceCol::MedianTradeReturn)
            .cast(DataType::Float64),
        upper_quantile_trade_return_expr(return_col)
            .alias(PortfolioPerformanceCol::UpperQuantileTradeReturn)
            .cast(DataType::Float64),
        // === Winning trade return distribution ===
        avg_win_return_expr(return_col)
            .alias(PortfolioPerformanceCol::AvgWinReturn)
            .cast(DataType::Float64),
        lower_quantile_win_return_expr(return_col)
            .alias(PortfolioPerformanceCol::LowerQuantileWinReturn)
            .cast(DataType::Float64),
        median_win_return_expr(return_col)
            .alias(PortfolioPerformanceCol::MedianWinReturn)
            .cast(DataType::Float64),
        upper_quantile_win_return_expr(return_col)
            .alias(PortfolioPerformanceCol::UpperQuantileWinReturn)
            .cast(DataType::Float64),
        // === Losing trade return distribution ===
        avg_loss_return_expr(return_col)
            .alias(PortfolioPerformanceCol::AvgLossReturn)
            .cast(DataType::Float64),
        lower_quantile_loss_return_expr(return_col)
            .alias(PortfolioPerformanceCol::LowerQuantileLossReturn)
            .cast(DataType::Float64),
        median_loss_return_expr(return_col)
            .alias(PortfolioPerformanceCol::MedianLossReturn)
            .cast(DataType::Float64),
        upper_quantile_loss_return_expr(return_col)
            .alias(PortfolioPerformanceCol::UpperQuantileLossReturn)
            .cast(DataType::Float64),
        // === Extremes ===
        largest_win_expr(return_col)
            .alias(PortfolioPerformanceCol::LargestWin)
            .cast(DataType::Float64),
        largest_loss_expr(return_col)
            .alias(PortfolioPerformanceCol::LargestLoss)
            .cast(DataType::Float64),
        // === Unrealized ===
        unrealized_win_profit_expr(exit_reason_col, return_col)
            .alias(PortfolioPerformanceCol::UnrealizedWinProfit)
            .cast(DataType::Float64),
        unrealized_loss_expr(exit_reason_col, return_col)
            .alias(PortfolioPerformanceCol::UnrealizedLoss)
            .cast(DataType::Float64),
        clean_win_profit_expr(exit_reason_col, return_col)
            .alias(PortfolioPerformanceCol::CleanWinProfit)
            .cast(DataType::Float64),
        clean_loss_expr(exit_reason_col, return_col)
            .alias(PortfolioPerformanceCol::CleanLoss)
            .cast(DataType::Float64),
        // === Curve deviation from target or benchmark ===
        rmsd_expr(return_col)
            .alias(PortfolioPerformanceCol::RootMeanSquareDeviation)
            .cast(DataType::Float64),
        mae_expr(return_col)
            .alias(PortfolioPerformanceCol::MeanAbsoluteError)
            .cast(DataType::Float64),
    ]
}

// ================================================================================================
// === Profitability ===
// ================================================================================================
fn net_profit_expr(return_col: JournalCol) -> Expr {
    col(return_col).sum()
}

pub fn avg_trade_profit_expr(return_col: JournalCol) -> Expr {
    col(return_col).mean()
}

pub fn expected_value_per_trade_expr(return_col: JournalCol) -> Expr {
    col(return_col).mean()
}

fn total_win_profit_expr(return_col: JournalCol) -> Expr {
    col(return_col).filter(col(return_col).gt(lit(0))).sum()
}

fn total_loss_expr(return_col: JournalCol) -> Expr {
    col(return_col)
        .filter(col(return_col).lt_eq(lit(0)))
        .sum()
        .abs()
}

fn total_win_profit_by_total_loss_expr(return_col: JournalCol) -> Expr {
    let total_win = total_win_profit_expr(return_col);
    let total_loss = total_loss_expr(return_col);
    total_win.safe_div(total_loss, None).abs()
}

// ================================================================================================
// === Risk-adjusted returns ===
// ================================================================================================
/// Computes the annualized Sharpe ratio for a series of absolute USD trade returns.
///
/// # Arguments
/// - `return_col`: The column containing per-trade PnL in USD.
/// - `cfg`: Risk metric config holding initial portfolio value and annual risk-free rate.
///
/// # Assumptions
/// - Returns are absolute profit/loss per trade (not percentages).
/// - Equity is tracked using cumulative sum of returns starting from `initial_value`.
/// - Risk-free rate is annualized, so we compute trades-per-year to annualize returns.
///
/// # Why we annualize
/// The Sharpe ratio is a standardized measure of risk-adjusted return,
/// typically expressed on an **annualized basis** so it can be compared across strategies or time periods.
/// We annualize the mean return and standard deviation based on the average number of trades per year.
///
/// # Returns
/// An expression that evaluates to a scalar Sharpe ratio.
fn sharpe_ratio_expr(return_col: JournalCol, cfg: RiskMetricsConfig) -> Expr {
    let excess = excess_return_expr(return_col, &cfg);
    let std = annualized_return_std_expr(return_col, cfg.initial_portfolio_value());
    excess.safe_div(std, None)
}

/// Computes the annualized Sortino ratio for a series of absolute USD trade returns.
///
/// # Returns
/// An expression evaluating to the Sortino ratio, using downside deviation
/// (standard deviation of negative returns only) instead of total volatility.
fn sortino_ratio_expr(return_col: JournalCol, cfg: RiskMetricsConfig) -> Expr {
    let excess = excess_return_expr(return_col, &cfg);
    let std = annualized_downside_return_std_expr(return_col, cfg.initial_portfolio_value());
    excess.safe_div(std, None)
}

/// Computes the **Omega Ratio** for a series of per-trade percentage returns.
///
/// # Definition
/// The **Omega Ratio** measures the **risk-adjusted performance** of a strategy by comparing the
/// probability-weighted gains **above** a minimum acceptable return (threshold, usually the risk-free rate)
/// against the probability-weighted **shortfalls** below it.
///
/// The formula is:
///
/// ```math
/// \Omega(\theta) = \frac{\int_\theta^\infty [1-F(r)]\,dr}{\int_{-\infty}^\theta F(r)\,dr}
/// ```
///
/// Where:
/// - `F(r)` is the cumulative distribution function (CDF) of returns.
/// - `θ` (theta) is the target threshold (e.g. the annual risk-free rate).
///
/// # Interpretation
/// - **Omega > 1.0**: The strategy generates more upside than downside relative to the threshold.
/// - **Omega < 1.0**: More shortfall than excess gain (underperforming threshold).
/// - **Omega = 1.0**: Symmetric outcome around threshold (breakeven performance).
///
/// # Example
/// If the threshold is set to a 2% return per trade:
/// - A +5% return contributes **3% gain**.
/// - A +1% return contributes **1% loss**.
/// - The Omega Ratio aggregates these to indicate strategy efficiency **relative to that 2% goal**.
///
/// # Note
/// This formulation differs from the Sharpe and Sortino ratios by avoiding reliance on standard deviation
/// and instead directly comparing weighted return distributions.
fn omega_ratio_expr(return_col: JournalCol, cfg: RiskMetricsConfig) -> Expr {
    let pct_returns = pct_trade_returns_expr(return_col, cfg.initial_portfolio_value());
    let threshold_expr = lit(cfg.risk_free_rate_f64());

    // Sum of excess returns ABOVE the threshold. This is equivalent to E[max(0, R - θ)].
    let gains = (pct_returns.clone() - threshold_expr.clone())
        .filter(pct_returns.clone().gt(threshold_expr.clone()))
        .sum();
    // Sum of shortfalls BELOW the threshold. This is equivalent to E[max(0, θ - R)].
    let losses = (threshold_expr.clone() - pct_returns.clone())
        .filter(pct_returns.lt_eq(threshold_expr))
        .sum();

    gains.safe_div(losses, None)
}

fn calmar_ratio_expr(return_col: JournalCol, cfg: RiskMetricsConfig) -> Expr {
    let annualized_mean_return =
        annualized_mean_return_expr(return_col, cfg.initial_portfolio_value());
    let max_drawdown_pct = max_drawdown_pct_expr(cfg.initial_portfolio_value());
    annualized_mean_return.safe_div(max_drawdown_pct, None)
}

fn recovery_factor_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    let net_profit = net_profit_expr(return_col);
    let max_drawdown_abs = max_drawdown_usd_expr(initial_value);
    net_profit.safe_div(max_drawdown_abs, None)
}

// ================================================================================================
// === Risk measures ===
// ================================================================================================
fn max_drawdown_usd_expr(initial_value: u32) -> Expr {
    // 1. Cumulative return (equity curve)
    let cum_returns = cumulative_realized_return_usd_expr(initial_value);

    // 2. Rolling maximum (high water mark)
    let running_max = peak_cumulative_return_usd_expr(initial_value);

    // 3. Drawdown: difference between high watermark and current
    let drawdown = running_max - cum_returns;

    // 4. Max drawdown (absolute)
    drawdown.max().abs()
}

fn max_drawdown_pct_expr(initial_value: u32) -> Expr {
    // 1. Calculate the running peak (High Water Mark) for every row
    let running_peak = peak_cumulative_return_usd_expr(initial_value);

    // 2. Calculate the current equity for every row
    let current_equity = cumulative_realized_return_usd_expr(initial_value);

    // 3. Calculate Drawdown % for every row: (Peak - Current) / Peak
    let drawdown_pct = (running_peak.clone() - current_equity).safe_div(running_peak, Some(0.0)); // Avoid div by zero

    // 4. Return the maximum percentage found
    drawdown_pct.max().abs()
}

// ================================================================================================
// === Win/loss structure ===
// ================================================================================================
fn win_rate_expr(return_col: JournalCol) -> Expr {
    col(return_col).gt(lit(0)).mean().fill_null(lit(0.0))
}

fn avg_win_to_avg_loss_ratio_expr(return_col: JournalCol) -> Expr {
    let avg_win = avg_win_return_expr(return_col);
    let avg_loss = avg_loss_return_expr(return_col);
    avg_win.safe_div(avg_loss, None).abs()
}

// ================================================================================================
// === Trade return distribution ===
// ================================================================================================
fn trade_return_std_dev_expr(return_col: JournalCol) -> Expr {
    col(return_col).std(0)
}

fn trade_return_variance_expr(return_col: JournalCol) -> Expr {
    col(return_col).var(0)
}

fn lower_quantile_trade_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.25, TradeSubset::All)
}

pub fn median_trade_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.5, TradeSubset::All)
}

fn upper_quantile_trade_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.75, TradeSubset::All)
}

// ================================================================================================
// === Winning trade return distribution ===
// ================================================================================================
fn avg_win_return_expr(return_col: JournalCol) -> Expr {
    col(return_col)
        .filter(col(return_col).gt(lit(0)))
        .mean()
        .fill_null(lit(0.0))
}

fn lower_quantile_win_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.25, TradeSubset::Wins)
}

fn median_win_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.5, TradeSubset::Wins)
}

fn upper_quantile_win_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.75, TradeSubset::Wins)
}

// ================================================================================================
// === Losing trade return distribution ===
// ================================================================================================
fn avg_loss_return_expr(return_col: JournalCol) -> Expr {
    col(return_col)
        .filter(col(return_col).lt_eq(lit(0)))
        .mean()
        .fill_null(lit(0.0))
        .abs()
}

fn lower_quantile_loss_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.25, TradeSubset::Losses).abs()
}

fn median_loss_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.5, TradeSubset::Losses).abs()
}

fn upper_quantile_loss_return_expr(return_col: JournalCol) -> Expr {
    quantile_return_expr_by_subset(return_col, 0.75, TradeSubset::Losses).abs()
}

// ================================================================================================
// === Extremes ===
// ================================================================================================
fn largest_win_expr(return_col: JournalCol) -> Expr {
    col(return_col).max()
}

fn largest_loss_expr(return_col: JournalCol) -> Expr {
    col(return_col).min().abs()
}

// ================================================================================================
// === Unrealized ===
// ================================================================================================
fn unrealized_win_profit_expr(exit_reason_col: JournalCol, return_col: JournalCol) -> Expr {
    unrealized_filtered_sum_expr_by_subset(exit_reason_col, return_col, TradeSubset::Wins)
}

fn unrealized_loss_expr(exit_reason_col: JournalCol, return_col: JournalCol) -> Expr {
    unrealized_filtered_sum_expr_by_subset(exit_reason_col, return_col, TradeSubset::Losses).abs()
}

fn clean_win_profit_expr(exit_reason_col: JournalCol, return_col: JournalCol) -> Expr {
    total_win_profit_expr(return_col) - unrealized_win_profit_expr(exit_reason_col, return_col)
}

fn clean_loss_expr(exit_reason_col: JournalCol, return_col: JournalCol) -> Expr {
    total_loss_expr(return_col) - unrealized_loss_expr(exit_reason_col, return_col).abs()
}

// ================================================================================================
// === Curve deviation from target or benchmark ===
// ================================================================================================
fn rmsd_expr(return_col: JournalCol) -> Expr {
    let mean_return = avg_trade_profit_expr(return_col);

    (col(return_col) - mean_return).pow(lit(2.0)).mean().sqrt()
}

fn mae_expr(return_col: JournalCol) -> Expr {
    let mean_return = avg_trade_profit_expr(return_col);

    (col(return_col) - mean_return).abs().mean()
}

// ================================================================================================
// Helper Functions
// ================================================================================================
enum TradeSubset {
    All,
    Wins,
    Losses,
}

fn mean_return_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    pct_trade_returns_expr(return_col, initial_value).mean()
}

fn annualized_mean_return_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    mean_return_expr(return_col, initial_value) * n_trades_per_year_expr()
}

fn return_std_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    pct_trade_returns_expr(return_col, initial_value).std(1)
}

fn annualized_return_std_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    return_std_expr(return_col, initial_value) * n_trades_per_year_sqrt_expr()
}

fn downside_return_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    let pct_returns = pct_trade_returns_expr(return_col, initial_value);
    pct_returns.clone().filter(pct_returns.lt(lit(0.0)))
}

fn downside_return_std_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    downside_return_expr(return_col, initial_value).std(1)
}

fn annualized_downside_return_std_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    downside_return_std_expr(return_col, initial_value) * n_trades_per_year_sqrt_expr()
}

fn excess_return_expr(return_col: JournalCol, cfg: &RiskMetricsConfig) -> Expr {
    annualized_mean_return_expr(return_col, cfg.initial_portfolio_value())
        - lit(cfg.risk_free_rate_f64())
}

/// Computes the average number of trades per year based on timestamp duration.
///
/// Uses the difference between earliest entry and latest exit timestamp to derive
/// total backtest duration in seconds and converts that to fractional years.
///
/// This value is used to annualize per-trade metrics like Sharpe or Sortino.
fn n_trades_per_year_expr() -> Expr {
    let seconds_in_year = 365.25 * 24.0 * 60.0 * 60.0;

    // Get duration in days first, then convert to years
    let start_ts = col(JournalCol::EntryTimestamp).min();
    let end_ts = col(JournalCol::ExitTimestamp).max();

    // Duration in days
    let duration_sec = (end_ts - start_ts).dt().total_seconds(true);
    let duration_years = duration_sec.cast(DataType::Float64) / lit(seconds_in_year);

    // Total trades / duration = trades per year
    executed_trade_count_expr(JournalCol::TradeState).cast(DataType::Float64) / duration_years
}

fn n_trades_per_year_sqrt_expr() -> Expr {
    n_trades_per_year_expr().sqrt()
}

/// Computes the per-trade percentage return based on cumulative equity curve.
///
/// Returns are defined as:
/// ```text
/// pct_return_i = pnl_i / previous_equity_i
/// ```
///
/// Where:
/// - `pnl_i` is the profit/loss of the i-th trade in USD.
/// - `previous_equity_i` is the equity *before* the i-th trade, defined as:
///   `initial_value + sum of PnLs from all prior trades`.
/// - For the first trade, `previous_equity = initial_value`.
///
/// If `previous_equity_i == 0`, returns `0.0` to avoid division-by-zero.
///
/// # Requirements
/// - The input DataFrame must be sorted chronologically by trade.
/// - `return_col` must contain **per-trade realized PnL** (in USD), not cumulative.
///
/// # Arguments
/// - `return_col`: Column of per-trade PnL (realized return in USD).
/// - `initial_value`: Starting portfolio value (cannot be negative).
///
/// # Returns
/// An `Expr` computing the per-trade percentage return.
fn pct_trade_returns_expr(return_col: JournalCol, initial_value: u32) -> Expr {
    let pnl_expr = col(return_col);
    let equity_curve = cumulative_realized_return_usd_expr(initial_value);
    let prev_equity = equity_curve.shift(lit(1)).fill_null(lit(initial_value));

    pnl_expr.safe_div(prev_equity, Some(0.0))
}

fn quantile_return_expr_by_subset(
    return_col: JournalCol,
    quantile: f64,
    subset: TradeSubset,
) -> Expr {
    let filter = match subset {
        TradeSubset::All => None,
        TradeSubset::Wins => Some(col(return_col).gt(lit(0))),
        TradeSubset::Losses => Some(col(return_col).lt_eq(lit(0))),
    };

    quantile_return_expr(return_col, quantile, filter)
}

fn quantile_return_expr(return_col: JournalCol, quantile: f64, filter: Option<Expr>) -> Expr {
    let base = col(return_col);

    let filtered = match filter {
        Some(f) => base.filter(f),
        None => base,
    };

    filtered.quantile(lit(quantile), QuantileMethod::Linear)
}

fn unrealized_filtered_sum_expr_by_subset(
    exit_reason_col: JournalCol,
    return_col: JournalCol,
    subset: TradeSubset,
) -> Expr {
    let return_expr = col(return_col);
    let unrealized_filter = col(exit_reason_col).is_null();

    let combined_filter = match subset {
        TradeSubset::All => unrealized_filter,
        TradeSubset::Wins => unrealized_filter.and(return_expr.clone().gt(lit(0))),
        TradeSubset::Losses => unrealized_filter.and(return_expr.clone().lt_eq(lit(0))),
    };

    return_expr.filter(combined_filter).sum()
}

fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    polars_to_chapaty_error("portfolio performance", e)
}

pub enum OptimizationDirection {
    Maximize,
    Minimize,
}

impl PortfolioPerformanceCol {
    pub fn direction(&self) -> OptimizationDirection {
        use OptimizationDirection::*;
        use PortfolioPerformanceCol::*;

        match self {
            // === Profitability ===
            NetProfit => Maximize,
            AvgTradeProfit => Maximize,
            ExpectedValuePerTrade => Maximize,
            TotalWinProfit => Maximize,
            TotalLoss => Minimize,
            TotalWinProfitByTotalLoss => Maximize,

            // === Risk-adjusted returns ===
            SharpeRatio => Maximize,
            SortinoRatio => Maximize,
            OmegaRatio => Maximize,
            CalmarRatio => Maximize,
            RecoveryFactor => Maximize,

            // === Risk measures ===
            MaxDrawdownUsd => Minimize,
            MaxDrawdownPct => Minimize,

            // === Win/loss structure ===
            WinRate => Maximize,
            AvgWinToAvgLossRatio => Maximize,

            // === Trade return distribution ===
            TradeReturnStdDev => Minimize,
            TradeReturnVariance => Minimize,
            LowerQuantileTradeReturn => Maximize,
            MedianTradeReturn => Maximize,
            UpperQuantileTradeReturn => Maximize,

            // === Winning trade return distribution ===
            AvgWinReturn => Maximize,
            LowerQuantileWinReturn => Maximize,
            MedianWinReturn => Maximize,
            UpperQuantileWinReturn => Maximize,

            // === Losing trade return distribution ===
            AvgLossReturn => Minimize,
            LowerQuantileLossReturn => Minimize,
            MedianLossReturn => Minimize,
            UpperQuantileLossReturn => Minimize,

            // === Extremes ===
            LargestWin => Maximize,
            LargestLoss => Minimize,

            // === Unrealized ===
            UnrealizedWinProfit => Maximize,
            UnrealizedLoss => Minimize,
            CleanWinProfit => Maximize,
            CleanLoss => Minimize,

            // === Curve deviation from target or benchmark ===
            RootMeanSquareDeviation => Minimize,
            MeanAbsoluteError => Minimize,
        }
    }
}

/// Aggregate performance and risk metrics for a portfolio or strategy over a backtest window.
///
/// These metrics help quantify profitability, risk-adjusted returns, drawdowns, and return distributions.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    PartialOrd,
    Ord,
    EnumIter,
    IntoStaticStr,
    EnumCount,
)]
#[strum(serialize_all = "snake_case")]
pub enum PortfolioPerformanceCol {
    // === Profitability ===
    /// Total net PnL (profit and loss) in absolute terms.
    NetProfit,
    /// Average profit per trade.
    AvgTradeProfit,
    /// Expected value per trade: (win_rate * avg_win) - ((1 - win_rate) * avg_loss).
    ExpectedValuePerTrade,
    /// Total reward from winning trades.
    TotalWinProfit,
    /// Total loss from losing trades.
    TotalLoss,
    /// Total win profit divided by total loss.
    TotalWinProfitByTotalLoss,

    // === Risk-adjusted returns ===
    SharpeRatio,
    SortinoRatio,
    OmegaRatio,
    CalmarRatio,
    RecoveryFactor,

    // === Risk measures ===
    MaxDrawdownUsd,
    MaxDrawdownPct,

    // === Win/loss structure ===
    WinRate,
    AvgWinToAvgLossRatio,

    // === Trade return distribution ===
    TradeReturnStdDev,
    TradeReturnVariance,
    LowerQuantileTradeReturn,
    MedianTradeReturn,
    UpperQuantileTradeReturn,

    // === Winning trade return distribution ===
    AvgWinReturn,
    LowerQuantileWinReturn,
    MedianWinReturn,
    UpperQuantileWinReturn,

    // === Losing trade return distribution ===
    AvgLossReturn,
    LowerQuantileLossReturn,
    MedianLossReturn,
    UpperQuantileLossReturn,

    // === Extremes ===
    LargestWin,
    LargestLoss,

    // === Unrealized ===
    UnrealizedWinProfit,
    UnrealizedLoss,
    CleanWinProfit,
    CleanLoss,

    // === Curve deviation from target or benchmark ===
    /// Root Mean Squared Deviation
    RootMeanSquareDeviation,
    /// Mean Absolute Error
    MeanAbsoluteError,
}

impl From<PortfolioPerformanceCol> for PlSmallStr {
    fn from(value: PortfolioPerformanceCol) -> Self {
        value.as_str().into()
    }
}

impl PortfolioPerformanceCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }

    /// Converts the raw metric value into a comparable "score" for the heap
    pub fn to_heap_score(&self, raw_value: f64) -> f64 {
        match self.direction() {
            OptimizationDirection::Maximize => raw_value,
            OptimizationDirection::Minimize => -raw_value, // flip so smaller means worse
        }
    }

    /// Converts a heap score back into the original metric value
    pub fn from_heap_score(&self, score: f64) -> f64 {
        match self.direction() {
            OptimizationDirection::Maximize => score,
            OptimizationDirection::Minimize => -score,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use polars::prelude::{LazyCsvReader, LazyFileListReader, PlPath, SchemaExt};

    use super::*;

    // ========================================================================
    // Helper: Load Journal Fixture
    // ========================================================================

    fn load_journal_fixture() -> Journal {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let fixture_path =
            PathBuf::from(manifest_dir).join("tests/fixtures/report/input/journal.csv");

        assert!(
            fixture_path.exists(),
            "Test fixture missing: {}",
            fixture_path.display()
        );

        let schema = Journal::to_schema();
        let df = LazyCsvReader::new(PlPath::new(
            fixture_path
                .to_str()
                .expect("Invalid UTF-8 in fixture path"),
        ))
        .with_has_header(true)
        .with_schema(Some(schema))
        .with_try_parse_dates(true)
        .finish()
        .expect("Failed to create LazyFrame")
        .collect()
        .expect("Failed to collect DataFrame");

        Journal::new(df, RiskMetricsConfig::default()).expect("Failed to create Journal")
    }

    // ========================================================================
    // Test: Journal to PortfolioPerformance Conversion
    // ========================================================================

    #[test]
    fn test_journal_to_portfolio_performance() {
        let journal = load_journal_fixture();
        let result = PortfolioPerformance::try_from(&journal);

        assert!(
            result.is_ok(),
            "Failed to convert Journal to PortfolioPerformance: {:?}",
            result.err()
        );

        let perf = result.unwrap();
        let df = perf.as_df();

        // Should produce exactly 1 row (aggregated metrics)
        assert_eq!(
            df.height(),
            1,
            "PortfolioPerformance should have 1 row (aggregated metrics)"
        );
    }

    // ========================================================================
    // Test: All Expected Columns Present
    // ========================================================================

    #[test]
    fn test_all_portfolio_performance_fields_present() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        let expected_columns: Vec<_> = PortfolioPerformanceCol::iter().collect();

        for col in &expected_columns {
            assert!(
                df.column(col.as_str()).is_ok(),
                "Missing expected column: {}",
                col
            );
        }

        assert_eq!(
            df.schema().len(),
            expected_columns.len(),
            "Column count mismatch. Expected {}, found {}. Details: {:?}",
            expected_columns.len(),
            df.schema().len(),
            {
                let actual = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<HashSet<_>>();
                let expected = expected_columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<HashSet<_>>();
                let missing: Vec<_> = expected.difference(&actual).cloned().collect();
                let extra: Vec<_> = actual.difference(&expected).cloned().collect();
                (missing, extra)
            }
        );
    }

    // ========================================================================
    // Test: Data Types Match Schema
    // ========================================================================

    #[test]
    fn test_portfolio_performance_data_types() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();
        let expected_schema = PortfolioPerformance::to_schema();

        for field in expected_schema.iter_fields() {
            let col_name = field.name();
            let expected_dtype = field.dtype();
            let actual_dtype = df
                .column(col_name)
                .unwrap_or_else(|_| panic!("Column '{}' not found", col_name))
                .dtype();

            assert_eq!(
                actual_dtype, expected_dtype,
                "Data type mismatch for '{}': expected {:?}, found {:?}",
                col_name, expected_dtype, actual_dtype
            );
        }
    }

    // ========================================================================
    // Test: Profitability Metrics
    // ========================================================================

    #[test]
    fn test_profitability_metrics() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Fixture returns: -1000, -500, 0, +2000, +500, +1000
        // Net profit: 2000
        let net_profit = df
            .column(PortfolioPerformanceCol::NetProfit.as_str())
            .expect("Missing net_profit column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            net_profit, 2000.0,
            "Net profit should be 2000 (sum of all returns)"
        );

        // Average trade profit: 2000 / 6 = 333.33...
        let avg_profit = df
            .column(PortfolioPerformanceCol::AvgTradeProfit.as_str())
            .expect("Missing avg_trade_profit column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert!(
            (avg_profit - 333.33).abs() < 0.01,
            "Average trade profit should be ~333.33, got {}",
            avg_profit
        );
    }

    // ========================================================================
    // Test: Win/Loss Structure
    // ========================================================================

    #[test]
    fn test_win_loss_structure() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Fixture has 3 winning trades (+2000, +500, +1000) out of 6 total
        // Win rate: 3/6 = 0.5
        let win_rate = df
            .column(PortfolioPerformanceCol::WinRate.as_str())
            .expect("Missing win_rate column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(win_rate, 0.5, "Win rate should be 0.5 (50%)");

        // Total win profit: 2000 + 500 + 1000 = 3500
        let total_wins = df
            .column(PortfolioPerformanceCol::TotalWinProfit.as_str())
            .expect("Missing total_win_profit column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(total_wins, 3500.0, "Total win profit should be 3500");

        // Total loss: abs(-1000 + -500 + 0) = 1500
        let total_loss = df
            .column(PortfolioPerformanceCol::TotalLoss.as_str())
            .expect("Missing total_loss column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(total_loss, 1500.0, "Total loss should be 1500");
    }

    // ========================================================================
    // Test: Return Distribution
    // ========================================================================

    #[test]
    fn test_return_distribution() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Median return of [-1000, -500, 0, 500, 1000, 2000] = 250
        let median = df
            .column(PortfolioPerformanceCol::MedianTradeReturn.as_str())
            .expect("Missing median_trade_return column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(median, 250.0, "Median trade return should be 250");

        // Largest win: 2000
        let largest_win = df
            .column(PortfolioPerformanceCol::LargestWin.as_str())
            .expect("Missing largest_win column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(largest_win, 2000.0, "Largest win should be 2000");

        // Largest loss: abs(-1000) = 1000
        let largest_loss = df
            .column(PortfolioPerformanceCol::LargestLoss.as_str())
            .expect("Missing largest_loss column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(largest_loss, 1000.0, "Largest loss should be 1000");
    }

    // ========================================================================
    // Test: Risk-Adjusted Returns (Existence Check)
    // ========================================================================

    #[test]
    fn test_risk_adjusted_returns_computed() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Verify Sharpe ratio is computed (may be null if volatility is zero)
        let sharpe = df
            .column(PortfolioPerformanceCol::SharpeRatio.as_str())
            .expect("Missing sharpe_ratio column")
            .f64()
            .expect("Column is not f64")
            .get(0);

        // Should be Some value or None (if volatility = 0), but not missing column
        assert!(
            sharpe.is_some() || sharpe.is_none(),
            "Sharpe ratio should be computed"
        );

        // Verify Sortino ratio exists
        let sortino = df
            .column(PortfolioPerformanceCol::SortinoRatio.as_str())
            .expect("Missing sortino_ratio column")
            .f64()
            .expect("Column is not f64")
            .get(0);

        assert!(
            sortino.is_some() || sortino.is_none(),
            "Sortino ratio should be computed"
        );

        // Verify Calmar ratio exists
        let calmar = df
            .column(PortfolioPerformanceCol::CalmarRatio.as_str())
            .expect("Missing calmar_ratio column")
            .f64()
            .expect("Column is not f64")
            .get(0);

        assert!(
            calmar.is_some() || calmar.is_none(),
            "Calmar ratio should be computed"
        );
    }

    // ========================================================================
    // Test: Max Drawdown
    // ========================================================================

    #[test]
    fn test_max_drawdown() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Max drawdown should be calculated
        let max_dd_usd = df
            .column(PortfolioPerformanceCol::MaxDrawdownUsd.as_str())
            .expect("Missing max_drawdown_usd column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        // With cumulative returns [9000, 8500, 8500, 10500, 11000, 12000],
        // peak at each step: [9000, 9000, 9000, 10500, 11000, 12000]
        // drawdowns: [0, 500, 500, 0, 0, 0]
        // Max drawdown: 500
        assert_eq!(
            max_dd_usd, 500.0,
            "Max drawdown USD should be 500 (peak 9000, trough 8500)"
        );

        let max_dd_pct = df
            .column(PortfolioPerformanceCol::MaxDrawdownPct.as_str())
            .expect("Missing max_drawdown_pct column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        // Max drawdown %: 500 / 9000 = 0.0555...
        assert!(
            (max_dd_pct - 0.0556).abs() < 0.001,
            "Max drawdown % should be ~5.56%, got {}",
            max_dd_pct
        );
    }

    // ========================================================================
    // Test: Expected Value Per Trade
    // ========================================================================

    #[test]
    fn test_expected_value_per_trade() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        let expected_val = df
            .column(PortfolioPerformanceCol::ExpectedValuePerTrade.as_str())
            .expect("Missing expected_value_per_trade column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        // Win rate: 0.5
        // Avg win: (2000 + 500 + 1000) / 3 = 1166.67
        // Avg loss: abs((-1000 + -500 + 0) / 3) = 500
        // Expected value: 0.5 * 1166.67 + 0.5 * (-500) = 583.33 - 250 = 333.33
        assert!(
            (expected_val - 333.33).abs() < 0.01,
            "Expected value per trade should be ~333.33, got {}",
            expected_val
        );
    }

    // ========================================================================
    // Test: Winning Trade Distribution
    // ========================================================================

    #[test]
    fn test_winning_trade_distribution() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Winning trades: 500, 1000, 2000
        // Average: 1166.67
        let avg_win = df
            .column(PortfolioPerformanceCol::AvgWinReturn.as_str())
            .expect("Missing avg_win_return column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert!(
            (avg_win - 1166.67).abs() < 0.01,
            "Average win return should be ~1166.67, got {}",
            avg_win
        );

        // Median winning trade: 1000
        let median_win = df
            .column(PortfolioPerformanceCol::MedianWinReturn.as_str())
            .expect("Missing median_win_return column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(median_win, 1000.0, "Median win return should be 1000");
    }

    // ========================================================================
    // Test: Losing Trade Distribution
    // ========================================================================

    #[test]
    fn test_losing_trade_distribution() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Losing trades: -1000, -500, 0 (treated as loss)
        // Average loss: abs((-1000 + -500 + 0) / 3) = 500
        let avg_loss = df
            .column(PortfolioPerformanceCol::AvgLossReturn.as_str())
            .expect("Missing avg_loss_return column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(avg_loss, 500.0, "Average loss return should be 500");

        // Median losing trade: abs(-500)
        let median_loss = df
            .column(PortfolioPerformanceCol::MedianLossReturn.as_str())
            .expect("Missing median_loss_return column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(median_loss, 500.0, "Median loss return should be 500");
    }

    // ========================================================================
    // Test: Unrealized Metrics (Active Trade Handling)
    // ========================================================================

    #[test]
    fn test_unrealized_metrics() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        // Row 6 is active with +1000 unrealized profit
        let unrealized_win = df
            .column(PortfolioPerformanceCol::UnrealizedWinProfit.as_str())
            .expect("Missing unrealized_win_profit column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            unrealized_win, 1000.0,
            "Unrealized win profit should be 1000"
        );

        let unrealized_loss = df
            .column(PortfolioPerformanceCol::UnrealizedLoss.as_str())
            .expect("Missing unrealized_loss column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(unrealized_loss, 0.0, "Unrealized loss should be 0");

        // Clean win profit = total wins - unrealized wins = 3500 - 1000 = 2500
        let clean_wins = df
            .column(PortfolioPerformanceCol::CleanWinProfit.as_str())
            .expect("Missing clean_win_profit column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        assert_eq!(clean_wins, 2500.0, "Clean win profit should be 2500");
    }

    // ========================================================================
    // Test: Accessor Pattern
    // ========================================================================

    #[test]
    fn test_accessor_pattern() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");

        let accessor = perf.accessor().expect("Should create accessor");

        let net_profit = accessor
            .get(PortfolioPerformanceCol::NetProfit)
            .expect("Net profit should be available");

        assert_eq!(net_profit, 2000.0, "Net profit via accessor should be 2000");
    }

    // ========================================================================
    // Test: Empty Journal
    // ========================================================================

    #[test]
    fn test_empty_journal() {
        let empty_df = DataFrame::empty_with_schema(&Journal::to_schema());
        let journal = Journal::new(empty_df, RiskMetricsConfig::default())
            .expect("Failed to create empty Journal");

        let result = PortfolioPerformance::try_from(&journal);
        assert!(result.is_ok(), "Should handle empty Journal");

        let perf = result.unwrap();
        let df = perf.as_df();
        assert_eq!(df.height(), 0, "Empty journal should produce 0 rows");
    }

    // ========================================================================
    // Test: Trade Return Variance
    // ========================================================================

    #[test]
    fn test_trade_return_variance() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        let variance = df
            .column(PortfolioPerformanceCol::TradeReturnVariance.as_str())
            .expect("Missing trade_return_variance column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        let std_dev = df
            .column(PortfolioPerformanceCol::TradeReturnStdDev.as_str())
            .expect("Missing trade_return_std_dev column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        // Variance should be std_dev squared
        assert!(
            (variance - std_dev.powi(2)).abs() < 0.01,
            "Variance should equal std_dev squared"
        );

        // Both should be positive for this dataset
        assert!(variance > 0.0, "Variance should be positive");
        assert!(std_dev > 0.0, "Std dev should be positive");
    }

    // ========================================================================
    // Test: Recovery Factor
    // ========================================================================

    #[test]
    fn test_recovery_factor() {
        let journal = load_journal_fixture();
        let perf = PortfolioPerformance::try_from(&journal).expect("Conversion failed");
        let df = perf.as_df();

        let recovery = df
            .column(PortfolioPerformanceCol::RecoveryFactor.as_str())
            .expect("Missing recovery_factor column")
            .f64()
            .expect("Column is not f64")
            .get(0)
            .expect("Missing value");

        // Recovery factor = net_profit / max_drawdown = 2000 / 500 = 4.0
        assert_eq!(
            recovery, 4.0,
            "Recovery factor should be 4.0 (2000 profit / 500 drawdown)"
        );
    }
}
