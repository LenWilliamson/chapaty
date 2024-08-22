# Design Decisions and Considerations

## Introduction

This document outlines the key design decisions made during the development of `chapaty`. It aims to provide a comprehensive explanation of these choices, especially in handling bugs or unexpected scenarios. This reference ensures that future development is informed by past decisions and maintains a clear rationale for each design choice.

## Handling Bug Fixes

To ensure traceability and clarity when addressing bugs, follow these steps:

1. **Open an Issue:** 
   - **Create a Detailed Report:** Before making any changes, open an issue in the tracking system.
   - **Include the Following Information:**
     - **Context:** Describe the circumstances under which the bug occurs.
     - **Problem:** Clearly state the issue and its impact on the system.
     - **Reproducibility:** Provide detailed steps, parameters, and data required to replicate the issue.

2. **Fix the Bug:** 
   - **Implement a Solution:** Apply a fix for the identified problem.
   - **Verify Functionality:** Ensure that the fix resolves the issue without introducing new problems by running all existing tests.

3. **Add an Integration Test:** 
   - **Create a Test Case:** Develop a new integration test that verifies the bug has been resolved.
   - **Ensure No Recurrence:** Confirm that the issue does not reappear with the applied fix.

4. **Update Documentation:** 
   - **Document the Fix:** Update the relevant sections of the codebase documentation to reflect the changes made.
   - **Link to the Test:** Include a reference to the new integration test and explain the nature of the changes.

5. **Commit Message Format:** 
   - **Follow This Format:** 
     ```
     #issue-ID Bugfix: <description>
     ```
   - **Description:** Provide a concise summary of the bug or the fix. Use the issue title or a relevant summary for clarity.

6. **Close the Issue:** Once the fix is verified and integrated, close the issue in the tracking system.

**Example:**

If a bug causes a "No Entry" trade to be incorrectly handled as a "report_with_trade()" in the "News Counter NFP" strategy for the "6E JUN24" contract on "2011-12-01 13:30", the commit message should be:

```
#issue-ID Bugfix: Corrected handling of No Entry trades in News Counter NFP strategy for 6E JUN24 on 2011-12-01
```

This structured approach helps maintain clear tracking of issues and resolutions, facilitating easier code reviews and ongoing maintenance.

---

**Table of Contents:**

1. [Design Decision 1: Curating Data](#design-decision-1-curating-data)  
   1.1 [Context](#context)  
   1.2 [Problem](#problem)  
   1.3 [Solution](#solution)  
   1.4 [Rationale](#rationale)  
   1.5 [Considerations](#considerations)

2. [Design Decision 2: Handling Missing Data Points in OHLC Data](#design-decision-2-handling-missing-data-points-in-ohlc-data)  
   2.1 [Context](#context-1)  
   2.2 [Problem](#problem-1)  
   2.3 [Solution](#solution-1)  
   2.4 [Rationale](#rationale-1)  
   2.5 [Considerations](#considerations-1)

3. [Design Decision 3: Handling No Entry Trades](#design-decision-3-handling-no-entry-trades)  
   3.1 [Context](#context-2)  
   3.2 [Problem](#problem-2)  
   3.3 [Solution](#solution-2)  
   3.4 [Rationale](#rationale-2)  
   3.5 [Considerations](#considerations-2)

4. [Design Decision 4: Handling Missing News Candle in OHLC Data](#design-decision-4-handling-missing-news-candle-in-ohlc-data)  
   4.1 [Context](#context-3)  
   4.2 [Problem](#problem-3)  
   4.3 [Solution](#solution-3)  
   4.4 [Rationale](#rationale-3)  
   4.5 [Considerations](#considerations-3)

---

## Design Decision 1: Curating Data

### Context

When working with floating-point numbers, small rounding errors can accumulate, leading to inaccuracies in results in the profit and loss (PnL) reports. For instance, consider the EUR/USD Futures contract traded on the CME Globex platform, denoted as `6E`. According to the [product sheet](https://www.cmegroup.com/markets/fx/g10/euro-fx.contractSpecs.html), the contract specifications are:

- **Tick Size**: 0.000050 per Euro increment
- **Tick Value**: $6.25

This tick size is the smallest unit of price movement for the contract, meaning the price can only change in increments of 0.000050 EUR. Each increment or tick represents a change in contract value of $6.25.

Different trading symbols have different rules for tick sizes and values, necessitating careful data curation after performing floating-point calculations to ensure accuracy.

### Problem

In the resulting `pnl.csv` files, data can exhibit arbitrary floating-point precision when rounded mathematically correct to a certain number of decimal places. However, this approach can produce incorrect data when tick steps are not moving in `1`s, as in the case of the 6E contract. For example, the data may appear as:

| Date       | Strategy | Market | TradeDirection | Entry  | TakeProfit | StopLoss |
|------------|----------|--------|----------------|--------|------------|----------|
| 2024-01-05 | USANFP   | 6e     | Long           | 1.097  | 1.09895    | 1.09603  |
| 2024-02-02 | USANFP   | 6e     | Long           | 1.08815| 1.09389    | 1.08528  |
| 2024-03-08 | USANFP   | 6e     | Short          | 1.09925| 1.09858    | 1.09959  |
| 2024-04-05 | USANFP   | 6e     | Long           | 1.08415| 1.08736    | 1.08255  |
| 2024-05-03 | USANFP   | 6e     | Short          | 1.08185| 1.07768    | 1.08393  |


Instead of:


| Date       | Strategy | Market | TradeDirection | Entry  | TakeProfit | StopLoss |
|------------|----------|--------|----------------|--------|------------|----------|
| 2024-01-05 | USANFP   | 6e     | Long           | 1.097  | 1.09895    | 1.09605  |
| 2024-02-02 | USANFP   | 6e     | Long           | 1.08815| 1.0939     | 1.0853   |
| 2024-03-08 | USANFP   | 6e     | Short          | 1.09925| 1.0986     | 1.0996   |
| 2024-04-05 | USANFP   | 6e     | Long           | 1.08415| 1.08735    | 1.08255  |
| 2024-05-03 | USANFP   | 6e     | Short          | 1.08185| 1.0777     | 1.08395  |


### Solution

To address the issue, the software, `chapaty`, curates precision after the strategy returns a `Trade` struct:

```rust
let trade = self.strategy.get_trade(&request).curate_precision(&self.market);
```

Here, the floating-point values for `entry_price`, `stop_loss`, and `take_profit` are curated respectively. For each market, the appropriate rounding function is applied:

```rust
pub fn round_float_to_correct_decimal_place(&self, f: f64) -> f64 {
    match self {
        MarketKind::BtcUsdt => f.round_to_n_decimal_places(2),
        MarketKind::EurUsdFuture => f.round_nth_decimal_place_to_nearest_5_or_0(5),
        MarketKind::GbpUsdFuture => f.round_to_n_decimal_places(4),
        // other markets...
    }
}
```

**Example of rounding for the 6E contract**:

```rust
fn round_nth_decimal_place_to_nearest_5_or_0(self, n: i32) -> Self {
    let x = 10.0_f64.powi(n);
    let shifted = self * x;
    let rounded = shifted.round();
    let last_digit = (rounded % 10.0) as i32;
    let adjustment = match last_digit {
        1 | 2 => -last_digit,         // Round down to 0
        3 | 4 => 5 - last_digit,      // Round up to 5
        6 | 7 => 5 - last_digit,      // Round down to 5
        8 | 9 => 10 - last_digit,     // Round up to 0
        _ => 0,                       // Already 0 or 5
    };
    ((rounded + adjustment as f64) / x).round_to_n_decimal_places(5)
}
```

In other cases, the built-in `round` function available in Rust is used.

### Rationale

Floating-point arithmetic can introduce minor inaccuracies due to how numbers are represented in binary. These inaccuracies can compound, leading to discrepancies in calculations. By curating data to match tick sizes and precision requirements specific to each market, the software ensures that results are consistent with real-world trading conditions and contract specifications. This approach provides a more accurate and reliable representation of financial data, essential for backtesting and performance evaluation.

### Considerations
Curating precision introduces a trade-off between absolute accuracy and practical usability. By rounding to the nearest tick size, the software may produce results that differ by a tick from those reported by brokers, due to inherent floating-point errors. This discrepancy is a consequence of trying to reconcile mathematical precision with practical trading constraints. However, this approach ensures that the PnL reports are clean and consistent, adhering to the tick sizes and values defined by the market. The benefit is a more understandable and usable dataset that aligns with the market's pricing structure, making it easier to interpret and act upon.

## Design Decision 2: Handling Missing Data Points in OHLC Data

### Context

In the context of backtesting, `chapaty` relies on data maps that associate required data requested by the bot with actual data from the historical database containing past traded data. Specifically, for `PreTradeDataKind::News`, the software populates the `RequiredPreTradeValuesWithData` map with data of type `OhlcCandle`. It was initially assumed that this data would be complete and perfect. However, during runtime, it was observed that certain OHLC data points were missing. For instance, in the data export of the 6EJUN24 contract, timestamps like `12:30:00 + n-Candles` for `n = 5` were missing, resulting in incomplete records.

Example:

```csv
03.04.2015 12:30:00;1,2707;1,2707;1,2707;1,2707
03.04.2015 12:31:00;1,2708;1,2708;1,2707;1,2708
03.04.2015 12:32:00;1,2708;1,2708;1,2708;1,2708
03.04.2015 12:34:00;1,2708;1,2708;1,2707;1,2708
03.04.2015 12:37:00;1,2708;1,2708;1,2708;1,2708
03.04.2015 12:38:00;1,2708;1,2708;1,2708;1,2708
```

This issue resulted in the `RequiredPreTradeValuesWithData` map containing OHLC candles with `None` values, leading to crashes when the `Bot` attempted to unwrap a `None` value.

### Problem

The problem arose when attempting to access an OHLC candle from `RequiredPreTradeValuesWithData` while implementing the `Strategy` trait for the news bot. The software would sometimes encounter a `None` field for the OHLC data type, due to missing data. The initial implementation did not account for the possibility of missing data, causing runtime errors when unwrapping these values.

Example:

```rust
pre_trade_values
    .news_candle(
        &self.news_kind,
        self.number_candles_to_wait.try_into().unwrap(),
    )
    .unwrap()
    .open
    .unwrap() // panic, as open might be None
```

### Solution

To address this issue, a fallback mechanism was implemented in the `news_candle` function. The modified function now attempts to retrieve the OHLC data from previous timestamps (`t+n-1`, `t+n-2`, etc.) if the data for time `t` is missing. This approach ensures continuity in the backtesting process and prevents crashes due to missing data.

Updated Implementation:

```rust
pub fn news_candle(&self, news_kind: &NewsKind, n: u32) -> Option<&OhlcCandle> {
    let mut offset = 0;

    while let Some(candle) = self
        .market_values
        .get(&PreTradeDataKind::News(*news_kind, n - offset))
    {
        if candle.is_valid() {
            return Some(candle);
        }
        if offset >= n {
            break;
        }
        offset += 1;
    }

    None
}
```

### Rationale

The fallback mechanism was chosen to enhance the software's robustness in real-world conditions, where data may not always be complete. By utilizing the most recent available data, the backtesting engine can continue to function, despite potential imperfections in the input data.

### Considerations

- **Accuracy**: The fallback data may not perfectly represent the missing data point, potentially affecting backtesting accuracy.
- **Monitoring**: It is crucial to log occurrences of missing data and fallback usage to monitor data quality and address any systemic issues. This can be done using the [log](https://github.com/rust-lang/log) crate, along with [env_logger](https://github.com/rust-cli/env_logger).


## Design Decision 3: Handling No Entry Trades

### Context

In the current implementation, the decision to process a trade or handle a non-entry is made by the `compute` method in the `PnLReportDataRowCalculator`:

```rust
impl PnLReportDataRowCalculator {
    pub fn compute(&self) -> PnLReportDataRow {
        let data = self.get_trade_and_pre_trade_values_with_data();
        match data.trade {
            Some(_) => self.handle_trade(data),
            None => self.handle_no_entry(data),
        }
    }
    // Additional code...
}
```

This approach was initially designed for the PPP strategy, where an entry price might not be hit within a day, leading to `None` for the entry timestamp and consequently no trade data (`data.trade`). When converting the `PnLReportDataRow` into a `DataFrame`:

```rust
impl From<PnLReportDataRow> for DataFrame {
    fn from(value: PnLReportDataRow) -> Self {
        match value.trade_pnl {
            Some(_) => value.report_with_trade(),
            None => value.report_without_trade(),
        }
    }
}
```

### Problem

The introduction of new strategies, such as news trading, which require timely entries around news events, has highlighted a flaw in this approach. In these strategies, a valid entry timestamp is expected, but not every valid entry timestamp results in a valid trade. The current implementation erroneously assumes that any valid entry timestamp signifies a valid trade, leading to potential errors when the trade does not meet validity criteria.

### Solution

To address this, we need to ensure that not only is `trade_pnl` present, but also that the trade itself is valid. This adjustment involves modifying the conversion logic to:

```rust
impl From<PnLReportDataRow> for DataFrame {
    fn from(value: PnLReportDataRow) -> Self {
        match (&value.trade_pnl, value.trade.is_valid) {
            (None, _) | (_, false) => value.report_without_trade(),
            _ => value.report_with_trade(),
        }
    }
}
```

Additionally, default values for missing take profit or stop loss are now set to the `entry_price` of the trade.

### Rationale

The previous implementation was based on the assumption that any trade with a valid entry timestamp was automatically a valid trade. This approach did not account for the evolving requirements of new trading strategies that need to differentiate between valid and invalid trades more rigorously.

### Considerations

Future updates may involve revising the program flow so that the validity of a trade is determined solely by the trading strategy itself. This would help to separate concerns and provide a cleaner API.


## Design Decision 4: Handling Missing News Candle in OHLC Data

### Context
It was initially assumed that this data would be complete and perfect. However, during runtime, it was observed that certain OHLC data points were missing, similar to [Design Decision 2: Handling Missing Data Points in OHLC Data](#design-decision-2-handling-missing-data-points-in-ohlc-data). We observed that not only certain time stamps are missing, but potentially there are gaps in the data. For example on Friday `03.04.2009` in the data export of the 6EJUN24 contract, data past 03:58 UTC time is missing, resulting in incomplete records. Therefore, we cannot calculate any pnl for that NFP news day.

Example:
```csv
03.04.2009 03:56:00;1,5266;1,5266;1,5264;1,5264
03.04.2009 03:57:00;1,5265;1,5265;1,5261;1,5265
03.04.2009 03:58:00;1,5264;1,5264;1,5264;1,5264
05.04.2009 22:01:00;1,5336;1,5338;1,5335;1,5335
05.04.2009 22:02:00;1,5337;1,5337;1,5335;1,5335
05.04.2009 22:03:00;1,5335;1,5337;1,5335;1,5337
```

This resulted in `panics` as we unwrapped `None` values at different locations in our code.

### Problem
When computing a data row for the PnL report, we first gather the trade and pre-trade data. First, pre-trade values are computed. Then we compute trade values. In case of the counter strategy for news trading, the pre trade values are the first n candles after the news came out (including the news candle itself). Because we have a gap in our data, we receive a struct `RequiredPreTradeValuesWithData` that contains a hash map with key value mappings from `News(NewsKind, u32) -> OhlcCandle`. In our case, we obtained Ohlc candles where all fields where `None` values.
```rust
News(USANFP, 0) -> OhlcCandle { open_ts: None, open: None, high: None, low: None, close: None, close_ts: None } // (news candle)
News(USANFP, 1) -> OhlcCandle { open_ts: None, open: None, high: None, low: None, close: None, close_ts: None } // (first candle after news event)
News(USANFP, 2) -> OhlcCandle { open_ts: None, open: None, high: None, low: None, close: None, close_ts: None } // (second candle after news event)
.
.
.
News(USANFP, n) -> OhlcCandle { open_ts: None, open: None, high: None, low: None, close: None, close_ts: None } // (nth canlde after news event)
```
After we computed the pre trade values, `chapaty` computes the trade values.
```rust
fn compute_trade_values(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Option<TradeValuesWithData> {
    let calculator_builder: TradeValuesCalculatorBuilder = self.into();
    calculator_builder
        .with_entry_price(self.strategy.get_entry_price(&pre_trade_values))
        .with_entry_ts(self.strategy.get_entry_ts(&pre_trade_values))
        .build_and_compute()
}
```
But calling `self.strategy.get_entry_price(&pre_trade_values)` and `self.strategy.get_entry_ts(&pre_trade_values)` causes a `panic` as we are unwrapping `None` values of our `OhlcCandle`.

### Solution
To account for gaps in the historical data, we maintain a hash map in our `RequiredPreTradeValuesWithData` that has values of type `Option<OhlcCandle>`.
```rust
#[derive(Clone)]
pub struct RequiredPreTradeValuesWithData {
    pub market_values: HashMap<PreTradeDataKind, Option<OhlcCandle>>,
    pub indicator_values: HashMap<TradingIndicatorKind, f64>,
}
```
Hence, we receive a struct `RequiredPreTradeValuesWithData` that contains a hash map with key value mappings from `News(NewsKind, u32) -> Option<OhlcCandle>>`. In our case, we now obtain 
```rust
News(USANFP, 0) -> None // (news candle)
News(USANFP, 1) -> None // (first candle after news event)
News(USANFP, 2) -> None // (second candle after news event)
.
.
.
News(USANFP, n) -> None // (nth canlde after news event)
```
To decide if we should skip a computation for the trade values, and therefore handle this trade as `handle_no_entry`, we had to modify the `Strategy` trait function
```rust
fn get_entry_ts(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> (Option<i64>, bool);
```
to return `(Option<i64>, bool)`, where thee first value is the entry timestamp (if available). The second value is a boolean indicating if the timestamp should be computed when the `Option<i64>` is `None`. This is necessary, as our news trading stratgies directly retturn an entry timestamp, as these strategies are timed around the news. Contrary to PPP or Magneto, or other stratgies, where `chapaty` determines the entry timestamp. Now, we skip the computation if there is no entry time stamp, due to gaps in data, and if for this strategy `chapaty` does not compute an entry timestamp if there is `None`, in that case the strategy sets `compute_entry_ts_if_none == false`. Because if we don't have a entry timestamp in our news strategy, then we don't have a trade, and therefore `chapaty` shouldn't compute an entry timestamp for us. It should handle that trade as a "no entry trade".

### Rationale
This solution was chosen to enhance the robustness of the backtesting framework. By introducing a fallback mechanism, the system can handle data gaps without causing runtime `panics`, allowing for a smoother and more resilient simulation process. If we have a gap during a trade that we'd like to perform, we simply output in our PnL report a row with `TradeDirection == Not Clear` and fill the rest with zeros. 

```csv
Date,Strategy,Market,TradeDirection
2009-04-03,USANFP,6e,Not Clear
```

### Considerations
- **Accuracy**: The missing trade is affecting backtesting accuracy, as we can not assess the accuracy of our strategy properly. However, we discovered that the data gaps only occured so far in three (2006, 2007 and 2009) out of 18 years and therefore we can neglect that inaccuracy for the moment. 
- **Monitoring**: It is crucial to log occurrences of missing data and fallback usage to monitor data quality and address any systemic issues. This can be done using the [log](https://github.com/rust-lang/log) crate, along with [env_logger](https://github.com/rust-cli/env_logger).

---

This revision clarifies the problem by separating the implementation details from the core issue, making it easier to understand the context and implications.
