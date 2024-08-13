# Design Decisions and Considerations

## Introduction

This document outlines the key design decisions made during the development of `chapaty`. The goal is to provide a comprehensive explanation of the choices made, particularly in addressing bugs or unexpected scenarios. This documentation serves as a reference for future development, ensuring that the rationale behind each decision is clear and that any modifications or extensions of the codebase are informed by past experiences.

**Table of Contents:**
1. [Commit Message for Bugfix](#commit-message-for-bugfix)
2. [Design Decision 1: Handling Missing Data Points in OHLC Data](#design-decision-1-handling-missing-data-points-in-ohlc-data)  
   2.1 [Context](#context)  
   2.2 [Problem](#problem)  
   2.3 [Solution](#solution)  
   2.4 [Rationale](#rationale)  
   2.5 [Considerations](#considerations)
3. [Design Decision 2: Handling No Entry Trades](#design-decision-2-handling-no-entry-trades)  
   3.1 [Context](#context-1)  
   3.2 [Problem](#problem-1)  
   3.3 [Solution](#solution-1)  
   3.4 [Rationale](#rationale-1)  
   3.5 [Considerations](#considerations-1)

---

## Commit Message for Bugfix

To ensure changes are traceable and easily understandable, commit messages for bug fixes should follow this format:

1. **Open an Issue:** Before making a commit, create an issue detailing the bug or enhancement needed.
2. **Add Integration Test:** Add an integration test that shows that this bugfix does not longer occur. 
3. **Commit Message Format:** `Bugfix: <strategy> <contract> <timestamp> <description>`

   - **Strategy:** The trading or analysis strategy affected by the bug.
   - **Contract:** The financial instrument or data set involved.
   - **Timestamp:** The date and time related to the issue.
   - **Description:** A brief summary of the bug or the fix implemented.

**Example:**

Suppose there is a bug where a "No Entry" trade is falsely handled as a "report_with_trade()" in the "News Counter NFP" strategy for the "6E JUN24" contract on "2011-12-01 13:30".

The commit message should look like this:
```
#issue-id Bugfix: News Counter NFP 6E JUN24 2011-12-01 13:30 NoEntry trade is falsely handled as report_with_trade()
```

This format helps in tracking the origin of the issue and understanding the context of the fix, making it easier to review and maintain the codebase.

## Design Decision 1: Handling Missing Data Points in OHLC Data

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


## Design Decision 2: Handling No Entry Trades

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

---

This revision clarifies the problem by separating the implementation details from the core issue, making it easier to understand the context and implications.
