# Design Decisions and Considerations

## Introduction

This document outlines the key design decisions made during the development of `chapaty`. The goal is to provide a comprehensive explanation of the choices made, particularly in addressing bugs or unexpected scenarios. This documentation serves as a reference for future development, ensuring that the rationale behind each decision is clear and that any modifications or extensions of the codebase are informed by past experiences.

**Table of Contents:**

1. [Design Decision 1: Handling Missing Data Points in OHLC Data](#design-decision-1-handling-missing-data-points-in-ohlc-data)  
   1.1 [Context](#context)  
   1.2 [Problem](#problem)  
   1.3 [Solution](#solution)  
   1.4 [Rationale](#rationale)  
   1.5 [Considerations](#considerations)

---

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

---

This document will be periodically updated to reflect new design decisions and changes in the software architecture. Contributions and suggestions for improvement are welcome.
