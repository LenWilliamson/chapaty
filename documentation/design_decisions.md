# Design Decisions and Considerations

## Introduction

This document outlines the key design decisions made during the development of `chapaty`. The goal is to provide a comprehensive explanation of why certain choices were made, especially when addressing bugs or unexpected scenarios. This documentation aims to serve as a reference for future development, ensuring that the rationale behind each decision is clear and that any modifications or extensions of the codebase are informed by past  experiences.

**Table of Contents:**
1. [Design Decision 1: Handling Missing Data Points in OHLC Data](#design-decision-1-handling-missing-data-points-in-ohlc-data)  
   2.1 [Context](#context)  
   2.2 [Problem](#problem)  
   2.3 [Solution](#solution)  
   2.4 [Rationale](#rationale)  
   2.5 [Considerations](#considerations)

---
## Design Decision 1: Handling Missing Data Points in OHLC Data

### Context

In the context of backtesting, `chapaty` relies on data maps that associate required data, requested by the bot, with the actual data from the historical data base that contains the past traded data.
- `RequriedPreTradeValues` and `RequiredPreTradeValuesWithData`
In case of the required pre trade values for the data type `PreTradeDataKind::News`, the software fills the `RequiredPreTradeValuesWithData` map with data of type `OhlcCandle`. The assumption is that the data provided would be complete and perfect, with no missing data points. However, during runtime, it was observed that certain OHLC data points are missing. In the data export of the 6EJUN24 contract we observerd that the timestamps `12:30:00 + n-Candles` for `n = 5` are missing.
```csv
03.04.2015 12:30:00;1,2707;1,2707;1,2707;1,2707
03.04.2015 12:31:00;1,2708;1,2708;1,2707;1,2708
03.04.2015 12:32:00;1,2708;1,2708;1,2708;1,2708
03.04.2015 12:34:00;1,2708;1,2708;1,2707;1,2708
03.04.2015 12:37:00;1,2708;1,2708;1,2708;1,2708
03.04.2015 12:38:00;1,2708;1,2708;1,2708;1,2708
```
Hence, in the `RequiredPreTradeValuesWithData` map we'll have inside the `market_values` map a OHLC candle with fields of type `None`. This discrepancy caused the program to crash when the `Bot` attempting to unwrap a `None` value.



### Problem

When attempting to access an OHLC candle from `RequiredPreTradeValuesWithData` while implementing the `Strategy` trait from the data map, the software would sometimes encounter a `None` field for the OHLC data type, due to the absence of data. The initial implementation unwrapped this potentially `None` value without handling the possibility of missing data, leading to runtime errors.
```rust
pre_trade_values
            .news_candle(
                &self.news_kind,
                self.number_candles_to_wait.try_into().unwrap(),
            )
            .unwrap()
            .open
            .unwrap()
```

### Solution

To address this, a fallback mechanism the function `news_candle` that the `Bot` uses to get the OHLC data will be modified in such way that, If the OHLC candle for time `t` is not present, `chapaty` needs to attempt to use data from previous timestamps, specifically `t+n-1`, `t+n-2`, etc., until a valid data point is found. This approach ensures the continuity of the backtesting process and prevents crashes due to missing data. Instead of returning the ohlc candle wihtout checking if the candle itself is valid
```rust
pub fn news_candle(&self, news_kind: &NewsKind, n: u32) -> Option<&OhlcCandle> {
    self.market_valeus.get(&PreTradeDataKind::News(*news_kind, n))
}
```
we only return a valid candle
```rust
pub fn news_candle(&self, news_kind: &NewsKind, n: u32) -> Option<&OhlcCandle> {
        let mut offset = 0;

        while let Some(candle) = self
            .market_valeus
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

This fallback mechanism was chosen to maintain the robustness of the software in real-world conditions where data may not be perfect. By using the most recent available data, the backtesting engine can continue to function, albeit with an acknowledgment of the potential imperfections in the input data.

### Considerations

- **Accuracy**: The fallback data may not perfectly represent the missing data point, potentially affecting backtesting accuracy.
- **Monitoring**: It is essential to log occurrences of missing data and fallback usage to monitor data quality and address any systemic issues. This should be done with [log](https://github.com/rust-lang/log), respectively [env_logger](https://github.com/rust-cli/env_logger).

---

This document will be periodically updated to reflect new design decisions and changes in the software architecture. Contributions and suggestions for improvement are welcome.