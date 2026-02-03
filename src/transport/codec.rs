use polars::{
    frame::DataFrame,
    prelude::{DataType, IntoLazy, LazyFrame, Schema, TimeUnit, col, df},
};
use prost_types::Timestamp;

use crate::{
    error::{ChapatyError, ChapatyResult, DataError},
    generated::chapaty::bq_exporter::v1::{
        EconomicCalendarResponse, OhlcvFutureResponse, OhlcvSpotResponse, TpoFutureResponse,
        TpoSpotResponse, TradesSpotResponse, VolumeProfileSpotResponse,
    },
    transport::schema::{
        CanonicalCol, economic_calendar_schema, ohlcv_future_schema, ohlcv_spot_schema,
        tpo_future_schema, tpo_spot_schema, trades_spot_schema, volume_profile_spot_schema,
    },
};

/// A trait for Protobuf messages that represent a batch of data
pub trait ProtoBatch {
    /// Converts this batch into a standardized Polars LazyFrame
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame>;
}

impl ProtoBatch for EconomicCalendarResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&economic_calendar_schema()),
        };

        let len = events.len();
        let mut data_sources = Vec::with_capacity(len);
        let mut categories = Vec::with_capacity(len);
        let mut event_timestamps = Vec::with_capacity(len);
        let mut news_types = Vec::with_capacity(len);
        let mut news_type_confidences = Vec::with_capacity(len);
        let mut news_type_sources = Vec::with_capacity(len);
        let mut periodicities = Vec::with_capacity(len);
        let mut news_names = Vec::with_capacity(len);
        let mut country_codes = Vec::with_capacity(len);
        let mut currency_codes = Vec::with_capacity(len);
        let mut importances = Vec::with_capacity(len);
        let mut actuals = Vec::with_capacity(len);
        let mut forecasts = Vec::with_capacity(len);
        let mut previouses = Vec::with_capacity(len);

        for event in events {
            data_sources.push(event.data_source);
            categories.push(event.category);
            event_timestamps.push(extract_timestamp(
                &event.event_timestamp,
                "event_timestamp",
            )?);
            news_types.push(event.news_type);
            news_type_confidences.push(event.news_type_confidence);
            news_type_sources.push(event.news_type_source);
            periodicities.push(event.periodicity);
            news_names.push(event.news_name);
            country_codes.push(event.country_code);
            currency_codes.push(event.currency_code);
            importances.push(event.importance as i64);
            actuals.push(event.actual);
            forecasts.push(event.forecast);
            previouses.push(event.previous);
        }

        let df = df![
            CanonicalCol::DataSource.to_string() => data_sources,
            CanonicalCol::Category.to_string() => categories,
            CanonicalCol::Timestamp.to_string() => event_timestamps,
            CanonicalCol::NewsType.to_string() => news_types,
            CanonicalCol::NewsTypeConfidence.to_string() => news_type_confidences,
            CanonicalCol::NewsTypeSource.to_string() => news_type_sources,
            CanonicalCol::Period.to_string() => periodicities,
            CanonicalCol::NewsName.to_string() => news_names,
            CanonicalCol::CountryCode.to_string() => country_codes,
            CanonicalCol::CurrencyCode.to_string() => currency_codes,
            CanonicalCol::EconomicImpact.to_string() => importances,
            CanonicalCol::Actual.to_string() => actuals,
            CanonicalCol::Forecast.to_string() => forecasts,
            CanonicalCol::Previous.to_string() => previouses,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df
            .lazy()
            .with_column(col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )));

        Ok(lf)
    }
}

impl ProtoBatch for OhlcvFutureResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&ohlcv_future_schema()),
        };

        let len = events.len();
        let mut open_timestamps = Vec::with_capacity(len);
        let mut opens = Vec::with_capacity(len);
        let mut highs = Vec::with_capacity(len);
        let mut lows = Vec::with_capacity(len);
        let mut closes = Vec::with_capacity(len);
        let mut volumes = Vec::with_capacity(len);
        let mut close_timestamps = Vec::with_capacity(len);

        for event in events {
            open_timestamps.push(extract_timestamp(&event.open_timestamp, "open_timestamp")?);
            opens.push(event.open);
            highs.push(event.high);
            lows.push(event.low);
            closes.push(event.close);
            volumes.push(event.volume);
            close_timestamps.push(extract_timestamp(
                &event.close_timestamp,
                "close_timestamp",
            )?);
        }

        let df = df![
            CanonicalCol::OpenTimestamp.to_string() => open_timestamps,
            CanonicalCol::Open.to_string() => opens,
            CanonicalCol::High.to_string() => highs,
            CanonicalCol::Low.to_string() => lows,
            CanonicalCol::Close.to_string() => closes,
            CanonicalCol::Volume.to_string() => volumes,
            CanonicalCol::Timestamp.to_string() => close_timestamps,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(CanonicalCol::OpenTimestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        Ok(lf)
    }
}

impl ProtoBatch for OhlcvSpotResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&ohlcv_spot_schema()),
        };

        let len = events.len();
        let mut open_timestamps = Vec::with_capacity(len);
        let mut opens = Vec::with_capacity(len);
        let mut highs = Vec::with_capacity(len);
        let mut lows = Vec::with_capacity(len);
        let mut closes = Vec::with_capacity(len);
        let mut volumes = Vec::with_capacity(len);
        let mut close_timestamps = Vec::with_capacity(len);
        let mut quote_asset_volumes = Vec::with_capacity(len);
        let mut number_of_trades = Vec::with_capacity(len);
        let mut taker_buy_base_asset_volumes = Vec::with_capacity(len);
        let mut taker_buy_quote_asset_volumes = Vec::with_capacity(len);

        for event in events {
            open_timestamps.push(extract_timestamp(&event.open_timestamp, "open_timestamp")?);
            opens.push(event.open);
            highs.push(event.high);
            lows.push(event.low);
            closes.push(event.close);
            volumes.push(event.volume);
            close_timestamps.push(extract_timestamp(
                &event.close_timestamp,
                "close_timestamp",
            )?);
            quote_asset_volumes.push(event.quote_asset_volume);
            number_of_trades.push(event.number_of_trades);
            taker_buy_base_asset_volumes.push(event.taker_buy_base_asset_volume);
            taker_buy_quote_asset_volumes.push(event.taker_buy_quote_asset_volume);
        }

        let df = df![
            CanonicalCol::OpenTimestamp.to_string() => open_timestamps,
            CanonicalCol::Open.to_string() => opens,
            CanonicalCol::High.to_string() => highs,
            CanonicalCol::Low.to_string() => lows,
            CanonicalCol::Close.to_string() => closes,
            CanonicalCol::Volume.to_string() => volumes,
            CanonicalCol::Timestamp.to_string() => close_timestamps,
            CanonicalCol::QuoteAssetVolume.to_string() => quote_asset_volumes,
            CanonicalCol::NumberOfTrades.to_string() => number_of_trades,
            CanonicalCol::TakerBuyBaseAssetVolume.to_string() => taker_buy_base_asset_volumes,
            CanonicalCol::TakerBuyQuoteAssetVolume.to_string() => taker_buy_quote_asset_volumes,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(CanonicalCol::OpenTimestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        Ok(lf)
    }
}

impl ProtoBatch for TradesSpotResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&trades_spot_schema()),
        };

        let len = events.len();
        let mut trade_ids = Vec::with_capacity(len);
        let mut prices = Vec::with_capacity(len);
        let mut quantities = Vec::with_capacity(len);
        let mut quote_quantities = Vec::with_capacity(len);
        let mut trade_timestamps = Vec::with_capacity(len);
        let mut is_buyer_makers = Vec::with_capacity(len);
        let mut is_best_matches = Vec::with_capacity(len);

        for event in events {
            trade_ids.push(event.trade_id);
            prices.push(event.price);
            quantities.push(event.quantity);
            quote_quantities.push(event.quote_quantity);
            trade_timestamps.push(extract_timestamp(
                &event.trade_timestamp,
                "trade_timestamp",
            )?);
            is_buyer_makers.push(event.is_buyer_maker);
            is_best_matches.push(event.is_best_match);
        }

        let df = df![
            CanonicalCol::TradeId.to_string() => trade_ids,
            CanonicalCol::Price.to_string() => prices,
            CanonicalCol::Volume.to_string() => quantities,
            CanonicalCol::QuoteAssetVolume.to_string() => quote_quantities,
            CanonicalCol::Timestamp.to_string() => trade_timestamps,
            CanonicalCol::IsBuyerMaker.to_string() => is_buyer_makers,
            CanonicalCol::IsBestMatch.to_string() => is_best_matches,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df
            .lazy()
            .with_column(col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )));

        Ok(lf)
    }
}

impl ProtoBatch for TpoFutureResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&tpo_future_schema()),
        };

        let len = events.len();
        let mut window_starts = Vec::with_capacity(len);
        let mut window_ends = Vec::with_capacity(len);
        let mut price_bin_starts = Vec::with_capacity(len);
        let mut price_bin_ends = Vec::with_capacity(len);
        let mut time_slot_counts = Vec::with_capacity(len);

        for event in events {
            window_starts.push(extract_timestamp(&event.window_start, "window_start")?);
            window_ends.push(extract_timestamp(&event.window_end, "window_end")?);
            price_bin_starts.push(event.price_bin_start);
            price_bin_ends.push(event.price_bin_end);
            time_slot_counts.push(event.time_slot_count);
        }

        let df = df![
            CanonicalCol::OpenTimestamp.to_string() => window_starts,
            CanonicalCol::Timestamp.to_string() => window_ends,
            CanonicalCol::PriceBinStart.to_string() => price_bin_starts,
            CanonicalCol::PriceBinEnd.to_string() => price_bin_ends,
            CanonicalCol::TimeSlotCount.to_string() => time_slot_counts,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(CanonicalCol::OpenTimestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        Ok(lf)
    }
}

impl ProtoBatch for TpoSpotResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&tpo_spot_schema()),
        };

        let len = events.len();
        let mut window_starts = Vec::with_capacity(len);
        let mut window_ends = Vec::with_capacity(len);
        let mut price_bin_starts = Vec::with_capacity(len);
        let mut price_bin_ends = Vec::with_capacity(len);
        let mut time_slot_counts = Vec::with_capacity(len);

        for event in events {
            window_starts.push(extract_timestamp(&event.window_start, "window_start")?);
            window_ends.push(extract_timestamp(&event.window_end, "window_end")?);
            price_bin_starts.push(event.price_bin_start);
            price_bin_ends.push(event.price_bin_end);
            time_slot_counts.push(event.time_slot_count);
        }

        let df = df![
            CanonicalCol::OpenTimestamp.to_string() => window_starts,
            CanonicalCol::Timestamp.to_string() => window_ends,
            CanonicalCol::PriceBinStart.to_string() => price_bin_starts,
            CanonicalCol::PriceBinEnd.to_string() => price_bin_ends,
            CanonicalCol::TimeSlotCount.to_string() => time_slot_counts,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(CanonicalCol::OpenTimestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        Ok(lf)
    }
}

impl ProtoBatch for VolumeProfileSpotResponse {
    fn into_lazyframe(self) -> ChapatyResult<LazyFrame> {
        let events = match self.batch {
            Some(b) => b.events,
            None => return empty_lf(&volume_profile_spot_schema()),
        };

        let len = events.len();
        let mut window_starts = Vec::with_capacity(len);
        let mut window_ends = Vec::with_capacity(len);
        let mut price_bin_starts = Vec::with_capacity(len);
        let mut price_bin_ends = Vec::with_capacity(len);
        let mut base_volumes = Vec::with_capacity(len);
        let mut taker_buy_base_volumes = Vec::with_capacity(len);
        let mut taker_sell_base_volumes = Vec::with_capacity(len);
        let mut quote_volumes = Vec::with_capacity(len);
        let mut taker_buy_quote_volumes = Vec::with_capacity(len);
        let mut taker_sell_quote_volumes = Vec::with_capacity(len);
        let mut number_of_trades = Vec::with_capacity(len);
        let mut number_of_buy_trades = Vec::with_capacity(len);
        let mut number_of_sell_trades = Vec::with_capacity(len);

        for event in events {
            window_starts.push(extract_timestamp(&event.window_start, "window_start")?);
            window_ends.push(extract_timestamp(&event.window_end, "window_end")?);
            price_bin_starts.push(event.price_bin_start);
            price_bin_ends.push(event.price_bin_end);
            base_volumes.push(event.base_volume);
            taker_buy_base_volumes.push(event.taker_buy_base_volume);
            taker_sell_base_volumes.push(event.taker_sell_base_volume);
            quote_volumes.push(event.quote_volume);
            taker_buy_quote_volumes.push(event.taker_buy_quote_volume);
            taker_sell_quote_volumes.push(event.taker_sell_quote_volume);
            number_of_trades.push(event.number_of_trades);
            number_of_buy_trades.push(event.number_of_buy_trades);
            number_of_sell_trades.push(event.number_of_sell_trades);
        }

        let df = df![
            CanonicalCol::OpenTimestamp.to_string() => window_starts,
            CanonicalCol::Timestamp.to_string() => window_ends,
            CanonicalCol::PriceBinStart.to_string() => price_bin_starts,
            CanonicalCol::PriceBinEnd.to_string() => price_bin_ends,
            CanonicalCol::Volume.to_string() => base_volumes,
            CanonicalCol::TakerBuyBaseAssetVolume.to_string() => taker_buy_base_volumes,
            CanonicalCol::TakerSellBaseAssetVolume.to_string() => taker_sell_base_volumes,
            CanonicalCol::QuoteAssetVolume.to_string() => quote_volumes,
            CanonicalCol::TakerBuyQuoteAssetVolume.to_string() => taker_buy_quote_volumes,
            CanonicalCol::TakerSellQuoteAssetVolume.to_string() => taker_sell_quote_volumes,
            CanonicalCol::NumberOfTrades.to_string() => number_of_trades,
            CanonicalCol::NumberOfBuyTrades.to_string() => number_of_buy_trades,
            CanonicalCol::NumberOfSellTrades.to_string() => number_of_sell_trades,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(CanonicalCol::OpenTimestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        Ok(lf)
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================
fn empty_lf(schema: &Schema) -> ChapatyResult<LazyFrame> {
    Ok(DataFrame::empty_with_schema(schema).lazy())
}

fn extract_timestamp(ts: &Option<Timestamp>, field: &str) -> ChapatyResult<i64> {
    ts.as_ref()
        .map(timestamp_to_micro)
        .transpose()?
        .ok_or_else(|| {
            ChapatyError::Data(DataError::TimestampConversion(format!(
                "Missing {} in microseconds",
                field
            )))
        })
}

fn timestamp_to_micro(ts: &Timestamp) -> ChapatyResult<i64> {
    let secs = ts.seconds;
    let nanos = ts.nanos as i64;
    secs.checked_mul(1_000_000)
        .and_then(|s| s.checked_add(nanos / 1_000))
        .ok_or_else(|| {
            ChapatyError::Data(DataError::TimestampConversion(
                "Failed to convert timestamp to microseconds due to overflow".to_string(),
            ))
        })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::chapaty::data::v1::{
        EconomicCalendarBatch, EconomicCalendarEvent, OhlcvFutureBatch, OhlcvFutureEvent,
        OhlcvSpotBatch, OhlcvSpotEvent, TpoFutureBatch, TpoFutureEvent, TpoSpotBatch, TpoSpotEvent,
        TradesSpotBatch, TradesSpotEvent, VolumeProfileSpotBatch, VolumeProfileSpotEvent,
    };
    use polars::prelude::AnyValue;
    use prost_types::Timestamp;

    // ========================================================================
    // Test Helpers
    // ========================================================================

    /// Creates a UTC timestamp for January 2026.
    ///
    /// # Panics
    /// Panics if `day` is 0 or if the resulting date is invalid.
    fn make_timestamp(day: u8, hour: u8) -> Timestamp {
        assert!(day >= 1 && day <= 31, "Day must be between 1 and 31");
        assert!(hour <= 23, "Hour must be between 0 and 23");

        let rfc3339 = format!("2026-01-{day:02}T{hour:02}:00:00Z");
        let dt = chrono::DateTime::parse_from_rfc3339(&rfc3339)
            .expect("Invalid date")
            .with_timezone(&chrono::Utc);

        Timestamp {
            seconds: dt.timestamp(),
            nanos: 0,
        }
    }

    /// Converts a protobuf Timestamp to microseconds for comparison.
    fn to_micros(ts: &Timestamp) -> i64 {
        ts.seconds * 1_000_000 + (ts.nanos as i64 / 1_000)
    }

    /// Extracts a scalar value from a DataFrame cell.
    fn get_i64(df: &DataFrame, col: CanonicalCol, row: usize) -> i64 {
        let series = df.column(col.as_str()).expect("Column not found");
        match series.get(row).expect("Row not found").into_static() {
            AnyValue::Int64(v) => v,
            AnyValue::Datetime(v, _, _) | AnyValue::DatetimeOwned(v, _, _) => v,
            other => panic!("Expected i64-compatible value, got {other:?}"),
        }
    }

    fn get_f64(df: &DataFrame, col: CanonicalCol, row: usize) -> f64 {
        let series = df.column(col.as_str()).expect("Column not found");
        match series.get(row).expect("Row not found") {
            AnyValue::Float64(v) => v,
            other => panic!("Expected f64, got {other:?}"),
        }
    }

    fn get_bool(df: &DataFrame, col: CanonicalCol, row: usize) -> bool {
        let series = df.column(col.as_str()).expect("Column not found");
        match series.get(row).expect("Row not found") {
            AnyValue::Boolean(v) => v,
            other => panic!("Expected bool, got {other:?}"),
        }
    }

    fn get_string(df: &DataFrame, col: CanonicalCol, row: usize) -> String {
        let series = df.column(col.as_str()).expect("Column not found");
        match series.get(row).expect("Row not found").into_static() {
            AnyValue::String(s) => s.to_string(),
            AnyValue::StringOwned(s) => s.into(),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    fn get_opt_f64(df: &DataFrame, col: CanonicalCol, row: usize) -> Option<f64> {
        let series = df.column(col.as_str()).expect("Column not found");
        match series.get(row).expect("Row not found") {
            AnyValue::Null => None,
            AnyValue::Float64(v) => Some(v),
            other => panic!("Expected Option<f64>, got {other:?}"),
        }
    }

    /// Asserts that a timestamp column matches the expected protobuf timestamp.
    fn assert_timestamp_eq(df: &DataFrame, col: CanonicalCol, row: usize, expected: &Timestamp) {
        let actual = get_i64(df, col, row);
        let expected_micros = to_micros(expected);
        assert_eq!(
            actual,
            expected_micros,
            "Timestamp mismatch for {}: expected {expected_micros}, got {actual}",
            col.as_str()
        );
    }

    // ========================================================================
    // Economic Calendar Tests
    // ========================================================================

    #[test]
    fn economic_calendar_single_event_all_fields() {
        let ts = make_timestamp(1, 10);
        let event = EconomicCalendarEvent {
            data_source: "investingcom".into(),
            category: "employment".into(),
            event_timestamp: Some(ts.clone()),
            news_type: "nfp".into(),
            news_type_confidence: Some(0.95),
            news_type_source: "ml".into(),
            periodicity: "mom".into(),
            news_name: "Non-Farm Payrolls".into(),
            country_code: "us".into(),
            currency_code: "usd".into(),
            importance: 3,
            actual: Some(3.5),
            forecast: Some(3.6),
            previous: Some(3.4),
        };

        let response = EconomicCalendarResponse {
            batch: Some(EconomicCalendarBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        // Schema validation
        assert_eq!(*df.schema(), economic_calendar_schema());
        assert_eq!(df.height(), 1);

        // All fields validation
        assert_eq!(get_string(&df, CanonicalCol::DataSource, 0), "investingcom");
        assert_eq!(get_string(&df, CanonicalCol::Category, 0), "employment");
        assert_eq!(get_string(&df, CanonicalCol::NewsType, 0), "nfp");
        assert_eq!(get_f64(&df, CanonicalCol::NewsTypeConfidence, 0), 0.95);
        assert_eq!(get_string(&df, CanonicalCol::NewsTypeSource, 0), "ml");
        assert_eq!(get_string(&df, CanonicalCol::Period, 0), "mom");
        assert_eq!(
            get_string(&df, CanonicalCol::NewsName, 0),
            "Non-Farm Payrolls"
        );
        assert_eq!(get_string(&df, CanonicalCol::CountryCode, 0), "us");
        assert_eq!(get_string(&df, CanonicalCol::CurrencyCode, 0), "usd");
        assert_eq!(get_i64(&df, CanonicalCol::EconomicImpact, 0), 3);
        assert_eq!(get_opt_f64(&df, CanonicalCol::Actual, 0), Some(3.5));
        assert_eq!(get_opt_f64(&df, CanonicalCol::Forecast, 0), Some(3.6));
        assert_eq!(get_opt_f64(&df, CanonicalCol::Previous, 0), Some(3.4));
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &ts);
    }

    #[test]
    fn economic_calendar_empty_batch_returns_empty_df_with_schema() {
        let response = EconomicCalendarResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), economic_calendar_schema());
    }

    #[test]
    fn economic_calendar_optional_values_as_none() {
        let ts = make_timestamp(2, 8);
        let event = EconomicCalendarEvent {
            event_timestamp: Some(ts),
            actual: None,
            forecast: None,
            previous: None,
            ..Default::default()
        };

        let response = EconomicCalendarResponse {
            batch: Some(EconomicCalendarBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(get_opt_f64(&df, CanonicalCol::Actual, 0), None);
        assert_eq!(get_opt_f64(&df, CanonicalCol::Forecast, 0), None);
        assert_eq!(get_opt_f64(&df, CanonicalCol::Previous, 0), None);
    }

    // ========================================================================
    // OHLCV Future Tests
    // ========================================================================

    #[test]
    fn ohlcv_future_single_candle_all_fields() {
        let open_ts = make_timestamp(2, 9);
        let close_ts = make_timestamp(2, 10);

        let event = OhlcvFutureEvent {
            open_timestamp: Some(open_ts.clone()),
            close_timestamp: Some(close_ts.clone()),
            open: 50000.0,
            high: 51000.0,
            low: 49500.0,
            close: 50500.0,
            volume: 100.0,
        };

        let response = OhlcvFutureResponse {
            batch: Some(OhlcvFutureBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(*df.schema(), ohlcv_future_schema());
        assert_eq!(df.height(), 1);

        assert_eq!(get_f64(&df, CanonicalCol::Open, 0), 50000.0);
        assert_eq!(get_f64(&df, CanonicalCol::High, 0), 51000.0);
        assert_eq!(get_f64(&df, CanonicalCol::Low, 0), 49500.0);
        assert_eq!(get_f64(&df, CanonicalCol::Close, 0), 50500.0);
        assert_eq!(get_f64(&df, CanonicalCol::Volume, 0), 100.0);
        assert_timestamp_eq(&df, CanonicalCol::OpenTimestamp, 0, &open_ts);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &close_ts);
    }

    #[test]
    fn ohlcv_future_empty_batch_returns_empty_df_with_schema() {
        let response = OhlcvFutureResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), ohlcv_future_schema());
    }

    // ========================================================================
    // OHLCV Spot Tests
    // ========================================================================

    #[test]
    fn ohlcv_spot_single_candle_all_fields() {
        let open_ts = make_timestamp(3, 14);
        let close_ts = make_timestamp(3, 15);

        let event = OhlcvSpotEvent {
            open_timestamp: Some(open_ts.clone()),
            close_timestamp: Some(close_ts.clone()),
            open: 100.0,
            high: 110.0,
            low: 90.0,
            close: 105.0,
            volume: 1000.0,
            quote_asset_volume: 105000.0,
            number_of_trades: 50,
            taker_buy_base_asset_volume: 600.0,
            taker_buy_quote_asset_volume: 63000.0,
        };

        let response = OhlcvSpotResponse {
            batch: Some(OhlcvSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(*df.schema(), ohlcv_spot_schema());
        assert_eq!(df.height(), 1);

        assert_eq!(get_f64(&df, CanonicalCol::Open, 0), 100.0);
        assert_eq!(get_f64(&df, CanonicalCol::High, 0), 110.0);
        assert_eq!(get_f64(&df, CanonicalCol::Low, 0), 90.0);
        assert_eq!(get_f64(&df, CanonicalCol::Close, 0), 105.0);
        assert_eq!(get_f64(&df, CanonicalCol::Volume, 0), 1000.0);
        assert_eq!(get_f64(&df, CanonicalCol::QuoteAssetVolume, 0), 105000.0);
        assert_eq!(get_i64(&df, CanonicalCol::NumberOfTrades, 0), 50);
        assert_eq!(
            get_f64(&df, CanonicalCol::TakerBuyBaseAssetVolume, 0),
            600.0
        );
        assert_eq!(
            get_f64(&df, CanonicalCol::TakerBuyQuoteAssetVolume, 0),
            63000.0
        );
        assert_timestamp_eq(&df, CanonicalCol::OpenTimestamp, 0, &open_ts);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &close_ts);
    }

    #[test]
    fn ohlcv_spot_empty_batch_returns_empty_df_with_schema() {
        let response = OhlcvSpotResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), ohlcv_spot_schema());
    }

    // ========================================================================
    // Trade Spot Tests
    // ========================================================================

    #[test]
    fn trade_spot_single_trade_all_fields() {
        let trade_ts = make_timestamp(4, 12);

        let event = TradesSpotEvent {
            trade_id: 999888,
            price: 200.50,
            quantity: 2.0,
            quote_quantity: 401.0,
            trade_timestamp: Some(trade_ts.clone()),
            is_buyer_maker: true,
            is_best_match: true,
        };

        let response = TradesSpotResponse {
            batch: Some(TradesSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(*df.schema(), trades_spot_schema());
        assert_eq!(df.height(), 1);

        assert_eq!(get_i64(&df, CanonicalCol::TradeId, 0), 999888);
        assert_eq!(get_f64(&df, CanonicalCol::Price, 0), 200.50);
        assert_eq!(get_f64(&df, CanonicalCol::Volume, 0), 2.0);
        assert_eq!(get_f64(&df, CanonicalCol::QuoteAssetVolume, 0), 401.0);
        assert!(get_bool(&df, CanonicalCol::IsBuyerMaker, 0));
        assert!(get_bool(&df, CanonicalCol::IsBestMatch, 0));
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &trade_ts);
    }

    #[test]
    fn trade_spot_empty_batch_returns_empty_df_with_schema() {
        let response = TradesSpotResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), trades_spot_schema());
    }

    #[test]
    fn trade_spot_boolean_flags_false() {
        let ts = make_timestamp(4, 13);
        let event = TradesSpotEvent {
            trade_id: 1,
            trade_timestamp: Some(ts),
            is_buyer_maker: false,
            is_best_match: false,
            ..Default::default()
        };

        let response = TradesSpotResponse {
            batch: Some(TradesSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert!(!get_bool(&df, CanonicalCol::IsBuyerMaker, 0));
        assert!(!get_bool(&df, CanonicalCol::IsBestMatch, 0));
    }

    // ========================================================================
    // TPO Future Tests
    // ========================================================================

    #[test]
    fn tpo_future_single_profile_all_fields() {
        let win_start = make_timestamp(5, 8);
        let win_end = make_timestamp(5, 9);

        let event = TpoFutureEvent {
            window_start: Some(win_start.clone()),
            window_end: Some(win_end.clone()),
            price_bin_start: 40000.0,
            price_bin_end: 40010.0,
            time_slot_count: 15,
        };

        let response = TpoFutureResponse {
            batch: Some(TpoFutureBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(*df.schema(), tpo_future_schema());
        assert_eq!(df.height(), 1);

        assert_eq!(get_f64(&df, CanonicalCol::PriceBinStart, 0), 40000.0);
        assert_eq!(get_f64(&df, CanonicalCol::PriceBinEnd, 0), 40010.0);
        assert_eq!(get_i64(&df, CanonicalCol::TimeSlotCount, 0), 15);
        assert_timestamp_eq(&df, CanonicalCol::OpenTimestamp, 0, &win_start);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &win_end);
    }

    #[test]
    fn tpo_future_empty_batch_returns_empty_df_with_schema() {
        let response = TpoFutureResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), tpo_future_schema());
    }

    // ========================================================================
    // TPO Spot Tests
    // ========================================================================

    #[test]
    fn tpo_spot_single_profile_all_fields() {
        let win_start = make_timestamp(6, 10);
        let win_end = make_timestamp(6, 11);

        let event = TpoSpotEvent {
            window_start: Some(win_start.clone()),
            window_end: Some(win_end.clone()),
            price_bin_start: 150.0,
            price_bin_end: 151.0,
            time_slot_count: 5,
        };

        let response = TpoSpotResponse {
            batch: Some(TpoSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(*df.schema(), tpo_spot_schema());
        assert_eq!(df.height(), 1);

        assert_eq!(get_f64(&df, CanonicalCol::PriceBinStart, 0), 150.0);
        assert_eq!(get_f64(&df, CanonicalCol::PriceBinEnd, 0), 151.0);
        assert_eq!(get_i64(&df, CanonicalCol::TimeSlotCount, 0), 5);
        assert_timestamp_eq(&df, CanonicalCol::OpenTimestamp, 0, &win_start);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &win_end);
    }

    #[test]
    fn tpo_spot_empty_batch_returns_empty_df_with_schema() {
        let response = TpoSpotResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), tpo_spot_schema());
    }

    // ========================================================================
    // Volume Profile Spot Tests
    // ========================================================================

    #[test]
    fn volume_profile_spot_single_bin_all_fields() {
        let win_start = make_timestamp(7, 0);
        let win_end = make_timestamp(7, 23);

        let event = VolumeProfileSpotEvent {
            window_start: Some(win_start.clone()),
            window_end: Some(win_end.clone()),
            price_bin_start: 25.0,
            price_bin_end: 25.5,
            base_volume: 5000.0,
            taker_buy_base_volume: 2500.0,
            taker_sell_base_volume: 2500.0,
            quote_volume: 125000.0,
            taker_buy_quote_volume: 62500.0,
            taker_sell_quote_volume: 62500.0,
            number_of_trades: 100,
            number_of_buy_trades: 60,
            number_of_sell_trades: 40,
        };

        let response = VolumeProfileSpotResponse {
            batch: Some(VolumeProfileSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(*df.schema(), volume_profile_spot_schema());
        assert_eq!(df.height(), 1);

        assert_eq!(get_f64(&df, CanonicalCol::PriceBinStart, 0), 25.0);
        assert_eq!(get_f64(&df, CanonicalCol::PriceBinEnd, 0), 25.5);
        assert_eq!(get_f64(&df, CanonicalCol::Volume, 0), 5000.0);
        assert_eq!(
            get_f64(&df, CanonicalCol::TakerBuyBaseAssetVolume, 0),
            2500.0
        );
        assert_eq!(
            get_f64(&df, CanonicalCol::TakerSellBaseAssetVolume, 0),
            2500.0
        );
        assert_eq!(get_f64(&df, CanonicalCol::QuoteAssetVolume, 0), 125000.0);
        assert_eq!(
            get_f64(&df, CanonicalCol::TakerBuyQuoteAssetVolume, 0),
            62500.0
        );
        assert_eq!(
            get_f64(&df, CanonicalCol::TakerSellQuoteAssetVolume, 0),
            62500.0
        );
        assert_eq!(get_i64(&df, CanonicalCol::NumberOfTrades, 0), 100);
        assert_eq!(get_i64(&df, CanonicalCol::NumberOfBuyTrades, 0), 60);
        assert_eq!(get_i64(&df, CanonicalCol::NumberOfSellTrades, 0), 40);
        assert_timestamp_eq(&df, CanonicalCol::OpenTimestamp, 0, &win_start);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &win_end);
    }

    #[test]
    fn volume_profile_spot_empty_batch_returns_empty_df_with_schema() {
        let response = VolumeProfileSpotResponse {
            batch: None,
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(*df.schema(), volume_profile_spot_schema());
    }

    // ========================================================================
    // Multi-Row & Order Preservation Tests
    // ========================================================================

    #[test]
    fn multiple_events_preserves_insertion_order() {
        let t1 = make_timestamp(12, 1);
        let t2 = make_timestamp(12, 2);
        let t3 = make_timestamp(12, 3);

        let events = vec![
            TradesSpotEvent {
                trade_id: 100,
                trade_timestamp: Some(t1.clone()),
                ..Default::default()
            },
            TradesSpotEvent {
                trade_id: 200,
                trade_timestamp: Some(t2.clone()),
                ..Default::default()
            },
            TradesSpotEvent {
                trade_id: 300,
                trade_timestamp: Some(t3.clone()),
                ..Default::default()
            },
        ];

        let response = TradesSpotResponse {
            batch: Some(TradesSpotBatch { events }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 3);
        assert_eq!(get_i64(&df, CanonicalCol::TradeId, 0), 100);
        assert_eq!(get_i64(&df, CanonicalCol::TradeId, 1), 200);
        assert_eq!(get_i64(&df, CanonicalCol::TradeId, 2), 300);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 0, &t1);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 1, &t2);
        assert_timestamp_eq(&df, CanonicalCol::Timestamp, 2, &t3);
    }

    #[test]
    fn multiple_ohlcv_candles_preserves_order() {
        let events = (1..=5)
            .map(|i| OhlcvFutureEvent {
                open_timestamp: Some(make_timestamp(i, 0)),
                close_timestamp: Some(make_timestamp(i, 1)),
                open: i as f64 * 100.0,
                high: i as f64 * 110.0,
                low: i as f64 * 90.0,
                close: i as f64 * 105.0,
                volume: i as f64 * 10.0,
            })
            .collect();

        let response = OhlcvFutureResponse {
            batch: Some(OhlcvFutureBatch { events }),
            metadata: None,
        };

        let df = response.into_lazyframe().unwrap().collect().unwrap();

        assert_eq!(df.height(), 5);

        for i in 0..5 {
            let expected_open = (i + 1) as f64 * 100.0;
            assert_eq!(get_f64(&df, CanonicalCol::Open, i), expected_open);
        }
    }

    // ========================================================================
    // Error Handling Tests
    // ========================================================================

    #[test]
    fn missing_required_timestamp_returns_error() {
        let event = TradesSpotEvent {
            trade_id: 1,
            trade_timestamp: None, // Required field is missing
            ..Default::default()
        };

        let response = TradesSpotResponse {
            batch: Some(TradesSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let result = response.into_lazyframe();

        assert!(result.is_err(), "Expected error for missing timestamp");
        let err = result.err().expect("Already checked is_err");
        assert!(
            err.to_string().contains("trade_timestamp"),
            "Error should mention the missing field: {err}"
        );
    }

    #[test]
    fn missing_ohlcv_timestamps_returns_error() {
        let event = OhlcvSpotEvent {
            open_timestamp: None,
            close_timestamp: Some(make_timestamp(1, 1)),
            ..Default::default()
        };

        let response = OhlcvSpotResponse {
            batch: Some(OhlcvSpotBatch {
                events: vec![event],
            }),
            metadata: None,
        };

        let result = response.into_lazyframe();
        assert!(result.is_err());
    }

    // ========================================================================
    // Helper Function Unit Tests
    // ========================================================================

    #[test]
    fn timestamp_to_micro_converts_correctly() {
        let ts = Timestamp {
            seconds: 1735689600, // 2025-01-01 00:00:00 UTC
            nanos: 500_000_000,  // 0.5 seconds
        };

        let micros = timestamp_to_micro(&ts).unwrap();

        // 1735689600 * 1_000_000 + 500_000 = 1735689600500000
        assert_eq!(micros, 1735689600_500_000);
    }

    #[test]
    fn timestamp_to_micro_zero_nanos() {
        let ts = Timestamp {
            seconds: 1000,
            nanos: 0,
        };

        let micros = timestamp_to_micro(&ts).unwrap();
        assert_eq!(micros, 1_000_000_000);
    }

    #[test]
    fn timestamp_to_micro_max_nanos() {
        let ts = Timestamp {
            seconds: 0,
            nanos: 999_999_999, // Just under 1 second
        };

        let micros = timestamp_to_micro(&ts).unwrap();
        assert_eq!(micros, 999_999); // 999_999_999 / 1000 = 999_999
    }

    #[test]
    fn extract_timestamp_missing_returns_error() {
        let result = extract_timestamp(&None, "test_field");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("test_field"),
            "Error should mention the field name: {err}"
        );
    }

    #[test]
    fn extract_timestamp_present_returns_micros() {
        let ts = Timestamp {
            seconds: 1000,
            nanos: 1_000_000, // 1ms = 1000 micros
        };

        let result = extract_timestamp(&Some(ts), "test_field");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1_000_001_000);
    }
}
