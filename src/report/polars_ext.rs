use polars::prelude::{
    Column, DataFrame, DataType, Expr, Field, IntoColumn, IntoSeries, JsonFormat, JsonWriter,
    LazyFrame, PolarsResult, SchemaRef, SerWriter, StringChunked, TimeUnit, col, lit, when,
};
use serde_json::Value;

use crate::error::{ChapatyError, ChapatyResult, DataError, IoError};

pub(super) fn polars_to_chapaty_error(report: &str, e: polars::error::PolarsError) -> ChapatyError {
    ChapatyError::Data(DataError::DataFrame(format!(
        "Error while building {report} from journal DataFrame: {e}"
    )))
}

pub trait ExprExt {
    /// Safely divides two expressions, protecting against division-by-zero.
    ///
    /// If the denominator is zero, returns `fallback` (default: `f64::INFINITY`).
    ///
    /// # Parameters
    /// - `numerator`: The `Expr` for the numerator.
    /// - `denominator`: The `Expr` for the denominator.
    /// - `fallback`: Optional fallback expression to use if denominator == 0.
    ///   If `None`, defaults to `lit(f64::INFINITY)`.
    fn safe_div(self, other: Expr, fallback: Option<f64>) -> Expr;

    /// Formats a Duration column into a human-readable string (e.g., "2h 30m").
    /// Returns null if the duration is negative or null.
    fn human_duration(self) -> Expr;
}

impl ExprExt for Expr {
    fn safe_div(self, other: Expr, fallback: Option<f64>) -> Expr {
        let fallback_val = fallback.unwrap_or(f64::INFINITY);
        when(other.clone().eq(lit(0.0)))
            .then(lit(fallback_val))
            .otherwise(self / other)
    }

    fn human_duration(self) -> Expr {
        self.map(fmt_duration_udf, |_, _| {
            Ok(Field {
                name: "tmp".into(),
                dtype: DataType::String,
            })
        })
    }
}

pub trait DataFrameExt {
    fn to_json_rows(&self) -> ChapatyResult<Vec<serde_json::Map<String, Value>>>;
}

impl DataFrameExt for DataFrame {
    fn to_json_rows(&self) -> ChapatyResult<Vec<serde_json::Map<String, Value>>> {
        let height = self.height();
        if height == 0 {
            return Ok(Vec::new());
        }

        // 1. Pre-allocate buffer (Heuristic: approx 2^6 bytes per row)
        let estimated_row_size = self.width() * (1 << 6);
        let mut buf = Vec::with_capacity(height * estimated_row_size);

        // 2. Serialize to memory
        JsonWriter::new(&mut buf)
            .with_json_format(JsonFormat::Json)
            .finish(&mut self.clone())
            .map_err(|e| DataError::DataFrame(e.to_string()))?;

        // 3. Parse back to Value
        let json_val: Value = serde_json::from_slice(&buf).map_err(IoError::Json)?;

        // 4. Transform to Vec<Map> with exact capacity
        match json_val {
            Value::Array(rows) => {
                // We know exactly how many rows we have
                let mut out_vec = Vec::with_capacity(rows.len());

                for v in rows {
                    if let Value::Object(map) = v {
                        out_vec.push(map);
                    }
                }
                Ok(out_vec)
            }
            _ => {
                Err(DataError::DataFrame("Polars JSON output was not an array".to_string()).into())
            }
        }
    }
}

pub trait LazyFrameExt {
    fn with_human_durations(self, schema: SchemaRef) -> Self;
}

impl LazyFrameExt for LazyFrame {
    fn with_human_durations(self, schema: SchemaRef) -> Self {
        let duration_exprs = schema
            .iter()
            .filter_map(|(name, dtype)| {
                if matches!(dtype, DataType::Duration(_)) {
                    Some(col(name.as_str()).human_duration().alias(name.as_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if duration_exprs.is_empty() {
            return self;
        }

        self.with_columns(duration_exprs)
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn fmt_duration_udf(c: Column) -> PolarsResult<Column> {
    let ca = c.duration()?;
    let unit = ca.time_unit();

    let out = ca
        .physical()
        .into_iter()
        .map(|opt_val| {
            opt_val.and_then(|v| {
                let val = u64::try_from(v).ok()?;
                let duration = match unit {
                    TimeUnit::Microseconds => std::time::Duration::from_micros(val),
                    TimeUnit::Milliseconds => std::time::Duration::from_millis(val),
                    TimeUnit::Nanoseconds => std::time::Duration::from_nanos(val),
                };
                Some(humantime::format_duration(duration).to_string())
            })
        })
        .collect::<StringChunked>()
        .into_series()
        .into_column();
    Ok(out)
}
