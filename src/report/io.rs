use std::{fs, path::Path};

use polars::{
    frame::DataFrame,
    io::cloud::CloudOptions,
    prelude::{
        CsvWriterOptions, IntoLazy, LazyFrame, ParquetWriteOptions, PlPath, SchemaRef, SinkOptions,
        SinkTarget,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum::{Display, EnumIter, EnumString, IntoStaticStr};
use tonic::async_trait;

use crate::{
    error::{ChapatyError, ChapatyResult, DataError, IoError, SystemError},
    report::polars_ext::{DataFrameExt, LazyFrameExt},
};

// ================================================================================================
// Traits
// ================================================================================================

/// Defines a common interface for all Report types (Journal, TradeStats, etc.).
pub trait Report {
    /// Access the underlying DataFrame (Immutable).
    fn as_df(&self) -> &DataFrame;

    /// Access the underlying DataFrame (Mutable).
    fn as_df_mut(&mut self) -> &mut DataFrame;
}

pub trait ReportName {
    fn base_name(&self) -> String;

    fn filename(&self, ext: FileExtension) -> String {
        format!("{}.{}", self.base_name(), ext)
    }
}

pub trait ToSchema {
    /// Returns the canonical schema for this report type.
    fn to_schema() -> SchemaRef;
}

pub trait AsFormattedLazyFrame {
    fn as_formatted_lf(&self) -> LazyFrame;
}

pub trait ToJson {
    /// Serializes the report to a generic JSON Value.
    /// Returns a `Value::Array` containing row objects.
    fn to_json(&self) -> ChapatyResult<serde_json::Value>;
}

pub trait ToCsv {
    /// Writes the report to a CSV file in the target directory.
    ///
    /// # Formatting
    /// - Applies human-readable formatting to Duration columns (e.g. "2d 1h").
    /// - Uses the canonical schema defined in `ToSchema`.
    ///
    /// # Arguments
    /// - `dir`: Target directory. Created if it doesn't exist.
    /// - `opts`: CSV writing options (delimiter, headers, etc.).
    ///
    /// # Side Effects
    /// - Creates the directory if missing.
    /// - Overwrites the file if it exists.
    fn to_csv(
        &self,
        dir: impl AsRef<Path>,
        opts: Option<&CsvWriterOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()>;
}

pub trait ToParquet {
    fn to_parquet(
        &self,
        dir: impl AsRef<Path>,
        opts: Option<&ParquetWriteOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()>;
}

#[async_trait]
pub trait ToCloudCsv {
    /// Streams the report to the specified Cloud URI as a CSV.
    ///
    /// # Performance
    /// - Uses Polars' **Streaming Engine**: Data is formatted and uploaded in chunks.
    /// - Memory Efficient: Does not materialize the full dataset in RAM.
    /// - Non-Blocking: Offloads the entire execution graph to a blocking thread.
    ///
    /// # Arguments
    /// * `uri` - The full bucket URI (e.g., `gs://bucket/path.csv`).
    /// * `opts` - CSV formatting options.
    /// * `cloud_opts` - Credentials/Region config.
    /// * `sink_opts` - Sink config.
    async fn stream_csv(
        &self,
        uri: &str,
        opts: Option<&CsvWriterOptions>,
        cloud_opts: Option<&CloudOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()>;
}

#[async_trait]
pub trait ToCloudParquet {
    async fn stream_parquet(
        &self,
        uri: &str,
        opts: Option<&ParquetWriteOptions>,
        cloud_opts: Option<&CloudOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()>;
}

// ================================================================================================
// Blanket Implementations
// ================================================================================================

impl<T> AsFormattedLazyFrame for T
where
    T: Report + ToSchema,
{
    fn as_formatted_lf(&self) -> LazyFrame {
        self.as_df()
            .clone()
            .lazy()
            .with_human_durations(T::to_schema())
    }
}

impl<T> ToJson for T
where
    T: Report + ToSchema,
{
    fn to_json(&self) -> ChapatyResult<serde_json::Value> {
        let rows = self
            .as_formatted_lf()
            .collect()
            .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?
            .to_json_rows()?;
        Ok(Value::Array(rows.into_iter().map(Value::Object).collect()))
    }
}

impl<T> ToCsv for T
where
    T: Report + ReportName + ToSchema,
{
    fn to_csv(
        &self,
        dir: impl AsRef<Path>,
        opts: Option<&CsvWriterOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()> {
        let dir = dir.as_ref();
        let file_path = dir.join(self.filename(FileExtension::Csv));

        if !dir.exists() {
            fs::create_dir_all(dir).map_err(|e| {
                IoError::FileSystem(format!(
                    "Failed to create directory {}: {}",
                    dir.display(),
                    e
                ))
            })?;
        }

        let uri = file_path.to_str().ok_or_else(|| {
            IoError::FileSystem(format!(
                "Path contains invalid UTF-8 characters: {}",
                file_path.display()
            ))
        })?;
        let target = SinkTarget::Path(PlPath::new(uri));
        let options = opts.cloned().unwrap_or_default();
        let sink_opts = sink_opts.cloned().unwrap_or_default();

        let lf = self.as_formatted_lf();

        let sink_plan = lf
            .sink_csv(target, options, None, sink_opts)
            .map_err(|e| DataError::DataFrame(format!("Failed to build CSV sink plan: {e}")))?;

        let _ = sink_plan.collect().map_err(|e| {
            DataError::DataFrame(format!(
                "Failed to write CSV to '{}': {e}",
                file_path.display()
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl<T> ToCloudCsv for T
where
    T: Report + ToSchema + Sync + Send,
{
    async fn stream_csv(
        &self,
        uri: &str,
        opts: Option<&CsvWriterOptions>,
        cloud_opts: Option<&CloudOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()> {
        let lf = self.as_formatted_lf();
        let target = SinkTarget::Path(PlPath::new(uri));
        let options = opts.cloned().unwrap_or_default();
        let cloud_options = cloud_opts.cloned();
        let sink_opts = sink_opts.cloned().unwrap_or_default();

        tokio::task::spawn_blocking(move || {
            let sink_plan = lf
                .sink_csv(target, options, cloud_options, sink_opts)
                .map_err(|e| DataError::DataFrame(format!("Failed to build sink plan: {e}")))?;

            let _ = sink_plan
                .collect()
                .map_err(|e| DataError::DataFrame(format!("Streaming CSV upload failed: {e}")))?;

            Ok(())
        })
        .await
        .map_err(|e| SystemError::Generic(format!("Task Join Error: {e}")))?
    }
}

impl<T> ToParquet for T
where
    T: Report + ReportName + ToSchema,
{
    fn to_parquet(
        &self,
        dir: impl AsRef<Path>,
        opts: Option<&ParquetWriteOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()> {
        let dir = dir.as_ref();
        let file_path = dir.join(self.filename(FileExtension::Parquet));

        if !dir.exists() {
            fs::create_dir_all(dir).map_err(|e| {
                IoError::FileSystem(format!(
                    "Failed to create directory {}: {}",
                    dir.display(),
                    e
                ))
            })?;
        }

        let uri = file_path.to_str().ok_or_else(|| {
            IoError::FileSystem(format!(
                "Path contains invalid UTF-8 characters: {}",
                file_path.display()
            ))
        })?;
        let target = SinkTarget::Path(PlPath::new(uri));
        let options = opts.cloned().unwrap_or_default();
        let sink_opts = sink_opts.cloned().unwrap_or_default();

        let lf = self.as_df().clone().lazy();

        let sink_plan = lf
            .sink_parquet(target, options, None, sink_opts)
            .map_err(|e| DataError::DataFrame(format!("Failed to build Parquet sink plan: {e}")))?;

        let _ = sink_plan.collect().map_err(|e| {
            DataError::DataFrame(format!(
                "Failed to write Parquet to '{}': {e}",
                file_path.display()
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl<T> ToCloudParquet for T
where
    T: Report + ToSchema + Sync + Send,
{
    async fn stream_parquet(
        &self,
        uri: &str,
        opts: Option<&ParquetWriteOptions>,
        cloud_opts: Option<&CloudOptions>,
        sink_opts: Option<&SinkOptions>,
    ) -> ChapatyResult<()> {
        let df = self.as_df().clone();
        let target = SinkTarget::Path(PlPath::new(uri));
        let options = opts.cloned().unwrap_or_default();
        let cloud_options = cloud_opts.cloned();
        let sink_opts = sink_opts.cloned().unwrap_or_default();

        tokio::task::spawn_blocking(move || {
            let lf = df.lazy();

            let sink_plan = lf
                .with_new_streaming(true)
                .sink_parquet(target, options, cloud_options, sink_opts)
                .map_err(|e| {
                    DataError::DataFrame(format!("Failed to build Parquet sink plan: {e}"))
                })?;

            let _ = sink_plan.collect().map_err(|e| {
                DataError::DataFrame(format!("Streaming Parquet upload failed: {e}"))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SystemError::Generic(format!("Task Join Error: {e}")))?
    }
}

/// Generates a base name dynamically based on the presence of grouping columns.
///
/// # Logic
/// 1. Scans the DataFrame column names.
/// 2. Filters for columns starting with `__` (the `GroupCol` prefix).
/// 3. Strips the prefix to get clean names (e.g., `__symbol` -> `symbol`).
/// 4. Joins them to form a prefix for the file.
///
/// # Example
/// - No groups: `cumulative_returns`
/// - Grouped by Symbol: `symbol_cumulative_returns`
/// - Grouped by Symbol & Year: `symbol_entry_year_cumulative_returns`
pub(crate) fn generate_dynamic_base_name(df: &DataFrame, base_name: &str) -> String {
    let group_keys = df
        .get_column_names()
        .iter()
        .filter_map(|name| {
            if name.starts_with("__") {
                Some(name.strip_prefix("__").unwrap_or(name))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if group_keys.is_empty() {
        base_name.to_string()
    } else {
        let prefix = group_keys.join("_");
        format!("{}_{}", prefix, base_name)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    EnumIter,
    IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum FileExtension {
    Csv,
    Parquet,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use polars::{
        df,
        prelude::{LazyCsvReader, LazyFileListReader},
    };

    use crate::{data::common::RiskMetricsConfig, report::journal::Journal};

    use super::*;

    #[test]
    fn test_generate_dynamic_base_name() {
        // 1. Case: No Groups (Plain)
        let df_plain = df![
            "pnl" => &[100.0],
            "count" => &[5]
        ]
        .expect("Failed to create plain DF");

        let name_plain = generate_dynamic_base_name(&df_plain, "stats");
        assert_eq!(name_plain, "stats");

        // 2. Case: Single Group (Symbol)
        // We simulate the "__" prefix that your GroupedJournal logic adds
        let df_symbol = df![
            "__symbol" => &["BTC"],
            "pnl" => &[100.0]
        ]
        .expect("Failed to create symbol DF");

        let name_symbol = generate_dynamic_base_name(&df_symbol, "stats");
        assert_eq!(name_symbol, "symbol_stats");

        // 3. Case: Multi Group (Symbol + Year)
        // Order matters in the output name, driven by column order in DF
        let df_multi = df![
            "__symbol" => &["BTC"],
            "__entry_year" => &[2023],
            "pnl" => &[100.0]
        ]
        .expect("Failed to create multi DF");

        let name_multi = generate_dynamic_base_name(&df_multi, "stats");
        assert_eq!(name_multi, "symbol_entry_year_stats");
    }

    #[test]
    fn test_to_json_rows() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let pb = PathBuf::from(manifest_dir).join("tests/fixtures/report/input/journal.csv");
        let path = PlPath::new(
            pb.as_os_str()
                .to_str()
                .expect("failed to convert input file path to string"),
        );

        let schema = Journal::to_schema();
        let df = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_schema(Some(schema.clone()))
            .with_try_parse_dates(true)
            .finish()
            .expect("failed to create LazyFrame from CSV")
            .collect()
            .expect("failed to collect DataFrame from LazyFrame");

        let journal = Journal::new(df, RiskMetricsConfig::default())
            .expect("failed to create Journal from DataFrame");

        let have = journal
            .to_json()
            .expect("failed to serialize journal to JSON");

        let expected_path =
            PathBuf::from(manifest_dir).join("tests/fixtures/report/expected/journal.json");
        let want =
            std::fs::read_to_string(&expected_path).expect("failed to read expected JSON file");
        let want_value: serde_json::Value =
            serde_json::from_str(&want).expect("failed to parse expected JSON file");

        assert_eq!(have, want_value, "JSON output does not match expected");
    }
}
