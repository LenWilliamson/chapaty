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
// Core Types & Configurations
// ================================================================================================

/// Defines the target export format and holds specific Polars writing options.
#[derive(Debug, Clone)]
pub enum ExportFormat {
    Csv(CsvWriterOptions),
    Parquet(ParquetWriteOptions),
}

impl Default for ExportFormat {
    fn default() -> Self {
        Self::Csv(CsvWriterOptions::default())
    }
}

/// Configuration for exporting reports to the local file system.
#[derive(Debug, Clone)]
pub struct FileConfig<'a> {
    pub dir: &'a Path,
    pub file_stem: Option<String>,
    pub format: ExportFormat,
    pub sink_opts: SinkOptions,
}

impl Default for FileConfig<'_> {
    fn default() -> Self {
        Self {
            dir: Path::new("./chapaty/reports"),
            file_stem: None,
            format: ExportFormat::default(),
            sink_opts: SinkOptions::default(),
        }
    }
}

impl<'a> FileConfig<'a> {
    pub fn with_dir(self, dir: &'a Path) -> Self {
        Self { dir, ..self }
    }

    pub fn with_file_stem(self, file_stem: impl Into<String>) -> Self {
        Self {
            file_stem: Some(file_stem.into()),
            ..self
        }
    }

    pub fn with_format(self, format: ExportFormat) -> Self {
        Self { format, ..self }
    }

    pub fn with_sink_opts(self, sink_opts: SinkOptions) -> Self {
        Self { sink_opts, ..self }
    }
}

/// Configuration for exporting reports to cloud storage (GCS, S3, Azure).
///
/// # Important: Full URIs Required
/// To prevent URL malformation, `CloudConfig` requires the **complete URI**, including
/// the file name and extension (e.g., `gs://bucket/path/to/my_report.csv`).
/// Do not pass a directory URI.
#[derive(Debug, Clone)]
pub struct CloudConfig<'a> {
    pub uri: &'a str,
    pub format: ExportFormat,
    pub cloud_opts: CloudOptions,
    pub sink_opts: SinkOptions,
}

impl<'a> CloudConfig<'a> {
    /// Creates a new `CloudConfig` targeting a specific, complete Cloud URI.
    pub fn new(uri: &'a str) -> Self {
        Self {
            uri,
            format: ExportFormat::default(),
            cloud_opts: CloudOptions::default(),
            sink_opts: SinkOptions::default(),
        }
    }

    pub fn with_format(self, format: ExportFormat) -> Self {
        Self { format, ..self }
    }

    pub fn with_cloud_opts(self, cloud_opts: CloudOptions) -> Self {
        Self { cloud_opts, ..self }
    }

    pub fn with_sink_opts(self, sink_opts: SinkOptions) -> Self {
        Self { sink_opts, ..self }
    }
}

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

pub trait ExportSync {
    /// Writes the report to a local file system (blocking).
    ///
    /// - Creates directories if they do not exist.
    /// - Automatically generates a file name if one is not provided in `FileConfig`.
    fn to_file_sync(&self, config: &FileConfig<'_>) -> ChapatyResult<()>;
}

#[async_trait]
pub trait Export {
    /// Streams the report to a cloud bucket using Polars' streaming engine.
    async fn to_cloud(&self, config: CloudConfig<'_>) -> ChapatyResult<()>;
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

impl<T> ExportSync for T
where
    T: Report + ReportName + ToSchema + Sync + Send,
{
    fn to_file_sync(&self, config: &FileConfig<'_>) -> ChapatyResult<()> {
        let ext: FileExtension = (&config.format).into();
        let filename = match &config.file_stem {
            Some(stem) => format!("{stem}.{ext}"),
            None => format!("{}.{ext}", self.base_name()),
        };
        let file_path = config.dir.join(&filename);

        if !config.dir.exists() {
            fs::create_dir_all(config.dir).map_err(|e| {
                IoError::FileSystem(format!(
                    "Failed to create directory {}: {}",
                    config.dir.display(),
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
        let sink_opts = &config.sink_opts;
        let lf = self.as_formatted_lf();

        let sink_plan = match &config.format {
            ExportFormat::Csv(opts) => lf
                .sink_csv(target, opts.clone(), None, sink_opts.clone())
                .map_err(|e| DataError::DataFrame(format!("Failed to build CSV sink plan: {e}"))),
            ExportFormat::Parquet(opts) => lf
                .sink_parquet(target, opts.clone(), None, sink_opts.clone())
                .map_err(|e| {
                    DataError::DataFrame(format!("Failed to build Parquet sink plan: {e}"))
                }),
        }?;

        let _ = sink_plan.collect().map_err(|e| {
            DataError::DataFrame(format!(
                "Failed to write file to '{}': {e}",
                file_path.display()
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl<T> Export for T
where
    T: Report + ReportName + ToSchema + Sync + Send,
{
    async fn to_cloud(&self, config: CloudConfig<'_>) -> ChapatyResult<()> {
        let lf = self.as_formatted_lf();
        let target = SinkTarget::Path(PlPath::new(config.uri));
        let cloud_opts = config.cloud_opts;
        let sink_opts = config.sink_opts;
        let format = config.format;

        // Clone URI to move into the blocking task safely
        let uri_string = config.uri.to_string();

        tokio::task::spawn_blocking(move || {
            let sink_plan = match format {
                ExportFormat::Csv(opts) => lf
                    .sink_csv(target, opts, Some(cloud_opts), sink_opts)
                    .map_err(|e| {
                        DataError::DataFrame(format!("Failed to build Cloud CSV plan: {e}"))
                    }),
                ExportFormat::Parquet(opts) => lf
                    .with_new_streaming(true)
                    .sink_parquet(target, opts, Some(cloud_opts), sink_opts)
                    .map_err(|e| {
                        DataError::DataFrame(format!("Failed to build Cloud Parquet plan: {e}"))
                    }),
            }?;

            let _ = sink_plan.collect().map_err(|e| {
                DataError::DataFrame(format!("Streaming upload failed to '{}': {e}", uri_string))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SystemError::Generic(format!("Task Join Error: {e}")))?
    }
}

// ================================================================================================
// Helpers
// ================================================================================================

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
enum FileExtension {
    Csv,
    Parquet,
}

impl From<&ExportFormat> for FileExtension {
    fn from(format: &ExportFormat) -> Self {
        match format {
            ExportFormat::Csv(_) => Self::Csv,
            ExportFormat::Parquet(_) => Self::Parquet,
        }
    }
}

impl From<ExportFormat> for FileExtension {
    fn from(format: ExportFormat) -> Self {
        (&format).into()
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
        let df_symbol = df![
            "__symbol" => &["BTC"],
            "pnl" => &[100.0]
        ]
        .expect("Failed to create symbol DF");

        let name_symbol = generate_dynamic_base_name(&df_symbol, "stats");
        assert_eq!(name_symbol, "symbol_stats");

        // 3. Case: Multi Group (Symbol + Year)
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
