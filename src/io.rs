use std::{
    io::{BufReader, BufWriter, Cursor, Read, Write},
    path::Path,
};

use bytes::Bytes;
use polars::{
    io::cloud::{BlockingCloudWriter, CloudOptions, build_object_store, object_path_from_str},
    prelude::PlPathRef,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::error::{ChapatyError, ChapatyResult, IoError};

// ================================================================================================
// I/O Configuration
// ================================================================================================

/// Configuration for loading and caching environment data.
///
/// Encapsulates the storage location, serialization format, and I/O buffer settings
/// to standardize reads and writes across Chapaty environments.
#[derive(Debug, Clone)]
pub struct IoConfig<'a> {
    /// The storage location (local directory, cloud path, or HF dataset).
    pub location: StorageLocation<'a>,
    /// Optional explicit file stem (filename without extension).
    /// If `None`, the environment's configuration hash is used.
    pub file_stem: Option<&'a str>,
    /// The serialization format to use. Defaults to `Postcard`.
    pub format: SerdeFormat,
    /// Size of the internal read/write buffer in bytes. Defaults to 128 KiB.
    pub buffer_size: usize,
}

impl<'a> IoConfig<'a> {
    /// Creates a new I/O configuration with sensible defaults.
    ///
    /// # Defaults
    /// * `file_stem`: `None` (auto-generates from the configuration hash)
    /// * `format`: `SerdeFormat::Postcard`
    /// * `buffer_size`: 128 KiB
    pub fn new(location: StorageLocation<'a>) -> Self {
        Self {
            location,
            file_stem: None,
            format: SerdeFormat::default(),
            buffer_size: 128 * 1024,
        }
    }

    /// Sets an explicit base filename (without extension).
    pub fn with_file_stem(self, file_stem: &'a str) -> Self {
        Self {
            file_stem: Some(file_stem),
            ..self
        }
    }

    /// Sets a specific serialization format.
    pub fn with_format(self, format: SerdeFormat) -> Self {
        Self { format, ..self }
    }

    /// Sets a custom internal I/O buffer size in bytes.
    pub fn with_buffer_size(self, size: usize) -> Self {
        Self {
            buffer_size: size,
            ..self
        }
    }
}

// ================================================================================================
// Cloud Reader
// ================================================================================================

/// An async cloud file reader that can be used synchronously via `Read`.
#[derive(Default, Debug, Clone)]
pub(crate) struct CloudReader {
    inner: Cursor<Bytes>,
}

impl CloudReader {
    pub async fn new(uri: &str, cloud_options: Option<&CloudOptions>) -> ChapatyResult<Self> {
        let (cloud_location, object_store) =
            build_object_store(PlPathRef::new(uri), cloud_options, false)
                .await
                .map_err(|e| IoError::ObjectStoreBuild(e.to_string()))?;

        let path = object_path_from_str(&cloud_location.prefix)
            .map_err(|e| IoError::ObjectPathBuild(e.to_string()))?;

        let result = object_store
            .to_dyn_object_store()
            .await
            .get(&path)
            .await
            .map_err(map_object_store_err)?;

        let bytes = result.bytes().await.map_err(map_object_store_err)?;

        Ok(CloudReader {
            inner: Cursor::new(bytes),
        })
    }
}

impl Read for CloudReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

fn map_object_store_err(err: object_store::Error) -> ChapatyError {
    IoError::ReadBytesFailed(err.to_string()).into()
}

// ================================================================================================
// Storage Location
// ================================================================================================

/// Storage location for simulation data.
///
/// Note: The `path` fields must be **directory paths only**.
/// Do **not** include the file name; it will be determined automatically.
#[derive(Debug, Clone)]
pub enum StorageLocation<'a> {
    /// Cloud storage location (directory only, not a file path).
    Cloud {
        path: &'a str,
        options: CloudOptions,
    },
    /// Local storage location (directory only, not a file path).
    Local { path: &'a Path },

    /// Hugging Face Hosted Dataset.
    ///
    /// By default, the `version` should be set to `None`, which automatically
    /// binds the download to the current `chapaty` crate version to guarantee
    /// strict memory-layout compatibility. You can explicitly provide
    /// a version string (e.g., `"v1.1.0"`) to override this behavior.
    HuggingFace { version: Option<&'a str> },
}

impl<'a> StorageLocation<'a> {
    pub(crate) async fn writer(
        &self,
        filename: &str,
        buffer_size: usize,
    ) -> ChapatyResult<Box<dyn Write + Send>> {
        match self {
            Self::Cloud { path, options } => {
                let full_path = format!("{path}/{filename}");
                BlockingCloudWriter::new(PlPathRef::new(&full_path), Some(options))
                    .await
                    .map(|writer| {
                        Box::new(BufWriter::with_capacity(buffer_size, writer))
                            as Box<dyn Write + Send>
                    })
                    .map_err(|e| ChapatyError::Io(IoError::WriterCreation(e.to_string())))
            }
            Self::Local { path } => {
                if !path.exists() {
                    std::fs::create_dir_all(path).map_err(|e| {
                        ChapatyError::Io(IoError::WriterCreation(format!(
                            "Failed to create directory {:?}: {}",
                            path, e
                        )))
                    })?;
                }

                let full_path = path.join(filename);
                std::fs::File::create(full_path)
                    .map(|file| {
                        Box::new(BufWriter::with_capacity(buffer_size, file))
                            as Box<dyn Write + Send>
                    })
                    .map_err(|e| ChapatyError::Io(IoError::WriterCreation(e.to_string())))
            }
            Self::HuggingFace { .. } => Err(ChapatyError::Io(IoError::WriterCreation("Writing directly to Hugging Face from environments is not supported. Use the upload CLI by Hugging Face.".to_string()))),
        }
    }

    /// Returns a reader and the file size in bytes.
    ///
    /// For local files, returns the exact file size.
    /// For cloud files, returns `None` if size cannot be determined.
    pub(crate) async fn reader_with_size(
        &self,
        filename: &str,
        buffer_size: usize,
    ) -> ChapatyResult<(Box<dyn Read + Send>, Option<u64>)> {
        match self {
            Self::Cloud { path, options } => {
                let full_path = format!("{path}/{filename}");
                let cloud_reader = CloudReader::new(&full_path, Some(options)).await?;
                Ok((
                    Box::new(BufReader::with_capacity(buffer_size, cloud_reader))
                        as Box<dyn Read + Send>,
                    None,
                ))
            }
            Self::Local { path } => {
                let full_path = path.join(filename);
                open_local_file(&full_path, buffer_size)
            }
            Self::HuggingFace { version } => {
                let revision = version
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| format!("v{}", crate::VERSION));

                let api = hf_hub::api::tokio::Api::new().map_err(|e| {
                    ChapatyError::Io(IoError::ReaderCreation(format!(
                        "Hugging Face API initialization failed: {e}"
                    )))
                })?;

                let repo = api.repo(hf_hub::Repo::with_revision(
                    "chapaty/environments".to_string(),
                    hf_hub::RepoType::Dataset,
                    revision,
                ));

                let cached_path = repo.get(filename).await.map_err(|e| {
                    ChapatyError::Io(IoError::ReadFailed(format!(
                        "Failed to fetch environment from Hugging Face: {e}"
                    )))
                })?;
                open_local_file(&cached_path, buffer_size)
            }
        }
    }
}

fn open_local_file(
    full_path: &Path,
    buffer_size: usize,
) -> ChapatyResult<(Box<dyn Read + Send>, Option<u64>)> {
    let metadata = std::fs::metadata(full_path)
        .map_err(|e| ChapatyError::Io(IoError::ReaderCreation(e.to_string())))?;
    let size = metadata.len();

    let file = std::fs::File::open(full_path)
        .map_err(|e| ChapatyError::Io(IoError::ReaderCreation(e.to_string())))?;

    Ok((
        Box::new(BufReader::with_capacity(buffer_size, file)) as Box<dyn Read + Send>,
        Some(size),
    ))
}

// ================================================================================================
// Serde Formats
// ================================================================================================

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Ord,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    IntoStaticStr,
    Default,
)]
#[strum(serialize_all = "lowercase")]
pub enum SerdeFormat {
    #[default]
    Postcard,
}
