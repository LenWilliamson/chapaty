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
    Local(&'a Path),
}

impl<'a> StorageLocation<'a> {
    pub(crate) async fn writer(
        &self,
        file_name: &str,
        buffer_size: usize,
    ) -> ChapatyResult<Box<dyn Write + Send>> {
        match self {
            Self::Cloud { path, options } => {
                let full_path = format!("{path}/{file_name}");
                BlockingCloudWriter::new(PlPathRef::new(&full_path), Some(options))
                    .await
                    .map(|writer| {
                        Box::new(BufWriter::with_capacity(buffer_size, writer))
                            as Box<dyn Write + Send>
                    })
                    .map_err(|e| ChapatyError::Io(IoError::WriterCreation(e.to_string())))
            }
            Self::Local(path) => {
                if !path.exists() {
                    std::fs::create_dir_all(path).map_err(|e| {
                        ChapatyError::Io(IoError::WriterCreation(format!(
                            "Failed to create directory {:?}: {}",
                            path, e
                        )))
                    })?;
                }

                let full_path = path.join(file_name);
                std::fs::File::create(full_path)
                    .map(|file| {
                        Box::new(BufWriter::with_capacity(buffer_size, file))
                            as Box<dyn Write + Send>
                    })
                    .map_err(|e| ChapatyError::Io(IoError::WriterCreation(e.to_string())))
            }
        }
    }

    /// Returns a reader and the file size in bytes.
    ///
    /// For local files, returns the exact file size.
    /// For cloud files, returns `None` if size cannot be determined.
    pub(crate) async fn reader_with_size(
        &self,
        file_name: &str,
        buffer_size: usize,
    ) -> ChapatyResult<(Box<dyn Read + Send>, Option<u64>)> {
        match self {
            Self::Cloud { path, options } => {
                let full_path = format!("{path}/{file_name}");
                let cloud_reader = CloudReader::new(&full_path, Some(options)).await?;
                Ok((
                    Box::new(BufReader::with_capacity(buffer_size, cloud_reader))
                        as Box<dyn Read + Send>,
                    None,
                ))
            }
            Self::Local(path) => {
                let full_path = path.join(file_name);
                let metadata = std::fs::metadata(&full_path)
                    .map_err(|e| ChapatyError::Io(IoError::ReaderCreation(e.to_string())))?;
                let size = metadata.len();

                let file = std::fs::File::open(full_path)
                    .map_err(|e| ChapatyError::Io(IoError::ReaderCreation(e.to_string())))?;

                Ok((
                    Box::new(BufReader::with_capacity(buffer_size, file)) as Box<dyn Read + Send>,
                    Some(size),
                ))
            }
        }
    }
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

impl SerdeFormat {
    pub fn from_path(path: &str) -> ChapatyResult<Self> {
        match path
            .rsplit_once('.')
            .ok_or_else(|| err(path, true))?
            .1
            .to_lowercase()
            .as_str()
        {
            "postcard" => Ok(Self::Postcard),
            ext => Err(err(ext, false)),
        }
    }
}

fn err(s: &str, missing_extension: bool) -> ChapatyError {
    let msg = if missing_extension {
        format!("Unsupported file format: missing or invalid extension in path '{s}'")
    } else {
        format!("Unsupported file format: '{s}'")
    };
    IoError::UnsupportedFormat(msg).into()
}
