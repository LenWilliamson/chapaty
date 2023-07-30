use std::io::Cursor;

use google_cloud_storage::http::objects::{download::Range, get::GetObjectRequest};
use polars::prelude::{CsvReader, DataFrame, SerReader};

use crate::{chapaty, config, serde::deserialize::deserialize_data_frame_map};

#[allow(dead_code)]
pub async fn download_df(bucket: String, abs_file_path: String) -> DataFrame {
    let client = config::get_google_cloud_storage_client().await;
    let req = GetObjectRequest {
        bucket: bucket,
        object: abs_file_path,
        ..Default::default()
    };

    let bytes = client
        .download_object(&req, &Range::default())
        .await
        .unwrap();
    CsvReader::new(Cursor::new(bytes))
        .has_header(true)
        .finish()
        .unwrap()
}

#[allow(dead_code)]
pub async fn download_df_as_bytes(bucket: String, abs_file_path: String) -> Vec<u8> {
    let client = config::get_google_cloud_storage_client().await;
    let req = GetObjectRequest {
        bucket: bucket,
        object: abs_file_path,
        ..Default::default()
    };

    client
        .download_object(&req, &Range::default())
        .await
        .unwrap()
}

#[allow(dead_code)]
pub async fn download_df_map(abs_file_path: String) -> chapaty::types::DataFrameMap {
    let client = config::get_google_cloud_storage_client().await;
    let req = GetObjectRequest {
        bucket: "chapaty-ai-test".to_string(),
        object: abs_file_path,
        ..Default::default()
    };

    let bytes = client
        .download_object(&req, &Range::default())
        .await
        .unwrap();
    deserialize_data_frame_map(bytes)
}
