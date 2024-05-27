use crate::{chapaty, config, serde::deserialize::deserialize_data_frame_map};
use google_cloud_storage::http::objects::{download::Range, get::GetObjectRequest};
use polars::{
    io::csv::read::CsvReadOptions,
    prelude::{DataFrame, SerReader},
};
use std::io::Cursor;

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
    CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(Cursor::new(bytes))
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
