// Intern crates
use crate::config::GCS_DATA_BUCKET;

// Extern crates
use google_cloud_storage::{
    client::Client,
    http::{
        objects::{
            delete::DeleteObjectRequest,
            download::Range,
            get::GetObjectRequest,
            list::ListObjectsRequest,
            upload::{Media, UploadObjectRequest, UploadType},
            Object,
        },
        Error,
    },
};
use std::path::PathBuf;

/// Wrapper to upload a file to Google Cloud Storage.
///
/// # Arguments
/// * `client` - Google Cloud Storage client
/// * `file_name` - File name of the bytes to be uploaded
/// * `bytes` - Content of the file
pub async fn upload_file(client: &Client, file_name: &PathBuf, bytes: Vec<u8>) {
    client
        .upload_object(
            &UploadObjectRequest {
                bucket: GCS_DATA_BUCKET.to_string(),

                ..Default::default()
            },
            bytes,
            &UploadType::Simple(Media {
                name: file_name.to_str().unwrap().to_string().into(),
                content_type: std::borrow::Cow::Borrowed("text/csv"),
                content_length: None,
            }),
        )
        .await.unwrap();
}

/// Wrapper to get a list of all files in a Google Cloud Storage bucket.
///
/// # Arguments
/// * `client` - Google Cloud Storage client
/// * `bucket` - bucket name
pub async fn get_files_in_bucket(client: &Client, bucket: &str) -> Vec<Object> {
    let mut lor = client
        .list_objects(&ListObjectsRequest {
            bucket: bucket.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut res = lor.items.unwrap();

    // Listen for more objects if the is some next_page_token
    while let Some(token) = lor.next_page_token {
        // Start new request
        lor = client
            .list_objects(&ListObjectsRequest {
                bucket: bucket.to_string(),
                page_token: Some(token),
                ..Default::default()
            })
            .await
            .unwrap();
        res.append(&mut lor.items.unwrap());
    }

    res
}

/// Wrapper to download a file from Google Cloud Storage.
///
/// # Arguments
/// * `client` - Google Cloud Storage client
/// * `file` - File we want to download
pub async fn download_file(client: &Client, file: &PathBuf) -> Result<Vec<u8>, Error> {
    client
        .download_object(
            &GetObjectRequest {
                bucket: "trust-data".to_string(),
                object: file.to_str().unwrap().to_string(),
                ..Default::default()
            },
            &Range::default(),
        )
        .await
}

/// Wrapper to delete a file in the Google Cloud Storage bucket.
///
/// # Arguments
/// * `client` - Google Cloud Storage client
/// * `file` - File to be deleted
pub async fn delete_file(client: &Client, file: &PathBuf) {
    client
        .delete_object(&DeleteObjectRequest {
            bucket: "trust-data".to_string(),
            object: file.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await.unwrap();
}
