 /// Google Cloud Storage bucket name
 pub static GCS_DATA_BUCKET: &str = "trust-data";

 use google_cloud_default::WithAuthExt;
 use google_cloud_storage::client::{Client, ClientConfig};

 pub async fn get_google_cloud_client() -> Client {
     let config = ClientConfig::default().with_auth().await.unwrap();
     Client::new(config)
 }