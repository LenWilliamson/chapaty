 use google_cloud_storage::client::{Client, ClientConfig};

 pub async fn get_google_cloud_client() -> Client {
     let config = ClientConfig::default().with_auth().await.unwrap();
     Client::new(config)
 }

#[derive(Clone)]
 pub struct GoogleCloudBucket {
    pub historical_market_data_bucket_name: String,
    pub cached_bot_data_bucket_name: String
 }