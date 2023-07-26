use crate::{
    bot::{indicator_data_pair::IndicatorDataPair, transformer::TransformerBuilder, Bot},
    chapaty,
    enums::{data::HdbSourceDirKind, error::ChapatyErrorKind, markets::MarketKind},
    serde::{deserialize::deserialize_data_frame_map, serialize::serialize_data_frame_map},
};
use google_cloud_storage::{
    client::Client,
    http::{
        objects::{
            download::Range,
            get::GetObjectRequest,
            list::ListObjectsRequest,
            upload::{Media, UploadObjectRequest, UploadType},
            Object,
        },
        Error,
    },
};
use polars::prelude::DataFrame;
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};
use std::sync::Arc;
use tokio::task::JoinHandle;

use super::{
    file_name_resolver::FileNameResolver, file_path_with_fallback::FilePathWithFallback,
    path_finder::PathFinderBuilder,
};

#[derive(Clone)]
pub struct CloudStorageClient {
    bot: Arc<Bot>,
    file_path_with_fallback: FilePathWithFallback,
    indicator_data_pair: Option<IndicatorDataPair>,
    simulation_data: HdbSourceDirKind,
    market: MarketKind,
    year: u32,
}

impl CloudStorageClient {
    pub async fn download_df_map(&self) -> chapaty::types::DataFrameMap {
        let bucket = self.bot.get_cached_data_bucket_name_ref();
        match self.try_download(bucket).await {
            Ok(v) => deserialize_data_frame_map(v),
            Err(e) => self.handle_chapaty_error(e).await,
        }
    }

    async fn handle_chapaty_error(&self, error: ChapatyErrorKind) -> chapaty::types::DataFrameMap {
        match error {
            ChapatyErrorKind::FileNotFound(_) => self.compute_df_map_from_hdb().await,
            _ => panic!("Cannot download df map. Execution is stopped, caused by: {error:?}"),
        }
    }

    async fn compute_df_map_from_hdb(&self) -> chapaty::types::DataFrameMap {
        let bucket = self.bot.get_historical_data_bucket_name_ref();
        let required_files = self.files_in_bucket(bucket).await;
        let required_data_frames = self.dfs_from_hdb_files(required_files).await;

        let transformer = TransformerBuilder::new(self.bot.clone())
            .with_indicator_data_pair(self.indicator_data_pair.clone())
            .with_market_sim_data(self.simulation_data)
            .with_market(self.market)
            .build();

        let df_map = transformer
            .transform_into_df_map(required_data_frames)
            .await;

        self.upload_df_map(df_map).await.unwrap()
        // df_map
    }

    fn upload_df_map(
        &self,
        df_map: chapaty::types::DataFrameMap,
    ) -> JoinHandle<chapaty::types::DataFrameMap> {
        let bot = self.bot.clone();
        let market = self.market;
        let year = self.year;
        let _self = Arc::new(self.clone());
        tokio::spawn(async move {
            let path_finder = PathFinderBuilder::new()
                .with_data_provider(bot.get_data_provider().get_data_producer_kind())
                .with_strategy(bot.get_strategy().get_bot_kind())
                .with_market(market)
                .with_year(year)
                .with_time_interval(*bot.get_time_interval_optional_ref())
                .with_time_frame(bot.get_time_frame_ref().to_string())
                .build();

            let file_name = _self.get_file_name_resolver().get_filename();
            let abs_file_path = path_finder.get_absolute_file_path(file_name);

            _self
                .cache_df_map_with_file_name(&df_map, abs_file_path)
                .await;
            df_map
        })
    }

    fn get_file_name_resolver(&self) -> FileNameResolver {
        let file_name_resolver = FileNameResolver::new(self.simulation_data);
        self.indicator_data_pair.clone().map_or_else(
            || file_name_resolver.clone(),
            |v| file_name_resolver.clone().with_indicator_data_pair(v),
        )
    }

    async fn cache_df_map_with_file_name(
        &self,
        df_map: &chapaty::types::DataFrameMap,
        file_name: String,
    ) {
        let bytes = serialize_data_frame_map(df_map).into_bytes();
        self.upload_to_cloud_storage(bytes, file_name).await;
    }

    async fn upload_to_cloud_storage(&self, bytes: Vec<u8>, file_name: String) {
        let upload_request = UploadObjectRequest {
            bucket: self.bot.get_cached_data_bucket_name_ref().to_string(),
            ..Default::default()
        };
        let upload_type = UploadType::Simple(Media {
            name: file_name.into(),
            content_type: std::borrow::Cow::Borrowed("application/json"),
            content_length: None,
        });

        let client = self.bot.get_client_ref();
        client
            .upload_object(&upload_request, bytes, &upload_type)
            .await
            .unwrap();
    }

    async fn dfs_from_hdb_files(&self, files: Vec<String>) -> Vec<DataFrame> {
        let bucket = self.bot.get_historical_data_bucket_name_owned();
        let client_builder: CloudStorageClientBuilder = self.clone().into();

        let tasks: Vec<_> = files
            .into_iter()
            .map(|file| {
                let bucket = bucket.clone();
                let fallback = self.file_path_with_fallback.get_fallback_ref().clone();
                let file_path_with_fallback = FilePathWithFallback::new(file, fallback);
                let client = client_builder
                    .clone()
                    .with_file_path_with_fallback(file_path_with_fallback)
                    .build();

                tokio::spawn(async move { client.try_download(&bucket).await.unwrap() })
            })
            .collect();

        let leaf_dir = self
            .indicator_data_pair
            .as_ref()
            .map_or_else(|| self.simulation_data, |v| v.data);

        futures::future::join_all(tasks)
            .await
            .into_par_iter()
            .map(Result::unwrap)
            .map(|df_as_bytes| {
                let dp = self.bot.get_data_provider();
                dp.get_df(df_as_bytes, &leaf_dir)
            })
            .collect()
    }

    pub async fn files_in_bucket(&self, bucket: &str) -> Vec<String> {
        let client = self.bot.get_client_ref();
        let files_in_bucket = get_files_in_bucket2(client, bucket).await.unwrap();

        files_in_bucket
            .into_iter()
            .filter(|x| {
                self.file_path_with_fallback
                    .get_fallback_ref()
                    .is_match(&x.name)
            })
            .map(|x| x.name)
            .collect()
    }

    pub async fn try_download(&self, bucket: &str) -> Result<Vec<u8>, ChapatyErrorKind> {
        let object = self.file_path_with_fallback.get_file_owned();
        let get_request = GetObjectRequest {
            bucket: bucket.to_string(),
            object: object.clone(),
            ..Default::default()
        };

        self.bot
            .get_client_ref()
            .download_object(&get_request, &Range::default())
            .await
            .map_err(|e| handle_google_cloud_error(e, object, bucket))
    }
}

#[derive(Clone)]
pub struct CloudStorageClientBuilder {
    bot: Arc<Bot>,
    file_path_with_fallback: Option<FilePathWithFallback>,
    indicator_data_pair: Option<IndicatorDataPair>,
    simulation_data: Option<HdbSourceDirKind>,
    market: Option<MarketKind>,
    year: Option<u32>,
}

impl From<CloudStorageClient> for CloudStorageClientBuilder {
    fn from(value: CloudStorageClient) -> Self {
        Self {
            bot: value.bot,
            file_path_with_fallback: Some(value.file_path_with_fallback),
            indicator_data_pair: value.indicator_data_pair,
            simulation_data: Some(value.simulation_data),
            market: Some(value.market),
            year: Some(value.year),
        }
    }
}

impl CloudStorageClientBuilder {
    pub fn new(bot: Arc<Bot>) -> Self {
        Self {
            bot,
            file_path_with_fallback: None,
            indicator_data_pair: None,
            simulation_data: None,
            market: None,
            year: None,
        }
    }

    pub fn with_file_path_with_fallback(
        self,
        file_path_with_fallback: FilePathWithFallback,
    ) -> Self {
        Self {
            file_path_with_fallback: Some(file_path_with_fallback),
            ..self
        }
    }

    pub fn with_simulation_data(self, simulation_data: HdbSourceDirKind) -> Self {
        Self {
            simulation_data: Some(simulation_data),
            ..self
        }
    }

    pub fn with_indicator_data_pair(self, indicator_data_pair: Option<IndicatorDataPair>) -> Self {
        Self {
            indicator_data_pair,
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn with_year(self, year: u32) -> Self {
        Self {
            year: Some(year),
            ..self
        }
    }

    pub fn build(self) -> CloudStorageClient {
        CloudStorageClient {
            bot: self.bot,
            file_path_with_fallback: self.file_path_with_fallback.unwrap(),
            simulation_data: self.simulation_data.unwrap(),
            indicator_data_pair: self.indicator_data_pair,
            market: self.market.unwrap(),
            year: self.year.unwrap(),
        }
    }
}

pub async fn get_files_in_bucket2(
    client: &Client,
    bucket: &str,
) -> Result<Vec<Object>, ChapatyErrorKind> {
    let mut lor = client
        .list_objects(&ListObjectsRequest {
            bucket: bucket.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| ChapatyErrorKind::UnknownGoogleCloudStorageError(e.to_string()))?;

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
        res.append(&mut lor.items.ok_or_else(|| {
            ChapatyErrorKind::UnknownGoogleCloudStorageError(
                "Invalid ListObjectsResponse".to_string(),
            )
        })?);
    }

    Ok(res)
}

fn handle_google_cloud_error(error: Error, file: String, bucket: &str) -> ChapatyErrorKind {
    if let Error::HttpClient(e) = &error {
        if is_file_not_found_error(e) {
            return ChapatyErrorKind::FileNotFound(format!(
                "{file} not found in cloud storage bucket <{bucket}>"
            ));
        }
    }
    ChapatyErrorKind::UnknownGoogleCloudStorageError(error.to_string())
}

fn is_file_not_found_error(error: &reqwest::Error) -> bool {
    let file_not_found_status_code = 404;
    error
        .status()
        .is_some_and(|s| s == file_not_found_status_code)
}
