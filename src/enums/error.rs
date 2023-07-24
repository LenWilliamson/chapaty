use tokio::task::JoinError;

#[derive(Debug, Clone)]
pub enum ChapatyErrorKind {
    ParseBotError(String),
    ParseDataProducerError(String),
    BuildBotError(String),
    FailedToFetchDataFrameFromMap(String),
    FailedToJoinFuturesInProfitAndLossComputation(String),
    FileNotFound(String),
    UnknownGoogleCloudStorageError(String),
}

impl From<JoinError> for ChapatyErrorKind {
    fn from(value: JoinError) -> Self {
        ChapatyErrorKind::FailedToJoinFuturesInProfitAndLossComputation(value.to_string())
    }
}
