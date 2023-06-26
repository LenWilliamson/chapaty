// Subdirectories
pub mod _consumer;
pub mod backtester;

// Intern crates
use crate::{
    common::{finder::Finder, time_interval::TimeInterval},
    enums::{
        bots::BotKind,
        data::{LeafDir, RootDir},
        jobs::JobKind,
        markets::{GranularityKind, MarketKind},
    },
    producers::DataProducer,
    // utils::{
    //     profit_and_loss::{
    //         csv_schema::profit_and_loss, ProfitAndLoss, StopLoss, StopLossTrade, TakeProfit,
    //         TakeProfitTrade, Timeout,
    //     },
    //     trade::{PreTradeData, TradeData},
    // },
};

// Extern crates
use polars::frame::DataFrame;
use std::sync::{Arc, Mutex};

#[cfg(test)]
pub mod tests {

    use crate::{common::functions::df_from_file, config};
    use polars::prelude::{DataFrame, PolarsResult};

    /// This is a helper function for our unit tests to load the respective `.csv` files from the disk
    /// and returns the `.csv` as a `DataFrame`
    ///
    /// # Arguments
    /// * `base_path` - Path to the directory where the files are located
    /// * `which` - Which file we want to load
    pub async fn load_csv(base_path: &std::path::PathBuf, which: &str) -> PolarsResult<DataFrame> {
        let client = config::get_google_cloud_client().await;
        Ok(df_from_file(&base_path.join(which), None, None)
            .await
            .unwrap())
    }
}
