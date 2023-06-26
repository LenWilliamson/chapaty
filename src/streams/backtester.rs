use std::{
    io::Cursor,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::Weekday;
use google_cloud_storage::client::Client;
use polars::{
    export::num::FromPrimitive,
    prelude::{CsvReader, CsvWriter, DataFrame, IntoLazy, SerReader, SerWriter, SortOptions},
};
use tokio::time::sleep;

use crate::{
    bots::Bot,
    common::{
        data_engine, finder::Finder, functions::load_dfs, time_interval::TimeInterval,
        wrappers::unwrap_int64,
    },
    config,
    //utils::profit_and_loss::csv_schema::profit_and_loss,
    enums::{
        bots::{BotKind, TradeDataKind},
        columns::{PerformanceStatisticColumnNames, ProfitAndLossColumnNames},
        data::{LeafDir, RootDir},
        jobs::JobKind,
        markets::{GranularityKind, MarketKind},
        trades::TradeKind,
    },
    producers::{
        performance_report::{self, generate_performance_report},
        profit_loss_report::{self, generate_profit_loss_report},
        DataProducer,
    },
    streams,
};

pub struct Backtester {
    pub dp: Arc<dyn DataProducer + Send + Sync>,
    pub years: Vec<u32>,
    pub market: Vec<MarketKind>,
    pub granularity: Vec<GranularityKind>,
    pub ti: Option<TimeInterval>,
}

impl Backtester {
    pub async fn backtest_bot(
        &self,
        bot: Arc<dyn Bot + Send + Sync>,
        pl_only: bool,
        vol_data: LeafDir,
        ohlc_data: LeafDir,
        pl_file_name: String,
    ) {
        // Initialize data
        // TODO fix
        let client = Arc::new(config::get_google_cloud_client().await);

        let time_interval = if let Some(ti) = self.ti {
            ti
        } else {
            TimeInterval {
                start_day: chrono::Weekday::Mon,
                start_h: 1,
                end_day: chrono::Weekday::Fri,
                end_h: 23,
            }
        };

        // clean directories
        self.clean_directories(
            client.clone(),
            bot.get_bot_kind(),
            pl_only,
            vol_data,
            ohlc_data,
        )
        .await;

        // prepare data
        if !pl_only {
            self.prepare_data(bot.clone(), vol_data, ohlc_data, time_interval)
                .await;
        }

        // compute profit and loss
        self.compute_profit_and_loss(bot.clone(), LeafDir::Vol, ohlc_data, pl_file_name)
            .await;
    }

    async fn clean_directories(
        &self,
        client: Arc<Client>,
        bot_kind: BotKind,
        pl_only: bool,
        vol_data: LeafDir,
        ohlc_data: LeafDir,
    ) {
        // BEGIN: Delete files
        // Stop time for reporting
        let start = Instant::now();

        // +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++
        // +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++
        // +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++
        // +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++ + +++++++++++++++++++++++++++++++++++++++++

        let mut tasks: Vec<_> = Vec::new();
        for year in &self.years {
            // Initialize finder and client_handle
            let finder = Finder::new(
                PathBuf::from("trust-data"),
                crate::enums::producers::ProducerKind::Ninja,
                self.market[0],
                year.clone(),
                bot_kind,
                self.granularity[0],
            )
            .await;

            let client_handle = client.clone();
            let task = tokio::spawn(async move {
                // Always delete current `pl.csv` because we are going to compute a new `.csv` file that reports the P&L
                let start = Instant::now();
                finder
                    .delete_files(
                        client_handle.clone(),
                        RootDir::Strategy,
                        LeafDir::ProfitAndLoss,
                    )
                    .await;
                let duration = start.elapsed();
                println!(
                    "Time elapsed in deleting ProfitAndLoss directory in RootDir::Strategy is: {duration:?}"
                );

                // We want to compute the P&L with new data. Hence, we have to delte all the files that are comptuted
                // from the old data
                if !pl_only {
                    // 1. Always delete `cw` directory: "bucket"/data/{producer}/{market}/{year}/{aggTrades | ohlc-{ts} | ...}/cw
                    let start = Instant::now();
                    finder
                        .delete_files(client_handle.clone(), RootDir::Data, ohlc_data)
                        .await;
                    let duration = start.elapsed();
                    println!("Time elapsed in deleting {ohlc_data:?}/cw directory in RootDir::Data is: {duration:?}");

                    let start = Instant::now();
                    finder
                        .delete_files(client_handle.clone(), RootDir::Data, vol_data)
                        .await;
                    let duration = start.elapsed();
                    println!(
                        "Time elapsed in deleting {vol_data:?}/cw directory in RootDir::Data is: {duration:?}"
                    );

                    // 2. Always delete "bucket"/strategy/{bot}/{market}/{year}/{granularity}/vol
                    let start = Instant::now();
                    finder
                        .delete_files(client_handle.clone(), RootDir::Strategy, LeafDir::Vol)
                        .await;
                    let duration = start.elapsed();
                    println!(
                        "Time elapsed in deleting Vol directory in RootDir::Strategy is: {duration:?}"
                    );

                    // 3. Upon configuration delete:
                    // 3.1. All files we used to compute the volume profile for
                    //      - ohlc-{ts}: If we computed TPO-Profile "bucket"/strategy/{bot}/{market}/{year}/{granularity}/ohlc-{ts}
                    //      - tick | aggTrades: If we computed Volume Profile "bucket"/strategy/{bot}/{market}/{year}/{granularity}/{tick | aggTrades}
                    let start = Instant::now();
                    finder
                        .delete_files(client_handle.clone(), RootDir::Strategy, vol_data)
                        .await;
                    let duration = start.elapsed();
                    println!(
                            "Time elapsed in deleting {vol_data:?} directory in RootDir::Strategy is: {duration:?}"
                        );
                    // 3.2. All OHLC files we used to run our analysis on
                    //      - "bucket"/strategy/{bot}/{market}/{year}/{granularity}/ohlc-{ts}
                    let start = Instant::now();
                    finder
                        .delete_files(client_handle.clone(), RootDir::Strategy, ohlc_data)
                        .await;
                    let duration = start.elapsed();
                    println!("Time elapsed in deleting {ohlc_data:?} directory in RootDir::Strategy is: {duration:?}");
                }
            });
            tasks.push(task);
        }

        // +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++
        // +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++
        // +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++
        // +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++ +++++++++++++++++++++++++++++++++++++++++

        // // Iterate over years
        // let tasks: Vec<_> = self.years.iter().map(|year| {
        //     // Initialize finder and client_handle
        //     let finder = Finder::new(
        //         PathBuf::from("trust-data"),
        //         crate::enums::producers::ProducerKind::Ninja,
        //         self.market[0],
        //         year.clone(),
        //         bot_kind,
        //         self.granularity[0],
        //     ).await;

        //     let client_handle = client.clone();
        //     tokio::spawn(async move {
        //         // Always delete current `pl.csv` because we are going to compute a new `.csv` file that reports the P&L
        //         let start = Instant::now();
        //         finder
        //             .delete_files(
        //                 client_handle.clone(),
        //                 RootDir::Strategy,
        //                 LeafDir::ProfitAndLoss,
        //             )
        //             .await;
        //         let duration = start.elapsed();
        //         println!(
        //             "Time elapsed in deleting ProfitAndLoss directory in RootDir::Strategy is: {duration:?}"
        //         );

        //         // We want to compute the P&L with new data. Hence, we have to delte all the files that are comptuted
        //         // from the old data
        //         if ! pl_only {
        //             // 1. Always delete `cw` directory: "bucket"/data/{producer}/{market}/{year}/{aggTrades | ohlc-{ts} | ...}/cw
        //             let start = Instant::now();
        //             finder
        //                 .delete_files(client_handle.clone(), RootDir::Data, ohlc_data)
        //                 .await;
        //             let duration = start.elapsed();
        //             println!("Time elapsed in deleting {ohlc_data:?}/cw directory in RootDir::Data is: {duration:?}");

        //             let start = Instant::now();
        //             finder
        //                 .delete_files(client_handle.clone(), RootDir::Data, vol_data)
        //                 .await;
        //             let duration = start.elapsed();
        //             println!(
        //                 "Time elapsed in deleting {vol_data:?}/cw directory in RootDir::Data is: {duration:?}"
        //             );

        //             // 2. Always delete "bucket"/strategy/{bot}/{market}/{year}/{granularity}/vol
        //             let start = Instant::now();
        //             finder
        //                 .delete_files(client_handle.clone(), RootDir::Strategy, LeafDir::Vol)
        //                 .await;
        //             let duration = start.elapsed();
        //             println!(
        //                 "Time elapsed in deleting Vol directory in RootDir::Strategy is: {duration:?}"
        //             );

        //             // 3. Upon configuration delete:
        //             // 3.1. All files we used to compute the volume profile for
        //             //      - ohlc-{ts}: If we computed TPO-Profile "bucket"/strategy/{bot}/{market}/{year}/{granularity}/ohlc-{ts}
        //             //      - tick | aggTrades: If we computed Volume Profile "bucket"/strategy/{bot}/{market}/{year}/{granularity}/{tick | aggTrades}
        //             let start = Instant::now();
        //             finder
        //                 .delete_files(client_handle.clone(), RootDir::Strategy, vol_data)
        //                 .await;
        //             let duration = start.elapsed();
        //             println!(
        //                     "Time elapsed in deleting {vol_data:?} directory in RootDir::Strategy is: {duration:?}"
        //                 );
        //             // 3.2. All OHLC files we used to run our analysis on
        //             //      - "bucket"/strategy/{bot}/{market}/{year}/{granularity}/ohlc-{ts}
        //             let start = Instant::now();
        //             finder
        //                 .delete_files(client_handle.clone(), RootDir::Strategy, ohlc_data)
        //                 .await;
        //             let duration = start.elapsed();
        //             println!("Time elapsed in deleting {ohlc_data:?} directory in RootDir::Strategy is: {duration:?}");
        //         }
        //     })
        // })
        // .collect();

        for task in tasks {
            task.await.unwrap();
        }

        // Print the duration of this operation to the console
        let duration = start.elapsed();
        println!("Time elapsed in deleting all files is: {duration:?}");
    }

    async fn prepare_data(
        &self,
        bot: Arc<dyn Bot + Send + Sync>,
        vol_data: LeafDir,
        ohlc_data: LeafDir,
        time_interval: TimeInterval,
    ) {
        let start = Instant::now();

        // Initialize handles
        let ti_handle = Arc::new(time_interval);
        let bot_handle = bot.clone();
        let market = self.market[0];
        let granularity = self.granularity[0];

        let tasks: Vec<_> = self
            .years
            .clone()
            .into_iter()
            .map(|year| {
                let time_interval = ti_handle.clone();
                let dp_handle = self.dp.clone();
                let bot = bot_handle.clone();

                tokio::spawn(async move {
                    // Compute volume profile
                    let start = Instant::now();
                    streams::_consumer::process_batch(
                        dp_handle.clone(),
                        year,
                        bot.get_bot_kind(),
                        market,
                        granularity,
                        vol_data,
                        *time_interval,
                        vol_data,
                        JobKind::Volume,
                    )
                    .await;
                    // .unwrap();
                    let duration = start.elapsed();
                    println!(
                        "Time elapsed in computing volume profile for {year} is: {duration:?}"
                    );

                    // Prepare OHLC data
                    let start = Instant::now();
                    streams::_consumer::process_batch(
                        dp_handle.clone(),
                        year,
                        bot.get_bot_kind(),
                        market,
                        granularity,
                        ohlc_data,
                        *time_interval,
                        ohlc_data,
                        JobKind::Chart,
                    )
                    .await;
                    // .unwrap();
                    let duration = start.elapsed();
                    println!(
                        "Time elapsed in computing ohlc market data for {year} is: {duration:?}"
                    );

                    // Sleep to be sure file is uploaded
                    println!("Sleeping for 30sec");
                    sleep(Duration::from_millis(30000)).await;
                    println!("30sec has elapsed");
                })
            })
            .collect();

        for task in tasks {
            task.await.unwrap();
        }

        // Print the duration of this operation to the console
        let duration = start.elapsed();
        let bot_kind = bot.get_bot_kind();
        println!("Time elapsed in backtesting the {bot_kind:?} bot is: {duration:?}");
        // END: Backtesting
    }

    async fn compute_profit_and_loss(
        &self,
        bot: Arc<dyn Bot + Send + Sync>,
        vol_data: LeafDir,
        ohlc_data: LeafDir,
        pl_file_name: String,
    ) {
        // BEGIN: Backtesting
        // Stop time for reporting
        let start = Instant::now();

        // Initialize handles
        let file_name_handle = Arc::new(pl_file_name);
        let bot_handle = bot.clone();

        let bytes: Vec<u8> = Vec::new();
        let reader = CsvReader::new(Cursor::new(bytes));
        let performance_report = Arc::new(Mutex::new(
            reader
                .has_header(false)
                .with_schema(Arc::new(performance_report::schema()))
                .finish()
                .unwrap(),
        ));

        let tasks: Vec<_> = self
            .years
            .clone()
            .into_iter()
            .map(|year| {
                let dp_handle = self.dp.clone();
                let pl_file_name = file_name_handle.clone();
                let bot = bot_handle.clone();
                let market = self.market[0];
                let granularity = self.granularity[0];
                let performance_report = performance_report.clone();

                tokio::spawn(async move {
                    let start = Instant::now();
                    let pl = eval_strategy(
                        // BEGIN: Replacement of self
                        dp_handle.clone(),
                        market,
                        granularity,
                        // END: Replacement of self
                        bot.clone(),
                        year,
                        ohlc_data,
                        vol_data,
                        (*pl_file_name).clone(),
                    )
                    .await;

                    let duration = start.elapsed();
                    println!("Time elapsed in computing PL for {year} is: {duration:?}");

                    let perf_report =
                        generate_performance_report(pl, year, bot.get_bot_kind(), market);
                    let mut df_performance_report = performance_report.lock().unwrap();
                    *df_performance_report = df_performance_report.vstack(&perf_report).unwrap();
                })
            })
            .collect();

        for task in tasks {
            task.await.unwrap();
        }

        // https://stackoverflow.com/questions/70333509/move-var-out-from-arcmutexvar
        // I'm positively sure there is no other strong reference, this is the last `Arc`. If not, let it panic.
        let mutex = Arc::try_unwrap(performance_report).unwrap();
        let mut performance_report = mutex.into_inner().unwrap();

        let year = PerformanceStatisticColumnNames::Year.to_string();
        performance_report = performance_report
            .lazy()
            .sort(
                &year,
                SortOptions {
                    descending: true,
                    ..Default::default()
                },
            )
            .collect()
            .unwrap();

        // Save file locally
        let mut file = std::fs::File::create(format!("out_performance_report.csv")).unwrap();
        CsvWriter::new(&mut file)
            .finish(&mut performance_report)
            .unwrap();

        // Save file in cloud
        let finder = Finder::new(
            self.dp.get_bucket_name(),
            self.dp.get_data_producer_kind(),
            self.market[0], // TODO fix, if we have multiple markets
            0,
            bot.get_bot_kind(),
            self.granularity[0], // TODO fix, if we have multiple granularities
        )
        .await;

        finder
            .save_performance_report(
                finder.get_client_clone(),
                "performance_report.csv".to_string(),
                performance_report,
            )
            .await;

        // Print the duration of this operation to the console
        let duration = start.elapsed();
        let bot_kind = bot.get_bot_kind();
        println!("Time elapsed in backtesting the {bot_kind:?} bot is: {duration:?}");
        // END: Backtesting
    }
}

async fn eval_strategy(
    // &self,
    // BEGIN: Replacement of self
    dp: Arc<dyn DataProducer + Send + Sync>,
    market: MarketKind,
    granularity: GranularityKind,
    // END: Replacement of self
    bot: Arc<dyn Bot + Send + Sync>,
    year: u32,
    ohlc_dir: LeafDir,
    vol_dir: LeafDir,
    pl_file_name: String,
) -> DataFrame {
    let finder = Finder::new(
        dp.get_bucket_name(),
        dp.get_data_producer_kind(),
        market,
        year,
        bot.get_bot_kind(),
        granularity,
    )
    .await;

    // Call subroutine
    let mut pl = compute(dp.clone(), bot, Arc::new(finder.clone()), ohlc_dir, vol_dir).await;

    // Save file locally
    let mut file = std::fs::File::create(format!("out_{year}.csv")).unwrap();
    CsvWriter::new(&mut file).finish(&mut pl).unwrap();

    finder
        .save_file(
            finder.get_client_clone(),
            RootDir::Strategy,
            LeafDir::ProfitAndLoss,
            pl_file_name,
            pl.clone(),
            None,
            None,
        )
        .await;

    pl
}

async fn compute(
    // &self,
    // BEGIN: Replacement of self

    // END: Replacement of self
    dp: Arc<dyn DataProducer + Send + Sync>,
    bot: Arc<dyn Bot + Send + Sync>,
    finder: Arc<Finder>,
    ohlc_dir: LeafDir,
    vol_dir: LeafDir,
) -> DataFrame {
    let bytes: Vec<u8> = Vec::new();
    let reader = CsvReader::new(Cursor::new(bytes));
    let mut d = reader
        .has_header(false)
        .with_schema(Arc::new(profit_loss_report::schema()))
        .finish()
        .unwrap();

    // Get market
    let market = load_dfs(dp.clone(), finder.clone(), RootDir::Strategy, ohlc_dir).await;
    // Get vol
    let vol = load_dfs(dp.clone(), finder.clone(), RootDir::Strategy, vol_dir).await;

    let bot_handle = bot.clone();
    let dp_handle = dp.clone();

    for cw in 2..53_i64 {
        let mut df_ohlc_cur = DataFrame::default();
        let mut df_ohlc_prev = DataFrame::default();
        let mut df_vol_prev = DataFrame::default();
        match finder.get_granularity() {
            GranularityKind::Daily => {
                for wd in 1..=5 {
                    if wd == 1 {
                        df_ohlc_prev = if let Some(data) = market.get(&(cw - 1, Some(5))) {
                            data.clone()
                        } else {
                            continue;
                        };
                        df_vol_prev = if let Some(data) = vol.get(&(cw - 1, Some(5))) {
                            data.clone()
                        } else {
                            continue;
                        };
                    } else {
                        df_ohlc_prev = if let Some(data) = market.get(&(cw, Some(wd - 1))) {
                            data.clone()
                        } else {
                            continue;
                        };
                        df_vol_prev = if let Some(data) = vol.get(&(cw, Some(wd - 1))) {
                            data.clone()
                        } else {
                            continue;
                        };
                    }
                    df_ohlc_cur = if let Some(data) = market.get(&(cw, Some(wd))) {
                        data.clone()
                    } else {
                        continue;
                    };

                    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                    let bot = bot_handle.clone();
                    let dp = dp_handle.clone();
                    let pre_trade_data_request = bot.register_pre_trade_data();
                    let pre_trade_data_map = data_engine::compute_pre_trade_data(
                        dp.clone(),
                        df_ohlc_prev,
                        df_vol_prev,
                        pre_trade_data_request,
                    );
                    let entry_price = bot.get_entry_price(&pre_trade_data_map);
                    let trade_kind = bot.get_trade_kind(&pre_trade_data_map);
                    let trade_data_map = if let Some(_) = trade_kind {
                        data_engine::compute_trade_data(
                            dp.clone(),
                            df_ohlc_cur.clone(),
                            entry_price,
                        )
                    } else {
                        None
                    };

                    let p_and_l = if let Some(trade) = &trade_data_map {
                        Some(data_engine::compute_profit_and_loss(
                            dp,
                            bot.clone(),
                            df_ohlc_cur,
                            &pre_trade_data_map,
                            trade,
                        ))
                    } else {
                        None
                    };

                    let entry_ts = if let Some(trade) = &trade_data_map {
                        Some(unwrap_int64(&trade[TradeDataKind::EntryTimestamp]))
                    } else {
                        None
                    };

                    // Write P&L statement
                    let (cw, day) = match finder.get_granularity() {
                        GranularityKind::Weekly => (cw, Weekday::Mon),
                        // If Daily, we store {cw}{day}, where day is just one digit
                        GranularityKind::Daily => (cw, chrono::Weekday::from_i64(wd - 1).unwrap()),
                    };

                    let trade_kind = match bot.get_trade_kind(&pre_trade_data_map) {
                        Some(trade) => trade,
                        None => TradeKind::None,
                    };

                    let df = generate_profit_loss_report(
                        &finder,
                        cw,
                        day,
                        bot.get_entry_price(&pre_trade_data_map),
                        &entry_ts,
                        &trade_kind,
                        &p_and_l,
                    );
                    d = d.vstack(&df).unwrap();
                    d.align_chunks();

                    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                }
            }
            GranularityKind::Weekly => {
                df_ohlc_cur = if let Some(data) = market.get(&(cw, None)) {
                    data.clone()
                } else {
                    continue;
                };
                df_ohlc_prev = if let Some(data) = market.get(&(cw - 1, None)) {
                    data.clone()
                } else {
                    continue;
                };
                df_vol_prev = if let Some(data) = vol.get(&(cw - 1, None)) {
                    data.clone()
                } else {
                    continue;
                };

                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                let bot = bot_handle.clone();
                let dp = dp_handle.clone();
                let pre_trade_data_request = bot.register_pre_trade_data();
                let pre_trade_data_map = data_engine::compute_pre_trade_data(
                    dp.clone(),
                    df_ohlc_prev,
                    df_vol_prev,
                    pre_trade_data_request,
                );
                let entry_price = bot.get_entry_price(&pre_trade_data_map);
                let trade_kind = bot.get_trade_kind(&pre_trade_data_map);
                let trade_data_map = if let Some(_) = trade_kind {
                    data_engine::compute_trade_data(dp.clone(), df_ohlc_cur.clone(), entry_price)
                } else {
                    None
                };

                let p_and_l = if let Some(trade) = &trade_data_map {
                    Some(data_engine::compute_profit_and_loss(
                        dp,
                        bot.clone(),
                        df_ohlc_cur,
                        &pre_trade_data_map,
                        trade,
                    ))
                } else {
                    None
                };

                let entry_ts = if let Some(trade) = &trade_data_map {
                    Some(unwrap_int64(&trade[TradeDataKind::EntryTimestamp]))
                } else {
                    None
                };

                // Write P&L statement
                let (cw, day) = match finder.get_granularity() {
                    GranularityKind::Weekly => (cw, Weekday::Mon),
                    // If Daily, we store {cw}{day}, where day is just one digit
                    GranularityKind::Daily => {
                        panic!("Shouldn't be here, because we are in the Weekly arm of the above match statement");
                    }
                };

                let trade_kind = match bot.get_trade_kind(&pre_trade_data_map) {
                    Some(trade) => trade,
                    None => TradeKind::None,
                };

                let df = generate_profit_loss_report(
                    &finder,
                    cw,
                    day,
                    bot.get_entry_price(&pre_trade_data_map),
                    &entry_ts,
                    &trade_kind,
                    &p_and_l,
                );
                d = d.vstack(&df).unwrap();
                d.align_chunks();

                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            }
        }
    }

    // https://stackoverflow.com/questions/70333509/move-var-out-from-arcmutexvar
    // I'm positively sure there is no other strong reference, this is the last `Arc`. If not, let it panic.
    // let mutex = Arc::try_unwrap(df_handle).unwrap();
    // let mut d = mutex.into_inner().unwrap();

    d.align_chunks();

    // Sort by date
    let date = ProfitAndLossColumnNames::Date.to_string();
    d.lazy().sort(&date, Default::default()).collect().unwrap()
}

#[cfg(test)]
mod test {
    use chrono::NaiveDateTime;
    use google_cloud_storage::client::ClientConfig;

    use crate::{
        bots::ppp::Ppp,
        config::GCS_DATA_BUCKET,
        enums::strategies::{StopLossKind, TakeProfitKind},
        producers::{
            profit_loss_report::{StopLoss, TakeProfit},
            test::Test,
        },
    };

    use super::*;
    use google_cloud_default::WithAuthExt;

    #[tokio::test]
    async fn test_compute() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> =
            Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

        let finder: Finder = Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            MarketKind::BtcUsdt,
            2022,
            BotKind::Ppp,
            GranularityKind::Weekly,
        )
        .await;
        let sl = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 494.06,
        };

        let tp = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 1_000.00,
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl);
        bot.set_take_profit(tp);

        compute(
            dp,
            Arc::new(bot),
            Arc::new(finder),
            LeafDir::Ohlcv60m,
            LeafDir::Vol,
        )
        .await;

        let dt = NaiveDateTime::from_timestamp_opt(1646035200000 / 1000, 0).unwrap();
        assert_eq!(
            dbg!(format!("{}", dt.format("%Y-%m-%d %H:%M:%S"))),
            "2022-02-28 08:00:00"
        );
    }
}
