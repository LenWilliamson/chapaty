use std::{collections::HashMap, sync::Arc};

use crate::{
    chapaty,
    enums::{
        bots::{PriceHistogram, TradingIndicatorKind},
        data::LeafDir,
        markets::{MarketKind, TimeFrame},
    },
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    price_histogram::{
        agg_trades_volume::AggTradesVolume, tick_volume::volume_profile_by_tick_data,
        tpo::TpoBuilder,
    }, data_frame_operations::is_not_an_empty_frame, converter::any_value::AnyValueConverter,
};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

use super::{
    indicator_data_pair::IndicatorDataPair, time_frame_snapshot::TimeFrameSnapshotBuilder, Bot,
};

pub struct Transformer {
    bot: Arc<Bot>,
    indicator_data_pair: Option<IndicatorDataPair>,
    market_sim_data: LeafDir,
    market: MarketKind,
}

impl Transformer {
    //create_chapaty_data_frame_map
    pub async fn transform_into_df_map(self, dfs: Vec<DataFrame>) -> chapaty::types::DataFrameMap {
        let (tx, rx) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let lazy_dfs: Vec<_> = dfs.into_iter().map(|df| df.lazy()).collect();
            let lazy_df = lazy_dfs.concatenate_to_lazy_frame();

            let _ = tx.send(self.build_df_map(lazy_df));
        });

        rx.await.unwrap()
    }

    fn build_df_map(&self, lazy_df: LazyFrame) -> chapaty::types::DataFrameMap {
        let time_frame = self.bot.get_time_frame_ref();
        let df_map = match time_frame {
            TimeFrame::Daily => self.compute_daily_df_map(lazy_df),
            TimeFrame::Weekly => self.compute_weekly_df_map(lazy_df),
        };

        match &self.indicator_data_pair {
            None => df_map,
            Some(pair) => self.trading_indicator_from_df_map(&pair.indicator, df_map),
        }
    }

    fn compute_daily_df_map(&self, lazy_df: LazyFrame) -> chapaty::types::DataFrameMap {
        let dp = self.bot.data_provider.clone();
        let time_interval = self.bot.time_interval;
        let time_frame = self.bot.time_frame;

        let ts_col = dp.get_ts_col_as_str(&self.market_sim_data).to_string();
        let mut ldf = lazy_df.add_cw_col(&ts_col).add_weekday_col(&ts_col);

        if time_interval.is_some() {
            ldf = ldf.filter_ts_col_by_time_interval(&ts_col, time_interval.unwrap(), time_frame);
        }

        let dfs = ldf
            .collect()
            .unwrap()
            .partition_by(["cw", "weekday"])
            .unwrap();

        self.populate_df_map(dfs)
    }

    fn compute_weekly_df_map(&self, lazy_df: LazyFrame) -> chapaty::types::DataFrameMap {
        let dp = self.bot.data_provider.clone();
        let time_interval = self.bot.time_interval;
        let time_frame = self.bot.time_frame;

        let ts_col = dp.get_ts_col_as_str(&self.market_sim_data).to_string();
        let mut ldf = lazy_df.add_cw_col(&ts_col);

        if time_interval.is_some() {
            ldf = ldf.filter_ts_col_by_time_interval(&ts_col, time_interval.unwrap(), time_frame);
        }

        let dfs: Vec<DataFrame> = ldf.collect().unwrap().partition_by(["cw"]).unwrap();

        self.populate_df_map(dfs)
    }

    fn populate_df_map(&self, dfs: Vec<DataFrame>) -> chapaty::types::DataFrameMap {
        dfs.into_iter().fold(HashMap::new(), |mut df_map, df| {
            if is_not_an_empty_frame(&df) {
                self.insert_df_into_df_map(df, &mut df_map);
            }
            df_map
        })
    }

    fn insert_df_into_df_map(&self, df: DataFrame, df_map: &mut chapaty::types::DataFrameMap) {
        match self.bot.time_frame {
            TimeFrame::Daily => handle_daily_update(df, df_map),
            TimeFrame::Weekly => handle_weekly_update(df, df_map),
        };
    }

    fn trading_indicator_from_df_map(
        &self,
        trading_indicator: &TradingIndicatorKind,
        df_map: chapaty::types::DataFrameMap,
    ) -> chapaty::types::DataFrameMap {
        match trading_indicator {
            TradingIndicatorKind::Poc(ph)
            | TradingIndicatorKind::VolumeAreaLow(ph)
            | TradingIndicatorKind::VolumeAreaHigh(ph) => self.handle_price_histogram(ph, df_map),
        }
    }

    fn handle_price_histogram(
        &self,
        price_histogram: &PriceHistogram,
        df_map: chapaty::types::DataFrameMap,
    ) -> chapaty::types::DataFrameMap {
        match price_histogram {
            PriceHistogram::Tpo1m => self.get_tpo(df_map),
            PriceHistogram::VolAggTrades => self.compute_vol_agg_trades(df_map),
            PriceHistogram::VolTick => self.compute_vol_tick(df_map),
        }
    }

    fn get_tpo(&self, df_map: chapaty::types::DataFrameMap) -> chapaty::types::DataFrameMap {
        let tpo = TpoBuilder::new()
            .with_data_provider(self.bot.data_provider.clone())
            .with_market(self.market)
            .build();

        tpo.from_df_map(df_map)
    }
    fn compute_vol_agg_trades(
        &self,
        df_map: chapaty::types::DataFrameMap,
    ) -> chapaty::types::DataFrameMap {
        AggTradesVolume::new(self.bot.data_provider.clone()).from_df_map(df_map)
    }

    fn compute_vol_tick(&self, _data: chapaty::types::DataFrameMap) -> chapaty::types::DataFrameMap {
        volume_profile_by_tick_data();
        HashMap::new()
    }
}

fn handle_daily_update(df: DataFrame, df_map: &mut chapaty::types::DataFrameMap) {
    let cw = get_df_map_key_of_current_df(&df, "cw");
    let wd = get_df_map_key_of_current_df(&df, "weekday");
    let snapshot = TimeFrameSnapshotBuilder::new(cw).with_weekday(wd).build();
    df_map.insert(snapshot, df);
}

fn get_df_map_key_of_current_df(df: &DataFrame, kind: &str) -> i64 {
    df.column(kind).unwrap().get(0).unwrap().unwrap_int64()
}

fn handle_weekly_update(df: DataFrame, df_map: &mut chapaty::types::DataFrameMap) {
    let cw = get_df_map_key_of_current_df(&df, "cw");
    let snapshot = TimeFrameSnapshotBuilder::new(cw).build();
    df_map.insert(snapshot, df);
}

pub struct TransformerBuilder {
    bot: Arc<Bot>,
    indicator_data_pair: Option<IndicatorDataPair>,
    market_sim_data: Option<LeafDir>,
    market: Option<MarketKind>,
}

impl TransformerBuilder {
    pub fn new(bot: Arc<Bot>) -> Self {
        Self {
            bot,
            indicator_data_pair: None,
            market_sim_data: None,
            market: None,
        }
    }

    pub fn with_indicator_data_pair(self, indicator_data_pair: Option<IndicatorDataPair>) -> Self {
        Self {
            indicator_data_pair,
            ..self
        }
    }

    pub fn with_market_sim_data(self, market_sim_data: LeafDir) -> Self {
        Self {
            market_sim_data: Some(market_sim_data),
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn build(self) -> Transformer {
        Transformer {
            bot: self.bot,
            indicator_data_pair: self.indicator_data_pair,
            market_sim_data: self.market_sim_data.unwrap(),
            market: self.market.unwrap(),
        }
    }
}
