// Internal crates
use crate::{
    common::wrappers::unwrap_float64,
    enums::{
        columns::{AggTradeColumnNames, Columns, OhlcColumnNames, VolumeProfileColumnNames},
        data::LeafDir,
    },
    producers::DataProducer,
};

// External crates
use polars::{
    lazy::dsl::GetOutput,
    prelude::{
        col, df, DataFrame, Float64Chunked, IntoLazy, IntoSeries, NamedFrom,
        PolarsResult as Result, Series,
    },
};
use std::{collections::HashMap, sync::Arc};

/// This function computes the volume profile or the time price opportunity for the given DataFrame and sorts it by price in ascending order.
///
/// # Arguments
/// * `dp` - producer of type `DataProducer`
/// * `df` - the `DataFrame` we want to compute the volume profile for
/// * `tpo_or_vol` - determines if we compute the volume profile or the time price opportuinity
/// * `exact` - is `true` if we don't want to round floats to their closest integer values, otherwise `false`
/// * `max_digits` - accuracy of available trade price.
///    * BtcUsdt (two digits for cents, e.g. 1258.33)
///    * 6e (five digits for ticks, e.g. 1.39450)
///
/// # Note
/// * We can only compute the volume_profile for `LeafDir::Tick | LeafDir::AggTrades`
/// * We can only compute the tpo for `LeafDir::Ohlc(_) | LeafDir::Ohlcv(_)`
pub async fn volume_profile(
    dp: Arc<dyn DataProducer + Send + Sync>,
    df: DataFrame,
    tpo_or_vol: LeafDir,
    exact: bool,
    max_digits: i32,
) -> Result<DataFrame> {
    match tpo_or_vol {
        LeafDir::Tick | LeafDir::AggTrades => vol_profile(dp, df, exact).await,
        LeafDir::Ohlc1m
        | LeafDir::Ohlc30m
        | LeafDir::Ohlc60m
        | LeafDir::Ohlcv1m
        | LeafDir::Ohlcv30m
        | LeafDir::Ohlcv60m => tpo(dp, df, max_digits).await,
        _ => panic!("Cannot compute volume profile for {tpo_or_vol:?}"),
    }
}

/// This function computes the volume profile for the given DataFrame and sorts it by price in ascending order.
/// The values for the price columns are rounded.
///
/// We have many calls to this function, hence we:
/// * `rayon::spawn` a thread for each call and inside this thread we compute result
/// * See: https://ryhl.io/blog/async-what-is-blocking/
///
/// # Arguments
/// * `dp` - producer of type `DataProducer`
/// * `df` - the `DataFrame` we want to compute the volume profile for
/// * `exact` - is `true` if we don't want to round floats to their closest integer values, otherwise `false`
///
/// # Example
/// Assume `px` and `qx` are some column names.
/// Calling this function on the following DataFrame will result into the target.
/// ```
/// let df = df!(
///    &px => &[1.0, 1.4, 2.49, 2.5, 3.1],
///    &qx => &[1.0, 1.0,  2.0, 3.0, 3.0],
/// );
///
///
/// let target = df!(
///     &px => &[1.0, 2.0, 3.0],
///     &qx => &[2.0, 2.0, 6.0],
/// );
///
/// // Calling function to compute volume_profile
/// let result = tick::volume_profile(df.unwrap()).unwrap();
/// assert_eq!(result.frame_equal(&target.unwrap()), true)
///
/// ```
async fn vol_profile(
    dp: Arc<dyn DataProducer + Send + Sync>,
    df: DataFrame,
    exact: bool,
) -> Result<DataFrame> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        // Get column name of the respective price (px) and quantity (qx) column
        let px = dp.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Price));
        let qx = dp.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Quantity));
        let px_vol = dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Price));
        let qx_vol = dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Quantity));

        // If we don't want to use exact values, then round float to closest integer
        if !exact {
            let result = df
                .lazy()
                .select([
                    col(&px).apply(|x| Ok(Some(round(&x))), GetOutput::default()),
                    col(&qx),
                ])
                .groupby([col(&px_vol)])
                .agg([col(&qx_vol).sum()])
                .collect()
                .unwrap();

            // Return result
            let _ = tx.send(result.sort([px], false));
        } else {
            let result = df
                .lazy()
                .select([col(&px), col(&qx)])
                .groupby([col(&px_vol)])
                .agg([col(&qx_vol).sum()])
                .collect()
                .unwrap();

            // Return result
            let _ = tx.send(result.sort([px], false));
        }
    });
    rx.await.expect("Panic in rayon::spawn")
}

/// TODO wrong step size. For 6E we move in 0.00005 steps and not 0.00001 steps.
///
/// This function computes the market profile described in https://www.vtad.de/lexikon/market-profile/,
/// for the given DataFrame and sorts it by price in ascending order. The values for the price columns are not rounded.
///
/// We have many calls to this function, hence we:
/// * `rayon::spawn` a thread for each call and inside this thread we compute result
/// * See: https://ryhl.io/blog/async-what-is-blocking/
///
/// # Arguments
/// * `df` - DataFrame we want to compute the volume profile for
/// * `max_digits` - accuracy of available trade price.
///    * BtcUsdt (two digits for cents, e.g. 1258.33)
///    * 6e (five digits for ticks, e.g. 1.39450)
/// ```
async fn tpo(
    dp: Arc<dyn DataProducer + Send + Sync>,
    df: DataFrame,
    max_digits: i32,
) -> Result<DataFrame> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        // Get index of respective columns in `DataFrame`
        let high_idx = dp.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::High));
        let low_idx = dp.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Low));

        // Get a reference to the respective columns
        let highs = &df.get_columns()[high_idx];
        let lows = &df.get_columns()[low_idx];

        // Create a `Hashmap` to compute the time price opportunities (tpos)
        let mut tpos = HashMap::<i32, (f64, f64)>::new();

        std::iter::zip(highs.iter(), lows.iter()).for_each(|(highw, loww)| {
            // Unwrap Anyvalue h
            let h = unwrap_float64(&highw);
            // Unwrap Anyvalue l
            let l = unwrap_float64(&loww);

            // Multiply h, l * 10^max-digits and transform to i32
            let high = (h * 10.0_f64.powi(max_digits)) as i32;
            let low = (l * 10.0_f64.powi(max_digits)) as i32;

            // Iterate for x in low..=high and add to hashmap
            // TODO https://docs.rs/ordered-float/latest/ordered_float/struct.OrderedFloat.html (improvement?)
            for x in low..=high {
                tpos.entry(x)
                    .and_modify(|(_, qx)| *qx += 1.0)
                    .or_insert((f64::try_from(x).unwrap() * 10.0_f64.powi(-max_digits), 1.0));
            }
        });

        // Create volume profile `DataFrame`
        let (px, qx): (Vec<_>, Vec<_>) = tpos.values().cloned().unzip();
        let result = df!(
            "px" => &px,
            "qx" => &qx,
        );

        let _ = tx.send(result.unwrap().sort(["px"], false));
    });
    rx.await.expect("Panic in rayon::spawn")
}

fn round(val: &Series) -> Series {
    val.f64()
        .unwrap()
        .into_iter()
        .map(|o: Option<f64>| o.map(|px: f64| px.round()))
        .collect::<Float64Chunked>()
        .into_series()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::{functions::write_df_to_bytes, gcs::upload_file},
        config::{self, GCS_DATA_BUCKET},
        enums::{data::LeafDir, markets::MarketKind},
        producers::test::Test,
        streams::tests::load_csv,
    };
    use google_cloud_default::WithAuthExt;
    use google_cloud_storage::client::{Client, ClientConfig};

    /// This unit test asserts, if:
    /// * the `volume_profile` of a DataFrame is computed correctly
    /// * the result is sorted in ascending order
    /// * the values for the price columns are rounded
    #[tokio::test]
    async fn test_volume_profile() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let d = Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET));
        let px = d.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Price));
        let qx = d.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Quantity));

        let df: polars::prelude::PolarsResult<DataFrame> = df!(
            &px => &[1.0, 1.4, 2.49, 2.5, 3.1],
            &qx => &[1.0, 1.0,  2.0, 3.0, 3.0],
        );

        // The target DataFrame is sorted in ascending order and the values for the price columns are rounded
        let target: polars::prelude::PolarsResult<DataFrame> = df!(
            &px => &[1.0, 2.0, 3.0],
            &qx => &[2.0, 2.0, 6.0],
        );

        let result = vol_profile(Arc::new(d), df.unwrap(), false).await.unwrap();
        dbg!(&result);
        assert_eq!(result.frame_equal(&target.unwrap()), true)
    }

    #[tokio::test]
    async fn test_market_profile() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        // Configure Data provider
        let d = Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

        // Configure file paths:
        // 1. Directory to test files
        let dir = std::path::PathBuf::from("data/test/other/market_profile");
        // 2. OHLC we want to compute the market profile for
        let test = dir.join("target_ohlc.csv");
        // 3. File where we want to store the result
        let ap = dir.clone().join("vol.csv");
        // 4. Load the OHLC `.csv` file from step (2.) into a `DataFrame`
        let df = d.get_df(&test, &LeafDir::Ohlc1m).await.unwrap();

        // Compute volume (i.e. market profile, as the underlying data is OHLC data)
        let market = MarketKind::EurUsd;
        let max_digits = market.number_of_digits();
        let vol = tpo(d.clone(), df, max_digits).await;

        // Save file to GCS
        let bytes = write_df_to_bytes(vol.unwrap());

        // TODO Manueller Call get_google_cloud_client
        upload_file(&config::get_google_cloud_client().await, &ap, bytes).await;

        // Compare result
        let target = load_csv(&dir, "target_vol.csv").await.unwrap();
        let result = load_csv(&dir, "vol.csv").await.unwrap();
        assert_eq!(result.frame_equal(&target), true);
    }
}
