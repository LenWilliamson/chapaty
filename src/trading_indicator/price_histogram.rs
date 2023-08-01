use crate::{
    converter::any_value::AnyValueConverter, enums::column_names::VolumeProfileColumnKind,
};

use polars::prelude::{col, lit, AnyValue, DataFrame, IntoLazy};

pub struct PriceHistogram {
    df: DataFrame,
}

impl PriceHistogram {
    pub fn new(df: DataFrame) -> Self {
        Self { df }
    }

    /// This function computes the POC for the given volume profile. The POC is the point of control. Hence,
    /// the price where the highest volume for a given time interval was traded.
    ///
    /// # Arguments
    /// * `df_vol` - volume profile
    pub fn poc(&self) -> f64 {
        let df = self.df.clone();

        let qx = &VolumeProfileColumnKind::Quantity.to_string();
        let px = &VolumeProfileColumnKind::Price.to_string();

        let sorted = df
            .lazy()
            .select([col(&px).filter(col(&qx).eq(col(&qx).max()))])
            .collect()
            .unwrap();

        let poc = sorted.get(0).unwrap()[0].unwrap_float64();
        poc
    }

    /// Computes the volume area described in <https://www.vtad.de/lexikon/market-profile/>
    /// # Arguments
    /// * `dp` - data producer
    /// * `df` - volume profile
    /// * `std_dev` - standard deviation
    /// * `poc` - peek of control
    pub fn volume_area(&self, std_dev: f64) -> (f64, f64) {
        let poc = self.poc();

        let df = self.df.clone();
        let qx_col = VolumeProfileColumnKind::Quantity as usize;

        // There could be more than one POC
        // Currently we choose the lowest price if there are multiple POCs
        let total_tpo: f64 = df.get_columns()[qx_col].sum().unwrap();

        let poc_df = df
            .with_row_count("row", None)
            .unwrap()
            .clone()
            .lazy()
            .filter(col("px").eq(lit(poc)))
            .select(&[col("*")])
            .collect()
            .unwrap();

        let poc_row = match poc_df.column("row").unwrap().get(0).unwrap() {
            AnyValue::UInt32(x) => x,
            _ => panic!("Expected f64 but got diffrent value."),
        };

        let poc_vol = match poc_df.column("qx").unwrap().get(0).unwrap() {
            AnyValue::Float64(x) => x,
            _ => panic!("Expected f64 but got diffrent value."),
        };

        println!("total_tpo={total_tpo}, poc_row={poc_row}, poc_vol={poc_vol}");
        compute(
            &df,
            total_tpo * std_dev - poc_vol,
            poc,
            poc,
            poc_row,
            poc_row,
        )
    }
}

fn compute(
    df: &DataFrame,
    total_tpo: f64,
    val: f64,
    vah: f64,
    val_row: u32,
    vah_row: u32,
) -> (f64, f64) {
    if total_tpo <= 0.0 {
        return (val, vah);
    }
    // TODO fix if we return df_vol descending
    let (vah_new, tpo_vah) = add(get_tpo(df, vah_row, 1), get_tpo(df, vah_row, 2));
    let (val_new, tpo_val) = add(get_tpo(df, val_row, -1), get_tpo(df, val_row, -2));

    if tpo_vah < tpo_val {
        return compute(df, total_tpo - tpo_val, val_new, vah, val_row - 2, vah_row);
    } else {
        return compute(df, total_tpo - tpo_vah, val, vah_new, val_row, vah_row + 2);
    }
}

fn add(t1: (f64, f64), t2: (f64, f64)) -> (f64, f64) {
    (t2.0, t1.1 + t2.1)
}

fn get_tpo(df: &DataFrame, row: u32, offset: i32) -> (f64, f64) {
    (
        match df
            .get((i32::try_from(row).unwrap() + offset) as usize)
            .unwrap()[0]
        {
            AnyValue::Float64(x) => x,
            _ => panic!("Matching against wrong value. Expected f64"),
        },
        match df
            .get((i32::try_from(row).unwrap() + offset) as usize)
            .unwrap()[1]
        {
            AnyValue::Float64(x) => x,
            _ => panic!("Matching against wrong value. Expected f64"),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bot::time_frame_snapshot::TimeFrameSnapshotBuilder,
        cloud_api::api_for_unit_tests::download_df_map,
    };
    use polars::{df, prelude::NamedFrom};

    #[tokio::test]
    async fn test_poc() {
        let df_map = download_df_map(
            "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1w/target_vol-aggTrades.json".to_string(),
        )
        .await;

        let mut snapshot = TimeFrameSnapshotBuilder::new(12).build();
        let mut df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(42000.0, PriceHistogram { df }.poc());

        snapshot = TimeFrameSnapshotBuilder::new(8).build();
        df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(38100.0, PriceHistogram { df }.poc());

        snapshot = TimeFrameSnapshotBuilder::new(9).build();
        df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(42100.0, PriceHistogram { df }.poc());

        snapshot = TimeFrameSnapshotBuilder::new(10).build();
        df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(42200.0, PriceHistogram { df }.poc());

        df = df!(
            "px" => &[1.0, 2.0, 3.0, 4.0],
            "qx" => &[10, 10, 9, 10]
        )
        .unwrap();
        assert_eq!(1.0, PriceHistogram { df }.poc());

        df = df!(
            "px" => &[ 83_200.0, 38_100.0, 38_000.0, 1.0],
            "qx" => &[100.0, 300.0, 150.0, 300.0],
        )
        .unwrap();
        assert_eq!(38_100.0, PriceHistogram { df }.poc());
    }

    #[tokio::test]
    async fn test_compute_volume_area() {
        let df = df!(
            "px"=> &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "qx" => &[0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 20.0, 15.0, 10.0, 5.0, 0.0],
        )
        .unwrap();
        assert_eq!((3.0, 5.0), PriceHistogram { df }.volume_area(0.3));
    }
}
