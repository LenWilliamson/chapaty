// Intern crates
use crate::{
    enums::columns::{Columns, VolumeProfileColumnNames},
    producers::DataProducer,
};

// Extern crates
use polars::prelude::{col, lit, AnyValue, DataFrame, IntoLazy};

/// Computes the volume area described in <https://www.vtad.de/lexikon/market-profile/>
/// # Arguments
/// * `dp` - data producer
/// * `df` - volume profile
/// * `std_dev` - standard deviation
/// * `poc` - peek of control
pub fn compute_volume_area<D: DataProducer>(
    dp: &D,
    df: &DataFrame, // Volume Profile
    std_dev: f64,
    poc: f64,
) -> (f64, f64) {

    let qx_col = dp.column_name_as_int(&Columns::Vol(VolumeProfileColumnNames::Quantity));

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

    println!(
        "total_tpo={total_tpo}, poc_row={poc_row}, poc_vol={poc_vol}"
    );
    compute(
        df,
        total_tpo * std_dev - poc_vol,
        poc,
        poc,
        poc_row,
        poc_row,
    )
}

fn compute(
    df: &DataFrame,
    total_tpo: f64,
    //poc: f64,
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
    // Intern crates
    use super::compute_volume_area;
    use crate::{config::GCS_DATA_BUCKET, producers::test::Test};

    // Extern crates
    use google_cloud_storage::client::{Client, ClientConfig};
    use polars::prelude::{df, NamedFrom};
    use google_cloud_default::WithAuthExt;
    #[tokio::test]
    async fn test_compute_volume_area() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let d = Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
            
        );
        let df = df!(
            "px"=> &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "qx" => &[0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 20.0, 15.0, 10.0, 5.0, 0.0],
        );

        let df_u = df.unwrap();

        // let result = compute_volume_area(&d, &df_u, 0.5, 5.0);
        // assert_eq!((3.0, 7.0), result);

        let result = compute_volume_area(&d, &df_u, 0.3, 5.0);
        assert_eq!((3.0, 5.0), result);
    }
}
