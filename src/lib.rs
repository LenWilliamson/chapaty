pub mod bots;
pub mod common;
pub mod config;
pub mod enums;
pub mod math;
pub mod producers;
pub mod streams;

#[cfg(test)]
mod test {
    use polars::prelude::*;

    use crate::common::functions::write_df_to_bytes;
    use std::{collections::HashMap, io::Cursor, vec};

    #[tokio::test]
    async fn test_ser() {
        let partitioned = df! {
            "fruits" => &["Apple", "Apple", "Pear", "Pear", "Pear", "Pear"],
            "maturity" => &["A", "B", "A", "C", "A", "D"],
            "N" => &[1, 2, 2, 4, 2, 8]

        }
        .unwrap()
        .partition_by(["N"])
        .unwrap();

        let mut map = HashMap::new();
        map.insert(None, partitioned[0].clone());
        map.insert(Some(1), partitioned[1].clone());
        dbg!(&partitioned);
        dbg!(&map);

        let mut df_bytes = Vec::new();
        for df in partitioned {
            df_bytes.push(write_df_to_bytes(df));
        }

        let mut map_df_bytes = HashMap::new();
        for (k, v) in map {
            if let Some(val) = k {
                map_df_bytes.insert(val, write_df_to_bytes(v));
            }
        }

        let ser = serde_json::to_string(&df_bytes).unwrap();
        dbg!(&ser);

        let ser_map = serde_json::to_string(&map_df_bytes).unwrap();

        let bytes: Vec<Vec<u8>> = serde_json::from_str(&ser).unwrap();
        let mut vec_dfs = Vec::<DataFrame>::new();
        for df in bytes {
            vec_dfs.push(
                CsvReader::new(Cursor::new(df))
                    .has_header(true)
                    .finish()
                    .unwrap(),
            );
        }

        let de_map: HashMap<i32, Vec<u8>> = serde_json::from_str(&ser_map).unwrap();
        let mut map_of_dfs = HashMap::new();
        for (k, v) in de_map {
            map_of_dfs.insert(
                k,
                CsvReader::new(Cursor::new(v))
                    .has_header(true)
                    .finish()
                    .unwrap(),
            );
        }

        dbg!(&vec_dfs);
        dbg!(map_of_dfs);

        let s = String::from("3,-4");
        let split: Vec<_> = s.split(',').collect();
        let tuple = (
            split[0].parse::<i64>().unwrap(),
            split[1].parse::<i64>().unwrap(),
        );

        dbg!(tuple);
    }

    #[test]
    fn test_df_to_json() {
        let df = df! {
            "a" => [10,20,30,40,50],
            "b" => [1,2,3,4,5],
        }
        .unwrap();
        let df_json = serde_json::to_value(&df).unwrap();
        println!("df_json {}", df_json);
        println!("df_json {}", df_json["columns"][0]["name"]);
        println!("df_json {}", df_json["columns"][0]["values"]);
        let vals: Vec<_> = df_json["columns"][0]["values"]
            .as_array()
            .unwrap()
            .into_iter()
            .map(|val| val.as_i64().unwrap())
            .collect();
        // let vals: Vec<_> = vals.into_iter().map(|val| val.as_i64().unwrap()).collect();
        dbg!(vals);
    }
}
