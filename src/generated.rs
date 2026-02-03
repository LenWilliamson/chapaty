pub mod chapaty {
    pub mod bq_exporter {
        pub mod v1 {
            tonic::include_proto!("chapaty.bq_exporter.v1");
        }
    }

    pub mod data {
        pub mod v1 {
            tonic::include_proto!("chapaty.data.v1");
        }
    }
}
