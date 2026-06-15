pub mod chapaty {
    pub mod bq_exporter {
        pub mod v1 {
            #![allow(clippy::all, clippy::pedantic, clippy::nursery)]
            include!("proto_gen/chapaty.bq_exporter.v1.rs");
        }
    }

    pub mod data {
        pub mod v1 {
            #![allow(clippy::all, clippy::pedantic, clippy::nursery)]
            include!("proto_gen/chapaty.data.v1.rs");
        }
    }
}
