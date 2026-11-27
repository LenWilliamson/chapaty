use std::path::PathBuf;

use polars::prelude::{LazyCsvReader, LazyFileListReader, PlRefPath};

use crate::{
    data::common::RiskMetricsConfig,
    report::{io::ToSchema, journal::Journal},
};

pub(super) fn load_journal_fixture() -> Journal {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let fixture_path = PathBuf::from(manifest_dir).join("tests/fixtures/report/input/journal.csv");

    assert!(
        fixture_path.exists(),
        "Test fixture missing: {}",
        fixture_path.display()
    );

    // Load with strict schema enforcement
    let schema = Journal::to_schema();
    let df = LazyCsvReader::new(PlRefPath::new(
        fixture_path
            .to_str()
            .expect("Invalid UTF-8 in fixture path"),
    ))
    .with_has_header(true)
    .with_schema(Some(schema))
    .with_try_parse_dates(true)
    .finish()
    .expect("Failed to create LazyFrame")
    .collect()
    .expect("Failed to collect DataFrame");

    Journal::new(&df, RiskMetricsConfig::default()).expect("Failed to create Journal")
}
