use std::path::PathBuf;

use anyhow::Result;
use catalog_bench_contract::{check_publication, PublicationProfile};

#[test]
fn checked_in_cross_scenario_publication_is_current_and_secret_scanned() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    check_publication(&root, PublicationProfile::Smoke)
}
