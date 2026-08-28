use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::Result;
use catalog_bench_common::contract::{
    parse_contract, ContractDocument, ProfilePurpose, ProfileReadiness,
};
use catalog_bench_contract::{check_phase1_profile, render_phase1_profile};

const SOURCE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-28.json");
const MATERIALIZATION: &[u8] =
    include_bytes!("../../../materializations/v1/phase1-2026-08-28.json");
const RUNNABLE: &[u8] = include_bytes!("../../../profiles/v1/phase1-2026-08-28.json");

#[test]
fn checked_in_phase1_profile_matches_audited_inputs() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    check_phase1_profile(
        &root.join("profiles/v1/current-2026-08-28.json"),
        &root.join("materializations/v1/phase1-2026-08-28.json"),
        &root.join("profiles/v1/phase1-2026-08-28.json"),
    )
}

#[test]
fn phase1_profile_is_runnable_and_contains_every_behavioral_component() -> Result<()> {
    let rendered = render_phase1_profile(SOURCE, MATERIALIZATION)?;
    assert_eq!(rendered, RUNNABLE);
    let ContractDocument::Profile(profile) = parse_contract(&rendered)? else {
        panic!("expected profile");
    };
    assert_eq!(profile.purpose, ProfilePurpose::Conformance);
    assert_eq!(profile.readiness, ProfileReadiness::Runnable);
    let components = profile
        .components
        .iter()
        .map(|component| component.id.as_str())
        .collect::<BTreeSet<_>>();
    for expected in [
        "catalog-bench-conformance",
        "lakecat",
        "polaris",
        "gravitino",
        "lakekeeper",
        "nessie",
        "pyiceberg",
    ] {
        assert!(components.contains(expected));
    }
    Ok(())
}
