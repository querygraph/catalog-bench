use catalog_bench_commit::sweep::{
    run_contention_sweep, verify_runner, RunnerObservation, SweepError,
};
use catalog_bench_common::contract::{parse_contract, ContractDocument, Profile, Scenario};
use catalog_bench_conformance::ContractDigests;

const PROFILE_BYTES: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO_BYTES: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");

#[test]
fn runner_identity_requires_exact_profile_runtime_and_source() {
    let (profile, _) = contracts();
    let revision = runner_revision(&profile);
    let matching = RunnerObservation::new("Linux", "aarch64", revision.clone()).unwrap();
    let transcript = verify_runner(&profile, &matching).unwrap();
    assert!(transcript.passed());

    let wrong_runtime = RunnerObservation::new("Linux", "x86_64", revision).unwrap();
    assert!(matches!(
        verify_runner(&profile, &wrong_runtime).unwrap_err(),
        SweepError::RunnerRuntimeMismatch { .. }
    ));

    let wrong_revision = RunnerObservation::new("Linux", "aarch64", "a".repeat(40)).unwrap();
    assert!(matches!(
        verify_runner(&profile, &wrong_revision).unwrap_err(),
        SweepError::RunnerSourceMismatch { .. }
    ));
}

#[test]
fn runner_observations_reject_unpinned_or_malformed_builds() {
    assert!(RunnerObservation::new("Linux", "aarch64", "development").is_err());
    assert!(RunnerObservation::new("", "aarch64", "a".repeat(40)).is_err());
    assert!(RunnerObservation::new("Linux", "", "a".repeat(40)).is_err());
    assert!(RunnerObservation::new("Linux", "aarch64", "A".repeat(40)).is_err());
}

#[tokio::test]
async fn runtime_mismatch_stops_before_credentials_or_network_are_touched() {
    let (profile, scenario) = contracts();
    let observation =
        RunnerObservation::new("macOS", "aarch64", runner_revision(&profile)).unwrap();
    let result = run_contention_sweep(
        &profile,
        &scenario,
        ContractDigests {
            profile_sha256: "0".repeat(64),
            scenario_sha256: "1".repeat(64),
        },
        "fixture",
        &observation,
        |_| panic!("runtime verification must precede credential access"),
        |_| panic!("runtime verification must precede round progress"),
    )
    .await;
    assert!(matches!(
        result.unwrap_err(),
        SweepError::RunnerRuntimeMismatch { .. }
    ));
}

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE_BYTES).unwrap() else {
        panic!("profile fixture");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO_BYTES).unwrap() else {
        panic!("scenario fixture");
    };
    (profile, scenario)
}

fn runner_revision(profile: &Profile) -> String {
    profile
        .components
        .iter()
        .find(|component| component.id.as_str() == "catalog-bench-commit")
        .and_then(|component| component.source.as_ref())
        .map(|source| source.revision.clone())
        .unwrap()
}
