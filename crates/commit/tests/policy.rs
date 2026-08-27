use std::collections::BTreeMap;

use catalog_bench_commit::policy::{ContentionPlan, RoundKind};
use catalog_bench_common::contract::{
    parse_contract, AdapterRequestHandling, ComponentId, ContractDocument,
};

const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");
const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");

fn contracts() -> (
    catalog_bench_common::contract::Profile,
    catalog_bench_common::contract::Scenario,
) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("fixture profile must parse as a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("fixture scenario must parse as a scenario");
    };
    (profile, scenario)
}

#[test]
fn current_contract_produces_a_balanced_six_round_plan() {
    let (profile, scenario) = contracts();
    let plan = ContentionPlan::from_contracts(&profile, &scenario).unwrap();

    assert_eq!(plan.rounds().len(), 6);
    assert_eq!(plan.rounds()[0].kind, RoundKind::Conditioning);
    assert!(plan.rounds()[1..]
        .iter()
        .all(|round| round.kind == RoundKind::Measured));

    let mut positions = BTreeMap::<ComponentId, Vec<usize>>::new();
    for round in &plan.rounds()[1..] {
        for (position, catalog) in round.catalogs.iter().enumerate() {
            positions
                .entry(catalog.catalog.clone())
                .or_default()
                .push(position);
        }
    }
    assert_eq!(positions.len(), 5);
    for observed in positions.values_mut() {
        observed.sort_unstable();
        assert_eq!(observed, &[0, 1, 2, 3, 4]);
    }
}

#[test]
fn fixtures_are_unique_per_catalog_and_round_and_reject_unsafe_ids() {
    let (profile, scenario) = contracts();
    let plan = ContentionPlan::from_contracts(&profile, &scenario).unwrap();
    let lakecat = ComponentId::from("lakecat");

    let first = plan.fixture(&lakecat, "run_0826", 1).unwrap();
    let second = plan.fixture(&lakecat, "run_0826", 2).unwrap();
    assert_eq!(first.table, "same_table_contention");
    assert_ne!(first.namespace, second.namespace);
    assert!(first.namespace.starts_with("cb_c108_lakecat_run_0826_r01"));

    assert!(plan.fixture(&lakecat, "contains-dash", 1).is_err());
    assert!(plan.fixture(&lakecat, "", 1).is_err());
    assert!(plan
        .fixture(&ComponentId::from("missing"), "safe", 1)
        .is_err());
}

#[test]
fn scenario_drift_and_behavior_changing_shims_are_rejected() {
    let (profile, mut scenario) = contracts();
    scenario.parameters.insert(
        "transcript_format".to_owned(),
        serde_json::json!("catalog-bench/contention-transcript/v99"),
    );
    let error = ContentionPlan::from_contracts(&profile, &scenario).unwrap_err();
    assert!(error.to_string().contains("transcript format"));

    let (profile, mut scenario) = contracts();
    scenario.parameters.insert(
        "metadata_retention".to_owned(),
        serde_json::json!({
            "delete_after_commit": true,
            "previous_versions_max": 100
        }),
    );
    let error = ContentionPlan::from_contracts(&profile, &scenario).unwrap_err();
    assert!(error.to_string().contains("metadata retention"));

    let (mut profile, scenario) = contracts();
    profile.catalog_adapters[0].request_handling = AdapterRequestHandling::BehaviorChangingShim {
        component: ComponentId::from("catalog-bench-commit"),
        description: "test-only mutation".to_owned(),
    };
    let error = ContentionPlan::from_contracts(&profile, &scenario).unwrap_err();
    assert!(error.to_string().contains("behavior-changing shim"));
}
