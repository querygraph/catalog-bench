use std::collections::BTreeSet;

use catalog_bench_common::contract::{
    parse_contract, AdapterRequestHandling, ContractDocument, ScenarioFamily,
};

const HISTORICAL: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.json");
const CURRENT: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");
const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");

#[test]
fn historical_and_current_contention_contracts_remain_distinct() {
    let ContractDocument::Scenario(historical) = parse_contract(HISTORICAL).unwrap() else {
        panic!("historical contention document must be a scenario");
    };
    let ContractDocument::Scenario(current) = parse_contract(CURRENT).unwrap() else {
        panic!("current contention document must be a scenario");
    };

    assert_eq!(historical.id, current.id);
    assert_eq!(historical.version, 1);
    assert_eq!(current.version, 2);
    assert_eq!(current.family, ScenarioFamily::Concurrency);
    assert_eq!(current.capabilities.len(), 10);
    assert_eq!(current.steps.len(), 13);
    assert_eq!(current.assertions.len(), 14);
    assert!(current
        .assertions
        .iter()
        .all(|assertion| assertion.required));
}

#[test]
fn current_contention_policy_is_common_strict_and_run_owned() {
    let value: serde_json::Value = serde_json::from_slice(CURRENT).unwrap();

    assert_eq!(
        value.pointer("/parameters/round_policy"),
        Some(&serde_json::json!({
            "conditioning_rounds": 1,
            "measured_rounds": 5,
            "catalog_order": "rotate-left",
            "aggregate": "median-with-min-max",
            "require_every_round_to_pass": true
        }))
    );
    assert_eq!(
        value.pointer("/parameters/workload/idempotency_key"),
        Some(&serde_json::json!("omitted"))
    );
    assert_eq!(
        value.pointer("/parameters/object_store/endpoint"),
        Some(&serde_json::json!("http://minio:9000"))
    );
    assert_eq!(
        value.pointer("/parameters/object_store/bucket"),
        Some(&serde_json::json!("warehouse"))
    );
    assert_eq!(
        value.pointer("/steps/5/parameters/scope"),
        Some(&serde_json::json!("returned table location"))
    );
    assert_eq!(
        value.pointer("/steps/10/parameters/minimum_formula"),
        Some(&serde_json::json!(
            "warmup.accepted + sequential.accepted + concurrent.accepted"
        ))
    );
    assert_eq!(
        value.pointer("/steps/11/parameters/purge_requested"),
        Some(&serde_json::json!(false))
    );
}

#[test]
fn every_current_adapter_schedules_same_table_contention_without_a_shim() {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("current profile must be a profile");
    };
    let capability = "iceberg-rest.concurrency.same-table-contention".into();
    let catalogs = profile
        .catalog_adapters
        .iter()
        .map(|adapter| {
            assert!(matches!(
                adapter.request_handling,
                AdapterRequestHandling::ProtocolNative
            ));
            assert!(adapter.capabilities.exercises(&capability));
            adapter.catalog.as_str()
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(
        catalogs,
        BTreeSet::from(["gravitino", "lakecat", "lakekeeper", "nessie", "polaris"])
    );
}
