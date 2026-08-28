//! Deterministic publication of the reviewed Phase 1 behavioral evidence set.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    parse_contract, AssertionOutcome, ContractDocument, Profile, Scenario,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::publication::{artifact, pretty_json};
use crate::{load_bundle, sha256_hex};

const REVIEW: &str = "results/source/phase1-2026-08-28/review.json";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    format: String,
    bundle: ReviewBundle,
    run: ReviewRun,
    source_profile: String,
    publication_profile: String,
    evidence_directory: String,
    environment: ReviewEnvironment,
    redaction: ReviewRedaction,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewBundle {
    id: String,
    title: String,
    output_directory: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewRun {
    id: String,
    started_at: String,
    completed_at: String,
    sanitized_invocation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewEnvironment {
    operating_system: String,
    architecture: String,
    logical_cpus: u32,
    memory_bytes: u64,
    container_runtime: String,
    network: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewRedaction {
    reviewed: bool,
    policy: String,
    removed_fields: Vec<String>,
}

struct RenderedBundle {
    output: PathBuf,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

pub fn write_phase1_result_bundle(root: &Path) -> Result<PathBuf> {
    let rendered = render(root)?;
    if rendered.output.exists() {
        bail!(
            "refusing to replace existing Phase 1 bundle {}",
            rendered.output.display()
        );
    }
    fs::create_dir(&rendered.output)?;
    for (relative, bytes) in &rendered.files {
        fs::write(rendered.output.join(relative), bytes)?;
    }
    check_phase1_result_bundle(root)
}

pub fn check_phase1_result_bundle(root: &Path) -> Result<PathBuf> {
    let rendered = render(root)?;
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(&rendered.output)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            bail!("Phase 1 bundle contains a non-file entry");
        }
        actual.insert(PathBuf::from(entry.file_name()));
    }
    if actual != rendered.files.keys().cloned().collect() {
        bail!("Phase 1 bundle contains a missing or unexpected entry");
    }
    for (relative, expected) in &rendered.files {
        let path = rendered.output.join(relative);
        if fs::read(&path)? != *expected {
            bail!("{} differs from reviewed Phase 1 evidence", path.display());
        }
    }
    let manifest = rendered.output.join("manifest.json");
    load_bundle(&manifest)?;
    Ok(manifest)
}

fn render(root: &Path) -> Result<RenderedBundle> {
    let review_bytes = fs::read(root.join(REVIEW))?;
    let review: Review = serde_json::from_slice(&review_bytes)?;
    if review.format != "catalog-bench/phase1-result-review/v1" || !review.redaction.reviewed {
        bail!("Phase 1 review is not admitted for publication");
    }
    let source_profile_bytes = fs::read(root.join(&review.source_profile))?;
    let source_profile = parse_profile(&source_profile_bytes)?;
    let source_profile_sha = sha256_hex(&source_profile_bytes);
    let publication_profile_bytes = fs::read(root.join(&review.publication_profile))?;
    let publication_profile = parse_profile(&publication_profile_bytes)?;
    let publication_profile_ref = artifact(
        &format!("../../../{}", review.publication_profile),
        "application/json",
        &publication_profile_bytes,
        "Runnable artifact-resolved Phase 1 correctness profile.",
    );
    let evidence_root = root.join(&review.evidence_directory);
    let mut evidence_paths = discover_json(&evidence_root)?;
    evidence_paths.sort();
    if evidence_paths.len() != 25 {
        bail!("Phase 1 evidence must contain exactly 25 transcripts");
    }

    let scenario_files = BTreeMap::from([
        (
            "client.pyiceberg.interoperability",
            "client.pyiceberg.interoperability.json",
        ),
        (
            "iceberg-rest.commit.correctness",
            "iceberg-rest.commit.correctness.json",
        ),
        (
            "iceberg-rest.config.negotiation",
            "iceberg-rest.config.negotiation.json",
        ),
        (
            "iceberg-rest.namespace.behavior",
            "iceberg-rest.namespace.behavior.json",
        ),
        (
            "iceberg-rest.table.behavior",
            "iceberg-rest.table.behavior.json",
        ),
    ]);
    let mut scenarios = BTreeMap::<String, (Scenario, Vec<u8>, String)>::new();
    for (id, file) in scenario_files {
        let location = format!("scenarios/v1/{file}");
        let bytes = fs::read(root.join(&location))?;
        let scenario = parse_scenario(&bytes)?;
        if scenario.id.as_str() != id {
            bail!("Phase 1 scenario identity drift for {file}");
        }
        scenarios.insert(id.to_owned(), (scenario, bytes, location));
    }
    let components = publication_profile
        .components
        .iter()
        .map(|component| (component.id.as_str(), component))
        .collect::<BTreeMap<_, _>>();
    let mut result_bytes = Vec::<(String, Vec<u8>, Value)>::new();
    let mut seen = BTreeSet::new();
    let mut source_artifacts = Vec::new();
    for path in &evidence_paths {
        let bytes = fs::read(path)?;
        let transcript: Value = serde_json::from_slice(&bytes)?;
        validate_sanitization(&transcript)?;
        let scenario_id = text(&transcript, "/scenario/id")?;
        let scenario_version = integer(&transcript, "/scenario/version")? as u32;
        let (scenario, scenario_bytes, _) = scenarios
            .get(scenario_id)
            .with_context(|| format!("unexpected Phase 1 scenario `{scenario_id}`"))?;
        if scenario.version != scenario_version
            || text(&transcript, "/contract_digests/scenario_sha256")? != sha256_hex(scenario_bytes)
            || text(&transcript, "/contract_digests/profile_sha256")? != source_profile_sha
        {
            bail!("transcript contract digest drift in {}", path.display());
        }
        let transcript_profile = transcript.pointer("/profile").context("missing profile")?;
        let transcript_profile_id = transcript_profile
            .as_str()
            .or_else(|| transcript_profile.get("id").and_then(Value::as_str))
            .context("invalid transcript profile identity")?;
        if transcript_profile_id != source_profile.id.as_str() {
            bail!("transcript source profile identity drift");
        }
        let catalog = text(&transcript, "/adapter/catalog")?;
        if !seen.insert((scenario_id.to_owned(), catalog.to_owned())) {
            bail!("duplicate Phase 1 scenario/catalog transcript");
        }
        let component = components
            .get(catalog)
            .context("catalog missing from profile")?;
        if text(&transcript, "/adapter/name")? != component.name
            || text(&transcript, "/adapter/version")? != component.version
        {
            bail!("transcript catalog identity drift for {catalog}");
        }
        validate_assertions(&transcript, scenario)?;
        let relative_source = path.strip_prefix(root.join("results"))?;
        let transcript_artifact = artifact(
            &format!("../../{}", relative_source.display()),
            "application/json",
            &bytes,
            "Sanitized value-safe Phase 1 stock-client transcript.",
        );
        source_artifacts.push(transcript_artifact.clone());
        let result = result_value(
            &review,
            &publication_profile,
            &publication_profile_bytes,
            scenario,
            scenario_bytes,
            &transcript,
            transcript_artifact,
            catalog,
        )?;
        let file = format!("{}--{}.json", scenario_slug(scenario_id), catalog);
        let bytes = pretty_json(&result)?;
        match parse_contract(&bytes)? {
            ContractDocument::Result(_) => {}
            _ => bail!("generated Phase 1 record is not a result"),
        }
        result_bytes.push((file, bytes, result));
    }
    if seen.len() != 25 {
        bail!("Phase 1 scenario/catalog matrix is incomplete");
    }
    result_bytes.sort_by(|left, right| left.0.cmp(&right.0));
    source_artifacts.sort_by(|left, right| left.location.cmp(&right.location));
    source_artifacts.push(artifact(
        "../../source/phase1-2026-08-28/review.json",
        "application/json",
        &review_bytes,
        "Human-reviewed execution environment, time boundary, and redaction admission.",
    ));
    let scenario_artifacts = scenarios
        .values()
        .map(|(_, bytes, location)| {
            artifact(
                &format!("../../../{location}"),
                "application/json",
                bytes,
                "Canonical Phase 1 correctness scenario.",
            )
        })
        .collect::<Vec<_>>();
    let results = result_bytes
        .iter()
        .map(|(file, bytes, result)| {
            artifact(
                file,
                "application/json",
                bytes,
                &format!(
                    "{} result for {}.",
                    text(result, "/scenario/id").unwrap_or("Phase 1"),
                    text(result, "/catalog/name").unwrap_or("catalog")
                ),
            )
        })
        .collect::<Vec<_>>();
    let manifest = json!({
        "contract_version": "catalog-bench/v1",
        "kind": "manifest",
        "id": review.bundle.id,
        "title": review.bundle.title,
        "created_at": review.bundle.created_at,
        "provenance": {"kind": "live-run", "runner": "catalog-bench-conformance", "sanitized_invocation": review.run.sanitized_invocation, "started_at": review.run.started_at, "completed_at": review.run.completed_at},
        "profile": publication_profile_ref,
        "scenarios": scenario_artifacts,
        "results": results,
        "source_evidence": source_artifacts,
        "redaction": {"reviewed": review.redaction.reviewed, "policy": review.redaction.policy, "removed_fields": review.redaction.removed_fields},
        "extensions": {"querygraph/source-profile": {"id": source_profile.id, "sha256": source_profile_sha}, "querygraph/evidence-matrix": "5 scenarios x 5 catalogs; correctness only; no timing claim"}
    });
    let manifest_bytes = pretty_json(&manifest)?;
    match parse_contract(&manifest_bytes)? {
        ContractDocument::Manifest(_) => {}
        _ => bail!("generated Phase 1 bundle index is not a manifest"),
    }
    let mut files = result_bytes
        .into_iter()
        .map(|(file, bytes, _)| (PathBuf::from(file), bytes))
        .collect::<BTreeMap<_, _>>();
    files.insert(PathBuf::from("manifest.json"), manifest_bytes);
    Ok(RenderedBundle {
        output: root.join(&review.bundle.output_directory),
        files,
    })
}

#[allow(clippy::too_many_arguments)]
fn result_value(
    review: &Review,
    profile: &Profile,
    profile_bytes: &[u8],
    scenario: &Scenario,
    scenario_bytes: &[u8],
    transcript: &Value,
    transcript_artifact: catalog_bench_common::contract::ArtifactReference,
    catalog: &str,
) -> Result<Value> {
    let classification = transcript
        .pointer("/classification")
        .context("missing classification")?;
    let evidence_id = "source-transcript";
    let outcome = match text(classification, "/status")? {
        "pass" => {
            json!({"status": "pass", "summary": "Every required assertion passed in the reviewed value-safe transcript."})
        }
        "fail" => {
            let summary = text(classification, "/summary")?;
            json!({"status": "fail", "failure": {"category": "assertion", "summary": summary, "detail": summary, "retryable": false, "evidence": [evidence_id]}})
        }
        value => bail!("unsupported Phase 1 classification `{value}`"),
    };
    let assertions = transcript
        .get("assertions")
        .and_then(Value::as_array)
        .context("missing assertions")?
        .iter()
        .map(|assertion| {
            let mut assertion = assertion.clone();
            assertion
                .as_object_mut()
                .expect("validated assertion object")
                .insert("evidence".to_owned(), json!([evidence_id]));
            assertion
        })
        .collect::<Vec<_>>();
    let component = profile
        .components
        .iter()
        .find(|component| component.id.as_str() == catalog)
        .context("catalog component absent")?;
    let mut value = json!({
        "contract_version": "catalog-bench/v1", "kind": "result",
        "id": format!("{}-{}-{}", review.bundle.id, scenario_slug(scenario.id.as_str()), catalog),
        "scenario": {"id": scenario.id, "version": scenario.version, "digest": {"algorithm": "sha256", "value": sha256_hex(scenario_bytes)}},
        "profile": {"id": profile.id, "digest": {"algorithm": "sha256", "value": sha256_hex(profile_bytes)}},
        "catalog": {"profile_component": component.id, "name": component.name, "version": component.version},
        "run": {"kind": "single", "id": format!("{}-{}-{}", review.run.id, scenario_slug(scenario.id.as_str()), catalog), "started_at": review.run.started_at, "finished_at": review.run.completed_at, "repetition": 1},
        "outcome": outcome,
        "environment": {"operating_system": review.environment.operating_system, "architecture": review.environment.architecture, "cpu_model": {"precision": "unknown", "explanation": "The LinuxKit container boundary did not expose a CPU model; no host model is substituted."}, "logical_cpus": {"precision": "exact", "value": review.environment.logical_cpus}, "memory_bytes": {"precision": "exact", "value": review.environment.memory_bytes}, "network": review.environment.network, "container_runtime": {"precision": "exact", "value": review.environment.container_runtime}, "runtime_flags": {"claim_scope": "correctness only; elapsed time and resource utilization are not measured", "runner_build": "Rust 1.97.1; release; opt-level=3; fat LTO; codegen-units=1; target-cpu=native; panic=abort; stripped"}},
        "assertions": assertions,
        "evidence": [{"id": evidence_id, "kind": "http-transcript", "artifact": transcript_artifact, "sanitized": true, "redactions": review.redaction.removed_fields}],
        "extensions": {"querygraph/source-transcript-format": text(transcript, "/format")?}
    });
    if scenario.id.as_str() == "client.pyiceberg.interoperability" {
        let client = profile
            .components
            .iter()
            .find(|component| component.id.as_str() == "pyiceberg")
            .context("PyIceberg component absent")?;
        value.as_object_mut().unwrap().insert(
            "client".to_owned(),
            json!({"profile_component": client.id, "name": client.name, "version": client.version}),
        );
    }
    Ok(value)
}

fn validate_assertions(transcript: &Value, scenario: &Scenario) -> Result<()> {
    let observed = transcript
        .get("assertions")
        .and_then(Value::as_array)
        .context("transcript assertions missing")?;
    if observed.len() != scenario.assertions.len() {
        bail!("transcript assertion count drift");
    }
    for expected in &scenario.assertions {
        let assertion = observed
            .iter()
            .find(|value| {
                value.get("assertion").and_then(Value::as_str) == Some(expected.id.as_str())
            })
            .with_context(|| format!("missing assertion `{}`", expected.id))?;
        if assertion.get("required").and_then(Value::as_bool) != Some(expected.required) {
            bail!("assertion requirement drift for `{}`", expected.id);
        }
        let _: AssertionOutcome = serde_json::from_value(assertion["outcome"].clone())?;
    }
    Ok(())
}

fn validate_sanitization(transcript: &Value) -> Result<()> {
    let sanitization = transcript
        .get("sanitization")
        .and_then(Value::as_object)
        .context("missing sanitization")?;
    if sanitization
        .iter()
        .any(|(key, value)| key.starts_with("raw_") && value.as_bool() != Some(false))
    {
        bail!("transcript persists raw secret, row, exception, or response material");
    }
    Ok(())
}

fn discover_json(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            paths.extend(discover_json(&path)?);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn parse_profile(bytes: &[u8]) -> Result<Profile> {
    match parse_contract(bytes)? {
        ContractDocument::Profile(value) => Ok(value),
        _ => bail!("expected profile"),
    }
}
fn parse_scenario(bytes: &[u8]) -> Result<Scenario> {
    match parse_contract(bytes)? {
        ContractDocument::Scenario(value) => Ok(value),
        _ => bail!("expected scenario"),
    }
}
fn text<'a>(value: &'a Value, pointer: &str) -> Result<&'a str> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .with_context(|| format!("missing text at `{pointer}`"))
}
fn integer(value: &Value, pointer: &str) -> Result<u64> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing integer at `{pointer}`"))
}
fn scenario_slug(id: &str) -> String {
    id.replace('.', "-")
}
