use std::fs;
use std::process::Command;

use catalog_bench_conformance::sha256_hex;
use catalog_bench_contract::validate_engine_result_review;
use serde_json::{json, Value};

mod support;

use support::engine::{assert_error_contains, pretty_json, EvidenceFixture, FIXTURE_ID};

#[tokio::test]
async fn admits_hash_bound_reviewed_live_run_metadata() {
    let fixture = ReviewFixture::new().await;

    let validated = fixture.validate().unwrap();

    assert_eq!(validated.bundle_id(), "spark-review-test");
    assert_eq!(
        validated.title(),
        "Stock Spark interoperability test review"
    );
    assert_eq!(validated.output_directory(), "results/v1/spark-review-test");
    assert_eq!(validated.created_at(), "2026-08-27T12:02:00Z");
    assert_eq!(validated.started_at(), "2026-08-27T12:00:00Z");
    assert_eq!(validated.completed_at(), "2026-08-27T12:01:00Z");
    assert_eq!(
        validated.sanitized_invocation(),
        "docker/run-spark-interoperability.sh \"evidence01\""
    );
    assert_eq!(validated.environment().network, "catalog-bench-net");
    assert!(validated.redaction().reviewed);
    assert_eq!(validated.review_path(), fixture.review_path);
    assert_eq!(
        validated.review_bytes(),
        fs::read(&fixture.review_path).unwrap()
    );
    assert_eq!(validated.evidence().summary().total, 4);
}

#[tokio::test]
async fn cli_validates_the_review_without_caller_supplied_contracts_or_fixture() {
    let fixture = ReviewFixture::new().await;

    let output = Command::new(env!("CARGO_BIN_EXE_catalog-bench-contract"))
        .args(["engine-evidence", "validate-review", "--root"])
        .arg(fixture.evidence.root())
        .arg("--review")
        .arg(&fixture.review_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("bundle spark-review-test"));
    assert!(stdout.contains("4 transcript(s), 0 pass, 4 fail, 0 fixture collision"));
}

#[tokio::test]
async fn rejects_source_identity_catalog_order_and_path_drift() {
    let mut fixture = ReviewFixture::new().await;
    fixture.review["transcripts"][0]["source"]["sha256"] = Value::String("0".repeat(64));
    fixture.write_review();
    assert_error_contains(fixture.validate(), "identity differs from admitted bytes");

    fixture.reset_review();
    fixture.review["profile"]["sha256"] = Value::String("A".repeat(64));
    fixture.write_review();
    assert_error_contains(fixture.validate(), "64 lowercase hexadecimal");

    fixture.reset_review();
    fixture.review["bundle"]["output_directory"] = Value::String("../outside".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "normalized repository-relative path");

    fixture.reset_review();
    fixture.review["bundle"]["output_directory"] = Value::String("docs/engine".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "must be below results/v1");

    fixture.reset_review();
    fixture.review["scenario"]["location"] = Value::String("scenario//file.json".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "normalized repository-relative path");

    fixture.reset_review();
    fixture.review["profile"]["location"] = Value::String("C:/profile.json".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "portable repository-relative path");

    fixture.reset_review();
    fixture.review["scenario"]["bytes"] = Value::from(support::engine::SCENARIO.len() as u64 + 1);
    fixture.write_review();
    assert_error_contains(fixture.validate(), "identity differs from admitted bytes");

    fixture.reset_review();
    fixture.review["transcripts"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    fixture.write_review();
    assert_error_contains(fixture.validate(), "unique and strictly sorted");

    fixture.reset_review();
    fixture.review["transcripts"][0]["source"]["location"] =
        Value::String("other/gravitino.json".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "must share one evidence directory");
}

#[tokio::test]
async fn rejects_time_invocation_environment_and_redaction_drift() {
    let mut fixture = ReviewFixture::new().await;
    fixture.review["run"]["started_at"] = Value::String("2026-02-30T12:00:00Z".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "invalid UTC calendar value");

    fixture.reset_review();
    fixture.review["run"]["completed_at"] = Value::String("2026-08-27T11:59:59Z".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "timestamps are not strictly ordered");

    fixture.reset_review();
    fixture.review["run"]["sanitized_invocation"] = Value::String("spark-submit".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "canonical Spark launcher");

    fixture.reset_review();
    fixture.review["environment"]["architecture"] = Value::String("x86_64".to_owned());
    fixture.write_review();
    assert_error_contains(
        fixture.validate(),
        "differs from the runnable profile platform",
    );

    fixture.reset_review();
    fixture.review["environment"]["container_runtime"] = json!({
        "precision": "unknown",
        "explanation": "not captured"
    });
    fixture.write_review();
    assert_error_contains(
        fixture.validate(),
        "requires an exact container runtime capture",
    );

    fixture.reset_review();
    fixture.review["redaction"]["reviewed"] = Value::Bool(false);
    fixture.write_review();
    assert_error_contains(fixture.validate(), "has not completed redaction review");

    fixture.reset_review();
    fixture.review["redaction"]["removed_fields"] = json!(["credentials", "credentials"]);
    fixture.write_review();
    assert_error_contains(fixture.validate(), "duplicate removed field category");

    fixture.reset_review();
    fixture.review["redaction"]["policy"] = Value::String("  ".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "redaction policy must not be empty");

    fixture.reset_review();
    fixture.review["redaction"]["removed_fields"] = json!([]);
    fixture.write_review();
    assert_error_contains(fixture.validate(), "at least one removed field category");
}

#[tokio::test]
async fn orders_canonical_utc_instants_instead_of_timestamp_strings() {
    let mut fixture = ReviewFixture::new().await;
    fixture.review["run"]["started_at"] = Value::String("2024-02-29T12:00:00Z".to_owned());
    fixture.review["run"]["completed_at"] = Value::String("2024-02-29T12:00:00.1Z".to_owned());
    fixture.review["bundle"]["created_at"] = Value::String("2024-02-29T12:00:00.20Z".to_owned());
    fixture.write_review();
    fixture.validate().unwrap();

    fixture.review["run"]["started_at"] = Value::String("2023-02-29T12:00:00Z".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "invalid UTC calendar value");

    fixture.reset_review();
    fixture.review["run"]["started_at"] = Value::String("2026-08-27T12:00:00+00:00".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "canonical UTC RFC 3339 timestamp");

    fixture.reset_review();
    fixture.review["run"]["started_at"] =
        Value::String("2026-08-27T12:00:00.1234567890Z".to_owned());
    fixture.write_review();
    assert_error_contains(fixture.validate(), "canonical UTC RFC 3339 timestamp");

    fixture.reset_review();
    fixture.review["unexpected"] = Value::Bool(true);
    fixture.write_review();
    assert_error_contains(fixture.validate(), "unknown field");
}

#[tokio::test]
async fn rejects_unbounded_nonregular_and_non_newline_review_files() {
    let fixture = ReviewFixture::new().await;
    let mut bytes = fs::read(&fixture.review_path).unwrap();
    bytes.pop();
    fs::write(&fixture.review_path, bytes).unwrap();
    assert_error_contains(fixture.validate(), "is not newline-terminated");

    fs::write(&fixture.review_path, vec![b'x'; 1024 * 1024 + 1]).unwrap();
    assert_error_contains(fixture.validate(), "expected 1 to 1048576");

    fs::remove_file(&fixture.review_path).unwrap();
    fs::create_dir(&fixture.review_path).unwrap();
    assert_error_contains(fixture.validate(), "is not a regular file");
}

struct ReviewFixture {
    evidence: EvidenceFixture,
    review_path: std::path::PathBuf,
    baseline: Value,
    review: Value,
}

impl ReviewFixture {
    async fn new() -> Self {
        let evidence = EvidenceFixture::new().await;
        let review_path = evidence.root().join("review.json");
        let baseline = review_value(&evidence);
        fs::write(&review_path, pretty_json(&baseline)).unwrap();
        Self {
            evidence,
            review_path,
            review: baseline.clone(),
            baseline,
        }
    }

    fn validate(&self) -> anyhow::Result<catalog_bench_contract::ValidatedEngineResultReview> {
        validate_engine_result_review(self.evidence.root(), &self.review_path)
    }

    fn write_review(&self) {
        fs::write(&self.review_path, pretty_json(&self.review)).unwrap();
    }

    fn reset_review(&mut self) {
        self.review = self.baseline.clone();
        self.write_review();
    }
}

fn review_value(evidence: &EvidenceFixture) -> Value {
    let transcripts = evidence
        .transcripts
        .iter()
        .map(|(catalog, bytes)| {
            json!({
                "catalog": catalog,
                "source": source_identity(&format!("evidence/{catalog}.json"), bytes)
            })
        })
        .collect::<Vec<_>>();
    json!({
        "format": "catalog-bench/engine-result-review/v1",
        "bundle": {
            "id": "spark-review-test",
            "title": "Stock Spark interoperability test review",
            "output_directory": "results/v1/spark-review-test",
            "created_at": "2026-08-27T12:02:00Z"
        },
        "run": {
            "fixture_id": FIXTURE_ID,
            "sanitized_invocation": format!(
                "docker/run-spark-interoperability.sh \"{FIXTURE_ID}\""
            ),
            "started_at": "2026-08-27T12:00:00Z",
            "started_at_basis": "Captured immediately before the launcher invocation.",
            "completed_at": "2026-08-27T12:01:00Z",
            "completed_at_basis": "Captured after all four runner processes returned."
        },
        "profile": source_identity("profile.json", &evidence.profile_bytes),
        "scenario": source_identity("scenario.json", support::engine::SCENARIO),
        "transcripts": transcripts,
        "environment": {
            "operating_system": "Linux",
            "architecture": "aarch64",
            "cpu_model": {
                "precision": "unknown",
                "explanation": "The test fixture does not substitute a host CPU model."
            },
            "logical_cpus": { "precision": "exact", "value": 10 },
            "memory_bytes": { "precision": "exact", "value": 8321712128_u64 },
            "network": "catalog-bench-net",
            "container_runtime": {
                "precision": "exact",
                "value": "Docker Engine test fixture"
            },
            "runtime_flags": {
                "docker_compose": "test fixture",
                "same_docker_boundary": "all workflow processes",
                "workflow_execution_order": "sequential"
            }
        },
        "redaction": {
            "reviewed": true,
            "policy": "catalog-bench/value-safe-engine-v1 plus manual source review",
            "removed_fields": [
                "catalog OAuth client credentials and bearer tokens",
                "object-store access and secret keys",
                "raw engine rows and response bodies",
                "raw backend exception and log detail"
            ]
        }
    })
}

fn source_identity(location: &str, bytes: &[u8]) -> Value {
    json!({
        "location": location,
        "sha256": sha256_hex(bytes),
        "bytes": bytes.len() as u64
    })
}
