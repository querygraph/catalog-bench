use std::fs;
use std::process::Command;

use catalog_bench_common::contract::{AssertionOutcome, FailureCategory, ResultOutcome};
use catalog_bench_contract::{
    check_engine_result_bundle, load_bundle, render_engine_matrix, validate_engine_result_review,
    write_engine_result_bundle,
};
use serde_json::Value;

mod support;

use support::engine::{assert_error_contains, ReviewFixture};

#[tokio::test]
async fn materializes_four_complete_fail_closed_results_and_matrix() {
    let fixture = ReviewFixture::new().await;

    let manifest =
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path).unwrap();
    let bundle = load_bundle(&manifest).unwrap();

    assert_eq!(bundle.results().len(), 4);
    assert_eq!(bundle.manifest().source_evidence.len(), 5);
    let output = manifest.parent().unwrap();
    assert_eq!(
        fs::read(bundle.profile_path()).unwrap(),
        fixture.evidence.profile_bytes
    );
    assert_eq!(
        fs::read(output.join("source/scenario.json")).unwrap(),
        support::engine::SCENARIO
    );
    assert_eq!(
        fs::read(output.join("source/review.json")).unwrap(),
        fs::read(&fixture.review_path).unwrap()
    );
    for (catalog, transcript) in &fixture.evidence.transcripts {
        assert_eq!(
            fs::read(output.join(format!("source/transcripts/{catalog}.json"))).unwrap(),
            *transcript
        );
    }
    for validated in bundle.results() {
        let result = validated.result();
        assert_eq!(
            result.client.as_ref().unwrap().profile_component.as_str(),
            "spark-4.1"
        );
        assert_eq!(result.adapters.len(), 1);
        assert_eq!(result.adapters[0].as_str(), "iceberg-java");
        assert_eq!(result.assertions.len(), 14);
        assert_eq!(
            result
                .assertions
                .iter()
                .filter(|evaluation| matches!(evaluation.outcome, AssertionOutcome::Pass))
                .count(),
            1
        );
        assert!(result.measurements.is_empty());
        let ResultOutcome::Fail { failure } = &result.outcome else {
            panic!("runtime-rejected fixture must materialize as fail");
        };
        assert_eq!(failure.category, FailureCategory::Assertion);
        assert!(!failure.retryable);
        assert_eq!(failure.evidence.len(), 2);
    }

    let matrix = fs::read_to_string(manifest.with_file_name("MATRIX.md")).unwrap();
    assert_eq!(matrix, render_engine_matrix(&bundle).unwrap());
    assert!(matrix.contains("not a latency or throughput ranking"));
    assert!(matrix.contains("| LakeCat 0.3.0-42-g962f43cb | Apache Spark 4.1.3 | `fail` |"));
    assert!(matrix.contains("| `transcript-sanitized` | pass | pass | pass | pass |"));
    assert!(!matrix.contains("| Rank |"));
    check_engine_result_bundle(fixture.evidence.root(), &fixture.review_path).unwrap();
}

#[tokio::test]
async fn materializes_validated_passes_without_inventing_performance_rankings() {
    let fixture = ReviewFixture::passing().await;

    let manifest =
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path).unwrap();
    let bundle = load_bundle(&manifest).unwrap();

    for validated in bundle.results() {
        let result = validated.result();
        assert!(matches!(result.outcome, ResultOutcome::Pass { .. }));
        assert!(result
            .assertions
            .iter()
            .all(|evaluation| matches!(evaluation.outcome, AssertionOutcome::Pass)));
        assert!(result.measurements.is_empty());
    }
    let matrix = fs::read_to_string(manifest.with_file_name("MATRIX.md")).unwrap();
    assert_eq!(matrix.matches("14 / 14 passed").count(), 4);
    assert!(!matrix.contains("| Rank |"));
}

#[tokio::test]
async fn fixture_collisions_remain_visible_as_not_tested() {
    let fixture = ReviewFixture::fixture_collisions().await;

    let manifest =
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path).unwrap();
    let bundle = load_bundle(&manifest).unwrap();

    for validated in bundle.results() {
        let result = validated.result();
        let ResultOutcome::NotTested { reason } = &result.outcome else {
            panic!("fixture collision must not become a catalog failure");
        };
        assert!(reason.explanation.contains("before mutation"));
        assert!(result.assertions.iter().all(|evaluation| {
            matches!(evaluation.outcome, AssertionOutcome::NotEvaluated { .. })
        }));
    }
    let matrix = fs::read_to_string(manifest.with_file_name("MATRIX.md")).unwrap();
    assert_eq!(
        matrix.matches("| `not-tested` | not evaluated |").count(),
        4
    );
    assert!(matrix.contains("Not tested: A pre-existing run-owned fixture"));
}

#[tokio::test]
async fn trusted_checks_cannot_hide_an_untrusted_process_terminal() {
    let fixture = ReviewFixture::harness_failures().await;

    let manifest =
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path).unwrap();
    let bundle = load_bundle(&manifest).unwrap();

    for validated in bundle.results() {
        let result = validated.result();
        assert!(result
            .assertions
            .iter()
            .all(|evaluation| matches!(evaluation.outcome, AssertionOutcome::Pass)));
        let ResultOutcome::Fail { failure } = &result.outcome else {
            panic!("untrusted terminal must remain a failed run");
        };
        assert_eq!(failure.category, FailureCategory::Harness);
        assert!(failure.detail.contains("protocol-rejected"));
        assert!(failure.detail.contains("No retryability claim"));
    }
}

#[tokio::test]
async fn writer_is_create_new_and_checker_rejects_tampering_or_extras() {
    let fixture = ReviewFixture::new().await;
    write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path).unwrap();

    assert_error_contains(
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path),
        "refusing to replace existing engine bundle output",
    );

    let output = fixture.evidence.root().join("results/v1/spark-review-test");
    let lakecat = output.join("lakecat.json");
    let original = fs::read(&lakecat).unwrap();
    let mut tampered = original.clone();
    tampered.push(b' ');
    fs::write(&lakecat, tampered).unwrap();
    assert_error_contains(
        check_engine_result_bundle(fixture.evidence.root(), &fixture.review_path),
        "is stale",
    );

    fs::write(&lakecat, original).unwrap();
    fs::write(output.join("unexpected.txt"), b"unexpected").unwrap();
    assert_error_contains(
        check_engine_result_bundle(fixture.evidence.root(), &fixture.review_path),
        "missing, unexpected, or nonregular entry",
    );
}

#[tokio::test]
async fn publication_requires_repository_archived_source_locations() {
    let mut fixture = ReviewFixture::new().await;
    let public_profile = fixture.evidence.root().join("public-profile.json");
    fs::write(&public_profile, &fixture.evidence.profile_bytes).unwrap();
    fixture.review["profile"]["location"] = Value::String("public-profile.json".to_owned());
    fixture.write_review();

    fixture.validate().unwrap();
    assert_error_contains(
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path),
        "is not in the public `profiles/v1/` evidence boundary",
    );
}

#[tokio::test]
async fn publication_requires_a_repository_archived_review_sidecar() {
    let fixture = ReviewFixture::new().await;
    let unarchived_review = fixture.evidence.root().join("review.json");
    fs::copy(&fixture.review_path, &unarchived_review).unwrap();

    validate_engine_result_review(fixture.evidence.root(), &unarchived_review).unwrap();
    assert_error_contains(
        write_engine_result_bundle(fixture.evidence.root(), &unarchived_review),
        "is not in the public `results/source/` evidence boundary",
    );
}

#[cfg(unix)]
#[tokio::test]
async fn writer_refuses_symlinked_output_parents() {
    use std::os::unix::fs::symlink;

    let fixture = ReviewFixture::new().await;
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), fixture.evidence.root().join("results/v1")).unwrap();

    assert_error_contains(
        write_engine_result_bundle(fixture.evidence.root(), &fixture.review_path),
        "engine bundle parent is not a real directory",
    );
    assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn cli_writes_checks_and_dispatches_the_engine_matrix() {
    let fixture = ReviewFixture::new().await;
    let write = contract_command()
        .current_dir(fixture.evidence.root())
        .args([
            "engine-import",
            "write",
            "--root",
            ".",
            "--review",
            support::engine::REVIEW_LOCATION,
        ])
        .output()
        .unwrap();
    assert!(
        write.status.success(),
        "{}",
        String::from_utf8_lossy(&write.stderr)
    );
    assert!(String::from_utf8(write.stdout)
        .unwrap()
        .contains("4 result(s)"));

    let check = contract_command()
        .current_dir(fixture.evidence.root())
        .args([
            "engine-import",
            "check",
            "--root",
            ".",
            "--review",
            support::engine::REVIEW_LOCATION,
        ])
        .output()
        .unwrap();
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );

    let matrix = contract_command()
        .current_dir(fixture.evidence.root())
        .args([
            "matrix",
            "check",
            "--manifest",
            "results/v1/spark-review-test/manifest.json",
            "--output",
            "results/v1/spark-review-test/MATRIX.md",
        ])
        .output()
        .unwrap();
    assert!(
        matrix.status.success(),
        "{}",
        String::from_utf8_lossy(&matrix.stderr)
    );
}

fn contract_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_catalog-bench-contract"))
}
