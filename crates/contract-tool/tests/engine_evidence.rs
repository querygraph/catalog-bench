use std::fs;
use std::process::Command;

use catalog_bench_conformance::encode_evidence;
use catalog_bench_engine::EngineTranscript;
use serde_json::Value;

mod support;

use support::engine::{
    assert_error_contains, pretty_json, EvidenceFixture, CATALOGS, FIXTURE_ID, SCENARIO,
};

#[tokio::test]
async fn admits_exact_profile_derived_catalog_set() {
    let fixture = EvidenceFixture::new().await;

    let evidence = fixture.validate().unwrap();

    assert_eq!(evidence.fixture_id(), FIXTURE_ID);
    assert_eq!(evidence.profile_bytes(), fixture.profile_bytes);
    assert_eq!(evidence.scenario_bytes(), SCENARIO);
    assert_eq!(evidence.contracts().profile().catalog_adapters.len(), 4);
    assert_eq!(
        evidence
            .transcripts()
            .iter()
            .map(|entry| entry.transcript().components.catalog.id.as_str())
            .collect::<Vec<_>>(),
        vec!["gravitino", "lakecat", "lakekeeper", "polaris"]
    );
    assert!(evidence
        .transcripts()
        .iter()
        .all(|entry| entry.path().is_file() && !entry.bytes().is_empty()));
    assert_eq!(
        evidence.summary(),
        catalog_bench_contract::EngineEvidenceSummary {
            total: 4,
            pass: 0,
            fail: 4,
            fixture_collision: 0,
        }
    );
}

#[tokio::test]
async fn cli_reports_only_validated_classification_counts() {
    let fixture = EvidenceFixture::new().await;

    let output = Command::new(env!("CARGO_BIN_EXE_catalog-bench-contract"))
        .args(["engine-evidence", "validate", "--profile"])
        .arg(&fixture.profile_path)
        .arg("--scenario")
        .arg(&fixture.scenario_path)
        .arg("--evidence-directory")
        .arg(&fixture.evidence_directory)
        .args(["--fixture-id", FIXTURE_ID])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("4 transcript(s), 0 pass, 4 fail, 0 fixture collision"));
    for catalog in CATALOGS {
        assert!(!stdout.contains(catalog));
    }
}

#[tokio::test]
async fn rejects_missing_unexpected_and_nonregular_entries() {
    let fixture = EvidenceFixture::new().await;
    let lakecat = fixture.evidence_directory.join("lakecat.json");
    fs::remove_file(&lakecat).unwrap();
    assert_error_contains(fixture.validate(), "missing [lakecat.json]");

    fs::write(&lakecat, &fixture.transcripts["lakecat"]).unwrap();
    fs::write(fixture.evidence_directory.join("unexpected.json"), b"{}\n").unwrap();
    assert_error_contains(fixture.validate(), "unexpected [unexpected.json]");

    fs::remove_file(fixture.evidence_directory.join("unexpected.json")).unwrap();
    let gravitino = fixture.evidence_directory.join("gravitino.json");
    fs::remove_file(&gravitino).unwrap();
    fs::create_dir(&gravitino).unwrap();
    assert_error_contains(fixture.validate(), "is not a regular file");
}

#[tokio::test]
async fn rejects_catalog_fixture_contract_and_size_drift() {
    let fixture = EvidenceFixture::new().await;
    fs::write(
        fixture.evidence_directory.join("polaris.json"),
        &fixture.transcripts["lakecat"],
    )
    .unwrap();
    assert_error_contains(
        fixture.validate(),
        "contains catalog `lakecat`, expected `polaris`",
    );

    fs::write(
        fixture.evidence_directory.join("polaris.json"),
        &fixture.transcripts["polaris"],
    )
    .unwrap();
    let lakecat_path = fixture.evidence_directory.join("lakecat.json");
    let mut lakecat: Value = serde_json::from_slice(&fixture.transcripts["lakecat"]).unwrap();
    lakecat["fixture"]["id"] = Value::String("different_fixture".to_owned());
    fs::write(&lakecat_path, pretty_json(&lakecat)).unwrap();
    assert_error_contains(
        fixture.validate(),
        "contains fixture `different_fixture`, expected `evidence01`",
    );

    fs::write(&lakecat_path, &fixture.transcripts["lakecat"]).unwrap();
    let mut unsafe_transcript: EngineTranscript =
        serde_json::from_slice(&fixture.transcripts["lakecat"]).unwrap();
    unsafe_transcript.sanitization.transcript_sanitized = false;
    fs::write(&lakecat_path, encode_evidence(&unsafe_transcript).unwrap()).unwrap();
    assert_error_contains(fixture.validate(), "Sanitization");

    fs::write(&lakecat_path, &fixture.transcripts["lakecat"]).unwrap();
    let mut noncanonical = fixture.transcripts["lakecat"].clone();
    noncanonical.push(b'\n');
    fs::write(&lakecat_path, noncanonical).unwrap();
    assert_error_contains(
        fixture.validate(),
        "not in canonical newline-terminated encoding",
    );

    fs::write(&lakecat_path, &fixture.transcripts["lakecat"]).unwrap();
    let mut drifted_profile = fixture.profile_bytes.clone();
    drifted_profile.push(b'\n');
    fs::write(&fixture.profile_path, drifted_profile).unwrap();
    assert_error_contains(fixture.validate(), "ContractDigests");

    fs::write(&fixture.profile_path, &fixture.profile_bytes).unwrap();
    fs::write(
        fixture.evidence_directory.join("lakekeeper.json"),
        vec![b'x'; 4 * 1024 * 1024 + 1],
    )
    .unwrap();
    assert_error_contains(fixture.validate(), "expected 1 to 4194304");
}
