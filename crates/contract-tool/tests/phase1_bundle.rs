use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use catalog_bench_contract::{check_phase1_result_bundle, load_bundle};

#[test]
fn checked_in_phase1_bundle_recomputes_to_five_by_five_matrix() -> Result<()> {
    let root = repository_root();
    let manifest = check_phase1_result_bundle(&root)?;
    let bundle = load_bundle(&manifest)?;
    assert_eq!(bundle.scenarios().len(), 5);
    assert_eq!(bundle.results().len(), 25);
    Ok(())
}

#[test]
fn phase1_import_rejects_source_transcript_drift() -> Result<()> {
    let source = repository_root();
    let temporary = tempfile::tempdir()?;
    for relative in [
        "profiles/v1/current-2026-08-26.json",
        "profiles/v1/phase1-2026-08-28.json",
        "scenarios/v1/client.pyiceberg.interoperability.json",
        "scenarios/v1/iceberg-rest.commit.correctness.json",
        "scenarios/v1/iceberg-rest.config.negotiation.json",
        "scenarios/v1/iceberg-rest.namespace.behavior.json",
        "scenarios/v1/iceberg-rest.table.behavior.json",
    ] {
        copy_file(&source, temporary.path(), relative)?;
    }
    copy_tree(
        &source.join("results/source/phase1-2026-08-28"),
        &temporary.path().join("results/source/phase1-2026-08-28"),
    )?;
    copy_tree(
        &source.join("results/v1/2026-08-28-phase1"),
        &temporary.path().join("results/v1/2026-08-28-phase1"),
    )?;
    check_phase1_result_bundle(temporary.path())?;
    let transcript = temporary
        .path()
        .join("results/source/phase1-2026-08-28/evidence/config-lakecat.json");
    let mut bytes = fs::read(&transcript)?;
    bytes.push(b'\n');
    fs::write(&transcript, bytes)?;
    assert!(check_phase1_result_bundle(temporary.path()).is_err());
    Ok(())
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn copy_file(source: &Path, destination: &Path, relative: &str) -> Result<()> {
    let output = destination.join(relative);
    fs::create_dir_all(output.parent().unwrap())?;
    fs::copy(source.join(relative), output)?;
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let output = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &output)?;
        } else {
            fs::copy(entry.path(), output)?;
        }
    }
    Ok(())
}
