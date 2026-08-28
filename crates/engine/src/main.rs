use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use catalog_bench_common::contract::ComponentId;
use catalog_bench_conformance::{encode_evidence, sha256_hex, write_new_evidence};
use catalog_bench_engine::{
    run_stock_engine_interoperability, EngineBehaviorClassification, EngineContracts,
    ProcessEnvironment,
};
use clap::Parser;

const FAILURE_EXIT: u8 = 2;
const FIXTURE_COLLISION_EXIT: u8 = 3;

#[derive(Debug, Parser)]
#[command(
    name = "catalog-bench-engine",
    about = "Run the profile-driven stock-engine Iceberg interoperability workflow"
)]
struct Cli {
    /// Runnable profile containing the stock engine and catalog adapter.
    #[arg(long)]
    profile: PathBuf,
    /// Canonical common engine-interoperability scenario contract.
    #[arg(long)]
    scenario: PathBuf,
    /// Profile component identifier for the catalog under test.
    #[arg(long)]
    catalog: String,
    /// Run-owned suffix: lowercase ASCII letters, digits, or underscores.
    #[arg(long)]
    fixture_id: String,
    /// New sanitized transcript file. Existing files are never overwritten.
    #[arg(long)]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(EngineBehaviorClassification::Pass) => ExitCode::SUCCESS,
        Ok(EngineBehaviorClassification::Fail) => ExitCode::from(FAILURE_EXIT),
        Ok(EngineBehaviorClassification::FixtureCollision) => {
            ExitCode::from(FIXTURE_COLLISION_EXIT)
        }
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<EngineBehaviorClassification> {
    let profile_bytes = read_contract(&cli.profile)?;
    let scenario_bytes = read_contract(&cli.scenario)?;
    let contracts = EngineContracts::parse(&profile_bytes, &scenario_bytes)
        .context("invalid engine interoperability contracts")?;
    let transcript = run_stock_engine_interoperability(
        &contracts,
        &ComponentId::new(cli.catalog),
        &cli.fixture_id,
        Arc::new(ProcessEnvironment),
    )
    .await
    .context("engine interoperability execution failed")?;
    transcript
        .validate(&contracts)
        .context("engine transcript failed final validation")?;
    let classification = transcript.execution.classification;
    let evidence = encode_evidence(&transcript).context("failed to encode engine transcript")?;
    write_new_evidence(&cli.output, &evidence)
        .with_context(|| format!("failed to publish {}", cli.output.display()))?;
    println!(
        "wrote {} (sha256={}, classification={})",
        cli.output.display(),
        sha256_hex(&evidence),
        classification_name(classification)
    );
    Ok(classification)
}

fn read_contract(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}

fn classification_name(classification: EngineBehaviorClassification) -> &'static str {
    match classification {
        EngineBehaviorClassification::Pass => "pass",
        EngineBehaviorClassification::Fail => "fail",
        EngineBehaviorClassification::FixtureCollision => "fixture-collision",
    }
}
