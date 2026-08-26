use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{parse_contract, ComponentId, ContractDocument};
use catalog_bench_conformance::{
    encode_evidence, run_config_probe, sha256_hex, ContractDigests, ProbeClassification,
};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "catalog-bench-conformance",
    about = "Run catalog-neutral conformance probes and emit sanitized evidence"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Negotiate Iceberg REST configuration and endpoint advertisement.
    Config(ConfigArgs),
}

#[derive(Debug, Args)]
struct ConfigArgs {
    /// Validated profile containing the catalog adapter.
    #[arg(long)]
    profile: PathBuf,
    /// Config-negotiation scenario contract.
    #[arg(long)]
    scenario: PathBuf,
    /// Profile component identifier to probe.
    #[arg(long)]
    catalog: String,
    /// New evidence file. Existing files are never overwritten.
    #[arg(long)]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<bool> {
    match cli.command {
        Command::Config(args) => run_config(args).await,
    }
}

async fn run_config(args: ConfigArgs) -> Result<bool> {
    let profile_bytes = read_contract(&args.profile)?;
    let scenario_bytes = read_contract(&args.scenario)?;
    let profile = match parse_contract(&profile_bytes)
        .with_context(|| format!("invalid profile {}", args.profile.display()))?
    {
        ContractDocument::Profile(profile) => profile,
        document => bail!(
            "{} is a {}, not a profile",
            args.profile.display(),
            document.kind()
        ),
    };
    let scenario = match parse_contract(&scenario_bytes)
        .with_context(|| format!("invalid scenario {}", args.scenario.display()))?
    {
        ContractDocument::Scenario(scenario) => scenario,
        document => bail!(
            "{} is a {}, not a scenario",
            args.scenario.display(),
            document.kind()
        ),
    };

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new(args.catalog),
        ContractDigests {
            profile_sha256: sha256_hex(&profile_bytes),
            scenario_sha256: sha256_hex(&scenario_bytes),
        },
        |name| std::env::var(name).ok(),
    )
    .await?;
    let passed = transcript.passed();
    let classification = classification_name(&transcript.classification);
    let evidence = encode_evidence(&transcript)?;
    write_new(&args.output, &evidence)?;
    println!(
        "wrote {} (sha256={}, classification={classification})",
        args.output.display(),
        sha256_hex(&evidence)
    );
    Ok(passed)
}

fn read_contract(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("refusing to overwrite evidence file {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))
}

fn classification_name(classification: &ProbeClassification) -> &'static str {
    match classification {
        ProbeClassification::Pass => "pass",
        ProbeClassification::Fail { .. } => "fail",
        ProbeClassification::Unsupported { .. } => "unsupported",
    }
}
