use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_conformance::{
    encode_evidence, run_config_probe, run_namespace_probe, sha256_hex, ContractDigests,
    ProbeClassification,
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
    /// Exercise namespace lifecycle, hierarchy, pagination, and errors.
    Namespace(NamespaceArgs),
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

#[derive(Debug, Args)]
struct NamespaceArgs {
    /// Validated profile containing the catalog adapter.
    #[arg(long)]
    profile: PathBuf,
    /// Namespace-behavior scenario contract.
    #[arg(long)]
    scenario: PathBuf,
    /// Profile component identifier to probe.
    #[arg(long)]
    catalog: String,
    /// Run-owned suffix: 1-24 lowercase ASCII letters, digits, or underscores.
    #[arg(long)]
    fixture_id: String,
    /// New evidence file. Existing files are never overwritten.
    #[arg(long)]
    output: PathBuf,
}

struct LoadedContracts {
    profile: Profile,
    scenario: Scenario,
    digests: ContractDigests,
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
        Command::Namespace(args) => run_namespace(args).await,
    }
}

async fn run_config(args: ConfigArgs) -> Result<bool> {
    let contracts = load_contracts(&args.profile, &args.scenario)?;
    let transcript = run_config_probe(
        &contracts.profile,
        &contracts.scenario,
        &ComponentId::new(args.catalog),
        contracts.digests,
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

async fn run_namespace(args: NamespaceArgs) -> Result<bool> {
    let contracts = load_contracts(&args.profile, &args.scenario)?;
    let transcript = run_namespace_probe(
        &contracts.profile,
        &contracts.scenario,
        &ComponentId::new(args.catalog),
        &args.fixture_id,
        contracts.digests,
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

fn load_contracts(profile_path: &Path, scenario_path: &Path) -> Result<LoadedContracts> {
    let profile_bytes = read_contract(profile_path)?;
    let scenario_bytes = read_contract(scenario_path)?;
    let profile = match parse_contract(&profile_bytes)
        .with_context(|| format!("invalid profile {}", profile_path.display()))?
    {
        ContractDocument::Profile(profile) => profile,
        document => bail!(
            "{} is a {}, not a profile",
            profile_path.display(),
            document.kind()
        ),
    };
    let scenario = match parse_contract(&scenario_bytes)
        .with_context(|| format!("invalid scenario {}", scenario_path.display()))?
    {
        ContractDocument::Scenario(scenario) => scenario,
        document => bail!(
            "{} is a {}, not a scenario",
            scenario_path.display(),
            document.kind()
        ),
    };
    Ok(LoadedContracts {
        profile,
        scenario,
        digests: ContractDigests {
            profile_sha256: sha256_hex(&profile_bytes),
            scenario_sha256: sha256_hex(&scenario_bytes),
        },
    })
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
