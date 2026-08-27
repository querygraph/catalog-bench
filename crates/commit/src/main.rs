use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use catalog_bench_commit::sweep::{run_contention_sweep, RunnerObservation, SweepProgress};
use catalog_bench_commit::transcript::{RankingDisposition, SweepClassification};
use catalog_bench_common::contract::{parse_contract, ContractDocument, Profile, Scenario};
use catalog_bench_conformance::{encode_evidence, sha256_hex, ContractDigests};
use clap::Parser;

const BUILD_REVISION: Option<&str> = option_env!("CATALOG_BENCH_SOURCE_REVISION");

#[derive(Debug, Parser)]
#[command(
    name = "catalog-bench-commit",
    about = "Run the profile-driven Iceberg REST same-table contention sweep"
)]
struct Cli {
    /// Validated profile containing every catalog and the shared MinIO service.
    #[arg(long)]
    profile: PathBuf,
    /// Canonical same-table contention scenario contract.
    #[arg(long)]
    scenario: PathBuf,
    /// Run-owned suffix: lowercase ASCII letters, digits, or underscores.
    #[arg(long)]
    fixture_id: String,
    /// New sanitized transcript file. Existing files are never overwritten.
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
    let contracts = load_contracts(&cli.profile, &cli.scenario)?;
    let observation = compiled_runner_observation()?;
    let transcript = run_contention_sweep(
        &contracts.profile,
        &contracts.scenario,
        contracts.digests,
        &cli.fixture_id,
        &observation,
        |name| std::env::var(name).ok(),
        print_progress,
    )
    .await?;
    let passed = transcript.passed();
    let classification = match transcript.classification {
        SweepClassification::Pass => "pass",
        SweepClassification::Fail { .. } => "fail",
    };
    let evidence = encode_evidence(&transcript)?;
    write_new(&cli.output, &evidence)?;
    print_ranking(&transcript.ranking);
    println!(
        "wrote {} (sha256={}, classification={classification})",
        cli.output.display(),
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

fn compiled_runner_observation() -> Result<RunnerObservation> {
    let revision = BUILD_REVISION.context(
        "binary has no compile-time CATALOG_BENCH_SOURCE_REVISION; use the pinned production Docker build",
    )?;
    RunnerObservation::new(
        observed_operating_system(),
        std::env::consts::ARCH,
        revision,
    )
    .map_err(anyhow::Error::from)
}

fn observed_operating_system() -> &'static str {
    match std::env::consts::OS {
        "linux" => "Linux",
        "macos" => "macOS",
        "windows" => "Windows",
        other => other,
    }
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
        .with_context(|| format!("refusing to overwrite transcript {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))
}

fn print_progress(progress: SweepProgress) {
    match progress {
        SweepProgress::Starting {
            repetition,
            kind,
            position,
            catalog,
        } => {
            eprintln!("starting repetition {repetition:02} {kind:?} position {position}: {catalog}")
        }
        SweepProgress::Completed {
            repetition,
            kind,
            position,
            catalog,
            passed,
        } => eprintln!(
            "completed repetition {repetition:02} {kind:?} position {position}: {catalog} ({})",
            if passed { "pass" } else { "fail" }
        ),
    }
}

fn print_ranking(ranking: &catalog_bench_commit::transcript::ContentionRanking) {
    println!("concurrent accepted-throughput ranking (median, min..max operations/s):");
    for entry in &ranking.entries {
        match &entry.disposition {
            RankingDisposition::Ranked { rank, score } => println!(
                "  {rank}. {}: {:.3} ({:.3}..{:.3})",
                entry.catalog.name, score.median, score.minimum, score.maximum
            ),
            RankingDisposition::NotRanked { reasons } => println!(
                "  — {}: not ranked ({})",
                entry.catalog.name,
                reasons.join("; ")
            ),
        }
    }
}
