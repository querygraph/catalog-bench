use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{generated_schemas, parse_contract};
use catalog_bench_contract::{
    check_contention_profile, check_contention_result_bundle, check_engine_result_bundle,
    check_flink_profile, check_historical_commit_bundle, check_phase1_profile,
    check_phase1_result_bundle, check_publication, check_spark_profile, check_trino_profile,
    load_bundle, render_matrix, validate_engine_evidence_set, validate_engine_result_review,
    write_contention_profile, write_contention_result_bundle, write_engine_result_bundle,
    write_flink_profile, write_historical_commit_bundle, write_phase1_profile,
    write_phase1_result_bundle, write_publication, write_spark_profile, write_trino_profile,
    PublicationProfile,
};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "catalog-bench-contract",
    about = "Generate and validate catalog-bench v1 contract documents"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage generated JSON Schemas.
    Schemas {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Deserialize and semantically validate JSON contract documents.
    Validate {
        /// JSON files or directories to validate. Directories are recursive.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
    /// Validate immutable artifacts and cross-document links in a result bundle.
    Bundle {
        #[command(subcommand)]
        command: BundleCommand,
    },
    /// Generate or check a human-readable matrix from validated result records.
    Matrix {
        #[command(subcommand)]
        command: MatrixCommand,
    },
    /// Materialize scenario-scoped runnable profiles.
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    /// Recompute the canonical 2026-08-08 result bundle from preserved TSVs.
    HistoricalImport {
        #[command(subcommand)]
        command: HistoricalImportCommand,
    },
    /// Materialize the canonical C110 production contention result bundle.
    ContentionImport {
        #[command(subcommand)]
        command: ContentionImportCommand,
    },
    /// Independently validate a complete stock-engine transcript set.
    EngineEvidence {
        #[command(subcommand)]
        command: EngineEvidenceCommand,
    },
    /// Materialize reviewed stock-engine evidence into an immutable bundle.
    EngineImport {
        #[command(subcommand)]
        command: EngineImportCommand,
    },
    /// Generate or verify the cross-scenario public result surface.
    Publication {
        #[command(subcommand)]
        command: PublicationCommand,
    },
    /// Materialize or verify the reviewed Phase 1 behavioral result bundle.
    Phase1Import {
        #[command(subcommand)]
        command: Phase1ImportCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SchemaCommand {
    /// Regenerate all checked-in Draft 2020-12 schemas.
    Write {
        #[arg(long, default_value = "schemas/v1")]
        directory: PathBuf,
    },
    /// Fail if checked-in schemas differ from the Rust contract types.
    Check {
        #[arg(long, default_value = "schemas/v1")]
        directory: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum BundleCommand {
    /// Verify every document, digest, size, identity, and cross-reference.
    Validate {
        #[arg(long)]
        manifest: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum MatrixCommand {
    /// Write the matrix rendered from a validated bundle.
    Write {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Fail if the checked-in matrix differs from its validated bundle.
    Check {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    /// Derive the runnable Phase 1 behavioral publication profile.
    MaterializePhase1(ProfileFiles),
    /// Check that the Phase 1 profile exactly matches its audited inputs.
    CheckPhase1(ProfileFiles),
    /// Derive a runnable contention profile from a draft and image observations.
    MaterializeContention(ProfileFiles),
    /// Check that a runnable contention profile exactly matches its inputs.
    CheckContention(ProfileFiles),
    /// Derive the runnable Spark interoperability profile.
    MaterializeSpark(ProfileFiles),
    /// Check that the Spark interoperability profile exactly matches its inputs.
    CheckSpark(ProfileFiles),
    /// Derive the runnable Flink interoperability profile.
    MaterializeFlink(ProfileFiles),
    /// Check that the Flink interoperability profile exactly matches its inputs.
    CheckFlink(ProfileFiles),
    /// Derive the runnable Trino interoperability profile.
    MaterializeTrino(ProfileFiles),
    /// Check that the Trino interoperability profile exactly matches its inputs.
    CheckTrino(ProfileFiles),
}

#[derive(Debug, Args)]
struct ProfileFiles {
    #[arg(long)]
    source_profile: PathBuf,
    #[arg(long)]
    materialization: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Subcommand)]
enum HistoricalImportCommand {
    /// Recompute and write records plus their immutable manifest.
    Write {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    /// Fail if checked-in records differ from a fresh recomputation.
    Check {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ContentionImportCommand {
    /// Recompute and write records, manifest, and generated matrix.
    Write {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    /// Fail if records or the generated matrix differ from reviewed evidence.
    Check {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum EngineEvidenceCommand {
    /// Bind one bounded transcript per profile catalog to exact contract bytes.
    Validate(EngineEvidenceFiles),
    /// Validate reviewed live-run metadata and every source identity it binds.
    ValidateReview(EngineReviewFile),
}

#[derive(Debug, Args)]
struct EngineEvidenceFiles {
    #[arg(long)]
    profile: PathBuf,
    #[arg(long)]
    scenario: PathBuf,
    #[arg(long)]
    evidence_directory: PathBuf,
    #[arg(long)]
    fixture_id: String,
}

#[derive(Debug, Args)]
struct EngineReviewFile {
    #[arg(long, default_value = ".")]
    root: PathBuf,
    #[arg(long)]
    review: PathBuf,
}

#[derive(Debug, Subcommand)]
enum EngineImportCommand {
    /// Create records, manifest, source copies, and matrix without overwriting.
    Write(EngineReviewFile),
    /// Recompute every byte and reject missing or unexpected output.
    Check(EngineReviewFile),
}

#[derive(Debug, Subcommand)]
enum PublicationCommand {
    /// Generate the bundle index and known-gaps report, then verify all bundles.
    Write(PublicationFiles),
    /// Verify generated reports, exact bundles, and the bundle-wide secret scan.
    Check(PublicationFiles),
}

#[derive(Debug, Subcommand)]
enum Phase1ImportCommand {
    /// Create the immutable 25-result Phase 1 correctness bundle.
    Write {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    /// Recompute every result and reject source or output drift.
    Check {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
}

#[derive(Debug, Args)]
struct PublicationFiles {
    #[arg(long, default_value = ".")]
    root: PathBuf,
    #[arg(long, value_parser = ["smoke", "full"], default_value = "smoke")]
    profile: String,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Schemas { command } => match command {
            SchemaCommand::Write { directory } => write_schemas(&directory),
            SchemaCommand::Check { directory } => check_schemas(&directory),
        },
        Command::Validate { paths } => validate_paths(&paths),
        Command::Bundle { command } => match command {
            BundleCommand::Validate { manifest } => validate_bundle(&manifest),
        },
        Command::Matrix { command } => match command {
            MatrixCommand::Write { manifest, output } => write_matrix(&manifest, &output),
            MatrixCommand::Check { manifest, output } => check_matrix(&manifest, &output),
        },
        Command::Profile { command } => match command {
            ProfileCommand::MaterializePhase1(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                write_phase1_profile(&source_profile, &materialization, &output)?;
                println!("wrote {}", output.display());
                Ok(())
            }
            ProfileCommand::CheckPhase1(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                check_phase1_profile(&source_profile, &materialization, &output)?;
                println!("{} matches its materialization inputs", output.display());
                Ok(())
            }
            ProfileCommand::MaterializeContention(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                write_contention_profile(&source_profile, &materialization, &output)?;
                println!("wrote {}", output.display());
                Ok(())
            }
            ProfileCommand::CheckContention(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                check_contention_profile(&source_profile, &materialization, &output)?;
                println!("{} matches its materialization inputs", output.display());
                Ok(())
            }
            ProfileCommand::MaterializeSpark(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                write_spark_profile(&source_profile, &materialization, &output)?;
                println!("wrote {}", output.display());
                Ok(())
            }
            ProfileCommand::CheckSpark(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                check_spark_profile(&source_profile, &materialization, &output)?;
                println!("{} matches its materialization inputs", output.display());
                Ok(())
            }
            ProfileCommand::MaterializeFlink(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                write_flink_profile(&source_profile, &materialization, &output)?;
                println!("wrote {}", output.display());
                Ok(())
            }
            ProfileCommand::CheckFlink(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                check_flink_profile(&source_profile, &materialization, &output)?;
                println!("{} matches its materialization inputs", output.display());
                Ok(())
            }
            ProfileCommand::MaterializeTrino(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                write_trino_profile(&source_profile, &materialization, &output)?;
                println!("wrote {}", output.display());
                Ok(())
            }
            ProfileCommand::CheckTrino(ProfileFiles {
                source_profile,
                materialization,
                output,
            }) => {
                check_trino_profile(&source_profile, &materialization, &output)?;
                println!("{} matches its materialization inputs", output.display());
                Ok(())
            }
        },
        Command::HistoricalImport { command } => match command {
            HistoricalImportCommand::Write { root } => {
                let manifest = write_historical_commit_bundle(&root)?;
                validate_bundle(&manifest)
            }
            HistoricalImportCommand::Check { root } => {
                let manifest = check_historical_commit_bundle(&root)?;
                validate_bundle(&manifest)
            }
        },
        Command::ContentionImport { command } => match command {
            ContentionImportCommand::Write { root } => {
                let manifest = write_contention_result_bundle(&root)?;
                validate_bundle(&manifest)
            }
            ContentionImportCommand::Check { root } => {
                let manifest = check_contention_result_bundle(&root)?;
                validate_bundle(&manifest)
            }
        },
        Command::EngineEvidence { command } => match command {
            EngineEvidenceCommand::Validate(EngineEvidenceFiles {
                profile,
                scenario,
                evidence_directory,
                fixture_id,
            }) => {
                let evidence = validate_engine_evidence_set(
                    &profile,
                    &scenario,
                    &evidence_directory,
                    &fixture_id,
                )?;
                let summary = evidence.summary();
                println!(
                    "valid engine evidence {}: {} transcript(s), {} pass, {} fail, {} fixture collision",
                    evidence_directory.display(),
                    summary.total,
                    summary.pass,
                    summary.fail,
                    summary.fixture_collision
                );
                Ok(())
            }
            EngineEvidenceCommand::ValidateReview(EngineReviewFile { root, review }) => {
                let validated = validate_engine_result_review(&root, &review)?;
                let summary = validated.evidence().summary();
                println!(
                    "valid reviewed engine evidence {}: bundle {}, {} transcript(s), {} pass, {} fail, {} fixture collision",
                    review.display(),
                    validated.bundle_id(),
                    summary.total,
                    summary.pass,
                    summary.fail,
                    summary.fixture_collision
                );
                Ok(())
            }
        },
        Command::EngineImport { command } => match command {
            EngineImportCommand::Write(EngineReviewFile { root, review }) => {
                let manifest = write_engine_result_bundle(&root, &review)?;
                validate_bundle(&manifest)
            }
            EngineImportCommand::Check(EngineReviewFile { root, review }) => {
                let manifest = check_engine_result_bundle(&root, &review)?;
                validate_bundle(&manifest)
            }
        },
        Command::Publication { command } => match command {
            PublicationCommand::Write(files) => {
                let profile = publication_profile(&files.profile)?;
                write_publication(&files.root, profile)?;
                println!("wrote and verified cross-scenario publication");
                Ok(())
            }
            PublicationCommand::Check(files) => {
                let profile = publication_profile(&files.profile)?;
                check_publication(&files.root, profile)?;
                println!("valid cross-scenario publication ({})", files.profile);
                Ok(())
            }
        },
        Command::Phase1Import { command } => match command {
            Phase1ImportCommand::Write { root } => {
                let manifest = write_phase1_result_bundle(&root)?;
                validate_bundle(&manifest)
            }
            Phase1ImportCommand::Check { root } => {
                let manifest = check_phase1_result_bundle(&root)?;
                validate_bundle(&manifest)
            }
        },
    }
}

fn publication_profile(value: &str) -> Result<PublicationProfile> {
    match value {
        "smoke" => Ok(PublicationProfile::Smoke),
        "full" => Ok(PublicationProfile::Full),
        value => bail!("unsupported publication profile `{value}`"),
    }
}

fn validate_bundle(manifest: &Path) -> Result<()> {
    let bundle = load_bundle(manifest)?;
    println!(
        "valid bundle {}: {} scenario(s), {} result(s)",
        manifest.display(),
        bundle.scenarios().len(),
        bundle.results().len()
    );
    Ok(())
}

fn write_matrix(manifest: &Path, output: &Path) -> Result<()> {
    let rendered = render_matrix(&load_bundle(manifest)?)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(output, rendered).with_context(|| format!("failed to write {}", output.display()))?;
    println!("wrote {}", output.display());
    Ok(())
}

fn check_matrix(manifest: &Path, output: &Path) -> Result<()> {
    let expected = render_matrix(&load_bundle(manifest)?)?;
    let actual = fs::read_to_string(output)
        .with_context(|| format!("failed to read {}", output.display()))?;
    if actual != expected {
        bail!(
            "{} is stale; rerun `catalog-bench-contract matrix write`",
            output.display()
        );
    }
    println!("{} matches its validated result bundle", output.display());
    Ok(())
}

fn write_schemas(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;
    for schema in generated_schemas().context("failed to generate schemas")? {
        let path = directory.join(schema.file_name);
        let mut contents = serde_json::to_string_pretty(&schema.document)?;
        contents.push('\n');
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("wrote {} ({})", path.display(), schema.kind);
    }
    Ok(())
}

fn check_schemas(directory: &Path) -> Result<()> {
    let mut stale = Vec::new();
    for schema in generated_schemas().context("failed to generate schemas")? {
        let path = directory.join(schema.file_name);
        let contents =
            fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let checked_in: serde_json::Value = serde_json::from_slice(&contents)
            .with_context(|| format!("{} is not valid JSON", path.display()))?;
        if checked_in != schema.document {
            stale.push(path);
        }
    }
    if stale.is_empty() {
        println!("all checked-in schemas match the Rust contract types");
        Ok(())
    } else {
        let paths = stale
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("stale schemas: {paths}; run `catalog-bench-contract schemas write`")
    }
}

fn validate_paths(paths: &[PathBuf]) -> Result<()> {
    let mut files = Vec::new();
    for path in paths {
        collect_json_files(path, &mut files)?;
    }
    files.sort();
    files.dedup();
    if files.is_empty() {
        bail!("no JSON files found");
    }

    let mut failures = Vec::new();
    for path in &files {
        let contents =
            fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        match parse_contract(&contents) {
            Ok(document) => println!("valid {} ({})", path.display(), document.kind()),
            Err(error) => failures.push(format!("{}: {error}", path.display())),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("\n"))
    }
}

fn collect_json_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            files.push(path.to_owned());
        }
        return Ok(());
    }
    if !path.is_dir() {
        bail!("{} is neither a file nor a directory", path.display());
    }
    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        collect_json_files(&entry.path(), files)?;
    }
    Ok(())
}
