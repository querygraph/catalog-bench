use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{generated_schemas, parse_contract};
use catalog_bench_contract::{
    check_contention_profile, check_historical_commit_bundle, load_bundle, render_commit_matrix,
    write_contention_profile, write_historical_commit_bundle,
};
use clap::{Parser, Subcommand};

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
    /// Derive a runnable contention profile from a draft and image observations.
    MaterializeContention {
        #[arg(long)]
        source_profile: PathBuf,
        #[arg(long)]
        materialization: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Check that a runnable contention profile exactly matches its inputs.
    CheckContention {
        #[arg(long)]
        source_profile: PathBuf,
        #[arg(long)]
        materialization: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
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
            ProfileCommand::MaterializeContention {
                source_profile,
                materialization,
                output,
            } => {
                write_contention_profile(&source_profile, &materialization, &output)?;
                println!("wrote {}", output.display());
                Ok(())
            }
            ProfileCommand::CheckContention {
                source_profile,
                materialization,
                output,
            } => {
                check_contention_profile(&source_profile, &materialization, &output)?;
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
    let rendered = render_commit_matrix(&load_bundle(manifest)?)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(output, rendered).with_context(|| format!("failed to write {}", output.display()))?;
    println!("wrote {}", output.display());
    Ok(())
}

fn check_matrix(manifest: &Path, output: &Path) -> Result<()> {
    let expected = render_commit_matrix(&load_bundle(manifest)?)?;
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
