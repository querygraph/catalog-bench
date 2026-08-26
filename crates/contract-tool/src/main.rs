use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{generated_schemas, parse_contract};
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
    }
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
