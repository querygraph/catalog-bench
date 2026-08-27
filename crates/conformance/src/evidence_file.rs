use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use tempfile::Builder as TempFileBuilder;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;

/// Fixed stages at which durable evidence publication can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceWriteFailureKind {
    CreateParent,
    CreateTemporaryFile,
    Write,
    SyncFile,
    Publish,
    RemoveTemporaryFile,
    SyncParent,
}

/// A bounded evidence-publication failure with its underlying I/O cause.
#[derive(Debug)]
pub struct EvidenceWriteFailure {
    kind: EvidenceWriteFailureKind,
    source: io::Error,
}

impl EvidenceWriteFailure {
    fn new(kind: EvidenceWriteFailureKind, source: io::Error) -> Self {
        Self { kind, source }
    }

    #[must_use]
    pub fn kind(&self) -> EvidenceWriteFailureKind {
        self.kind
    }
}

impl Display for EvidenceWriteFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "evidence publication failed at stage {:?}",
            self.kind
        )
    }
}

impl Error for EvidenceWriteFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Publish complete evidence at a new path without exposing partial contents.
///
/// Bytes are written and synchronized through a temporary file in the target
/// directory. A same-filesystem hard link then gives the complete file its
/// public name only if that name does not already exist. The temporary name is
/// removed when this function returns.
pub fn write_new_evidence(path: &Path, bytes: &[u8]) -> Result<(), EvidenceWriteFailure> {
    let parent = output_parent(path);
    fs::create_dir_all(&parent).map_err(|error| {
        EvidenceWriteFailure::new(EvidenceWriteFailureKind::CreateParent, error)
    })?;
    let mut builder = TempFileBuilder::new();
    builder.prefix(".catalog-bench-evidence-");
    #[cfg(unix)]
    builder.permissions(fs::Permissions::from_mode(0o666));
    let mut temporary = builder.tempfile_in(&parent).map_err(|error| {
        EvidenceWriteFailure::new(EvidenceWriteFailureKind::CreateTemporaryFile, error)
    })?;
    temporary
        .write_all(bytes)
        .map_err(|error| EvidenceWriteFailure::new(EvidenceWriteFailureKind::Write, error))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| EvidenceWriteFailure::new(EvidenceWriteFailureKind::SyncFile, error))?;
    fs::hard_link(temporary.path(), path)
        .map_err(|error| EvidenceWriteFailure::new(EvidenceWriteFailureKind::Publish, error))?;
    temporary.close().map_err(|error| {
        EvidenceWriteFailure::new(EvidenceWriteFailureKind::RemoveTemporaryFile, error)
    })?;
    sync_parent(&parent)?;
    Ok(())
}

fn output_parent(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_owned()
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> Result<(), EvidenceWriteFailure> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| EvidenceWriteFailure::new(EvidenceWriteFailureKind::SyncParent, error))
}

#[cfg(not(unix))]
fn sync_parent(_parent: &Path) -> Result<(), EvidenceWriteFailure> {
    Ok(())
}
