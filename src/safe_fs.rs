use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::RawFd;
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow, bail};
use rustix::fd::{AsFd, OwnedFd};
use rustix::fs::{AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags};
use serde::{Deserialize, Serialize};

const REMOVE_QUARANTINE_PREFIX: &str = ".telegram-video-downloader-remove";
const REMOVE_QUARANTINE_MANIFEST_NAME: &str = "manifest.json";
const REMOVE_QUARANTINE_TOMBSTONE_SUFFIX: &str = ".cleanup.json";
const REMOVE_QUARANTINE_MANIFEST_VERSION: u32 = 3;
const REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION: u32 = 2;
const REMOVE_QUARANTINE_LEGACY_MANIFEST_VERSION: u32 = 1;
const REMOVE_QUARANTINE_MANIFEST_LIMIT: usize = 1024;
static REMOVE_QUARANTINE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntryIdentity {
    device: u64,
    inode: u64,
    file_type: FileType,
}

impl EntryIdentity {
    pub(crate) fn is_file(self) -> bool {
        self.file_type == FileType::RegularFile
    }

    pub(crate) fn is_dir(self) -> bool {
        self.file_type == FileType::Directory
    }

    pub(crate) fn device(self) -> u64 {
        self.device
    }

    pub(crate) fn inode(self) -> u64 {
        self.inode
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RootedFs {
    logical_root: PathBuf,
    canonical_root: PathBuf,
    root_fd: Arc<OwnedFd>,
    root_identity: EntryIdentity,
}

#[derive(Clone, Debug)]
struct BoundParent {
    fd: Arc<OwnedFd>,
    relative_path: PathBuf,
    identity: EntryIdentity,
}

#[derive(Clone, Debug)]
pub(crate) struct BoundEntry {
    path: PathBuf,
    parent: BoundParent,
    leaf: OsString,
}

#[derive(Clone)]
pub(crate) struct BoundFile {
    fd: Arc<OwnedFd>,
    identity: EntryIdentity,
}

#[derive(Clone)]
pub(crate) struct BoundDirectory {
    fd: Arc<OwnedFd>,
    identity: EntryIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AtomicBoundFileReplaceCheckpoint {
    BeforeExchange,
    AfterExchange,
}

#[derive(Debug)]
pub(crate) struct RemoveQuarantineRecoveryReport {
    pub(crate) messages: Vec<String>,
    pub(crate) unresolved: bool,
    pub(crate) restored: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoveQuarantineManifest {
    version: u32,
    quarantine_name: String,
    #[serde(default)]
    quarantine_device: Option<u64>,
    #[serde(default)]
    quarantine_inode: Option<u64>,
    parent_device: u64,
    parent_inode: u64,
    entry_device: u64,
    entry_inode: u64,
    entry_is_directory: bool,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    original_name_hex: Option<String>,
    #[serde(default)]
    restore_requires_revalidation: bool,
}

#[derive(Debug)]
struct PrivateRemoveQuarantine {
    name: OsString,
    directory: OwnedFd,
    identity: EntryIdentity,
    manifest: RemoveQuarantineManifest,
    manifest_identity: EntryIdentity,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PostQuarantineFailure {
    Retain,
    Restore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoveQuarantineRecoveryAction {
    Deleted,
    Restored,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemoveQuarantineScanDepth {
    CurrentDirectory,
    Recursive,
}

#[derive(Default)]
struct RemoveQuarantineScanState {
    messages: Vec<String>,
    unresolved: bool,
    restored: bool,
}

#[derive(Clone, Copy)]
struct RemoveQuarantinePolicy {
    recursive: bool,
    post_failure: PostQuarantineFailure,
}

impl RemoveQuarantinePolicy {
    fn retain(recursive: bool) -> Self {
        Self {
            recursive,
            post_failure: PostQuarantineFailure::Retain,
        }
    }

    fn restore_file() -> Self {
        Self {
            recursive: false,
            post_failure: PostQuarantineFailure::Restore,
        }
    }
}

impl std::fmt::Debug for BoundFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundFile")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for BoundDirectory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundDirectory")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl PartialEq for BoundFile {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for BoundFile {}

impl BoundFile {
    pub(crate) fn identity(&self) -> EntryIdentity {
        self.identity
    }

    pub(crate) fn validate_identity(&self) -> Result<()> {
        if identity_for_fd(self.fd.as_ref())? != self.identity {
            bail!("bound file descriptor identity changed");
        }
        Ok(())
    }

    pub(crate) fn validate_private_single_link(&self, mode: u16) -> Result<()> {
        self.validate_private_link_count(mode, 1)
    }

    pub(crate) fn validate_private_link_count(&self, mode: u16, link_count: u16) -> Result<()> {
        self.validate_identity()?;
        let stat = rustix::fs::fstat(self.fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to inspect private bound file")?;
        if identity_from_stat(&stat) != self.identity
            || stat.st_mode & 0o777 != mode
            || stat.st_uid != unsafe { libc::geteuid() }
            || stat.st_nlink != link_count
        {
            bail!("private bound file ownership, permissions, or link count changed");
        }
        Ok(())
    }

    pub(crate) fn validate_private_unlinked(&self, mode: u16) -> Result<()> {
        self.validate_identity()?;
        let stat = rustix::fs::fstat(self.fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to inspect private unlinked bound file")?;
        if identity_from_stat(&stat) != self.identity
            || stat.st_mode & 0o777 != mode
            || stat.st_uid != unsafe { libc::geteuid() }
            || stat.st_nlink != 0
        {
            bail!("private unlinked bound file ownership, permissions, or link count changed");
        }
        Ok(())
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        self.validate_identity()?;
        rustix::fs::fsync(self.fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to sync bound file")?;
        self.validate_identity()
    }

    pub(crate) fn set_mode(&self, mode: u16) -> Result<()> {
        self.validate_identity()?;
        rustix::fs::fchmod(self.fd.as_ref(), Mode::from_raw_mode(mode))
            .map_err(errno_to_io)
            .context("failed to set bound file permissions")?;
        let stat = rustix::fs::fstat(self.fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to revalidate bound file permissions")?;
        if identity_from_stat(&stat) != self.identity || stat.st_mode & 0o777 != mode {
            bail!("bound file identity or permissions changed");
        }
        Ok(())
    }

    pub(crate) fn byte_len(&self) -> Result<u64> {
        self.validate_identity()?;
        let stat = rustix::fs::fstat(self.fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to inspect bound file length")?;
        if identity_from_stat(&stat) != self.identity {
            bail!("bound file descriptor identity changed");
        }
        u64::try_from(stat.st_size).context("bound file has a negative length")
    }

    pub(crate) fn read_limited(&self, limit: usize) -> Result<Vec<u8>> {
        self.validate_identity()?;
        let duplicate = rustix::io::dup(self.fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to duplicate bound file descriptor")?;
        let mut reader = File::from(duplicate).take((limit as u64).saturating_add(1));
        reader
            .get_mut()
            .seek(SeekFrom::Start(0))
            .context("failed to seek bound file")?;
        let mut contents = Vec::new();
        reader
            .read_to_end(&mut contents)
            .context("failed to read bound file")?;
        if contents.len() > limit {
            bail!("bound file exceeds the {limit}-byte read limit");
        }
        self.validate_identity()?;
        Ok(contents)
    }

    #[cfg(unix)]
    pub(crate) fn duplicate_fd_cloexec_at_least(&self, minimum: RawFd) -> Result<OwnedFd> {
        self.validate_identity()?;
        let duplicate = rustix::io::fcntl_dupfd_cloexec(self.fd.as_ref(), minimum)
            .map_err(errno_to_io)
            .context("failed to duplicate bound file descriptor for a child process")?;
        self.validate_identity()?;
        Ok(duplicate)
    }

    pub(crate) fn lock_exclusive(&self) -> Result<()> {
        rustix::fs::flock(self.fd.as_ref(), FlockOperation::LockExclusive)
            .map_err(errno_to_io)
            .context("failed to lock bound file")
    }

    pub(crate) fn try_lock_exclusive(&self) -> Result<bool> {
        match rustix::fs::flock(self.fd.as_ref(), FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(true),
            Err(err) if errno_to_io(err).kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(err) => Err(errno_to_io(err)).context("failed to lock bound file"),
        }
    }
}

impl BoundDirectory {
    pub(crate) fn validate_identity(&self) -> Result<()> {
        if identity_for_fd(self.fd.as_ref())? != self.identity {
            bail!("bound directory descriptor identity changed");
        }
        Ok(())
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        self.validate_identity()?;
        sync_directory(self.fd.as_ref())?;
        self.validate_identity()
    }

    #[cfg(unix)]
    pub(crate) fn duplicate_fd_cloexec_at_least(&self, minimum: RawFd) -> Result<OwnedFd> {
        self.validate_identity()?;
        let duplicate = rustix::io::fcntl_dupfd_cloexec(self.fd.as_ref(), minimum)
            .map_err(errno_to_io)
            .context("failed to duplicate bound directory descriptor for a child process")?;
        self.validate_identity()?;
        Ok(duplicate)
    }
}

impl BoundEntry {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl RootedFs {
    pub(crate) fn new(root: &Path) -> Result<Self> {
        let canonical_root = std::fs::canonicalize(root)
            .with_context(|| format!("failed to resolve output root {}", root.display()))?;
        let root_fd = open_directory(&canonical_root)
            .with_context(|| format!("failed to open output root {}", canonical_root.display()))?;
        let root_identity = identity_for_fd(&root_fd)?;
        if !root_identity.is_dir() {
            bail!(
                "configured output root is not a directory: {}",
                root.display()
            );
        }
        Ok(Self {
            logical_root: root.to_path_buf(),
            canonical_root,
            root_fd: Arc::new(root_fd),
            root_identity,
        })
    }

    pub(crate) fn root_path(&self) -> &Path {
        &self.canonical_root
    }

    pub(crate) fn root_identity(&self) -> EntryIdentity {
        self.root_identity
    }

    pub(crate) fn logical_root_path(&self) -> &Path {
        &self.logical_root
    }

    pub(crate) fn validate_configured_root(&self) -> Result<()> {
        self.validate_root()
    }

    #[cfg(test)]
    pub(crate) fn reconcile_remove_quarantines(&self) -> Result<Vec<String>> {
        Ok(self.reconcile_remove_quarantines_with_status()?.messages)
    }

    pub(crate) fn reconcile_remove_quarantines_with_status(
        &self,
    ) -> Result<RemoveQuarantineRecoveryReport> {
        self.reconcile_remove_quarantines_with_status_and_restore_decider(|_| {
            bail!("interrupted validated removal requires caller revalidation")
        })
    }

    pub(crate) fn reconcile_remove_quarantines_with_status_and_restore_decider<F>(
        &self,
        should_restore: F,
    ) -> Result<RemoveQuarantineRecoveryReport>
    where
        F: FnMut(EntryIdentity) -> Result<bool>,
    {
        self.reconcile_remove_quarantines_with_status_and_restore_decider_at_depth(
            should_restore,
            RemoveQuarantineScanDepth::Recursive,
        )
    }

    pub(crate) fn reconcile_remove_quarantines_in_current_directory_with_status_and_restore_decider<
        F,
    >(
        &self,
        should_restore: F,
    ) -> Result<RemoveQuarantineRecoveryReport>
    where
        F: FnMut(EntryIdentity) -> Result<bool>,
    {
        self.reconcile_remove_quarantines_with_status_and_restore_decider_at_depth(
            should_restore,
            RemoveQuarantineScanDepth::CurrentDirectory,
        )
    }

    fn reconcile_remove_quarantines_with_status_and_restore_decider_at_depth<F>(
        &self,
        mut should_restore: F,
        scan_depth: RemoveQuarantineScanDepth,
    ) -> Result<RemoveQuarantineRecoveryReport>
    where
        F: FnMut(EntryIdentity) -> Result<bool>,
    {
        self.validate_root()?;
        let mut state = RemoveQuarantineScanState::default();
        reconcile_remove_quarantines_in_directory(
            self.root_fd.as_ref(),
            self.root_identity,
            &self.logical_root,
            &mut state,
            &mut should_restore,
            scan_depth,
        )?;
        self.validate_root()?;
        Ok(RemoveQuarantineRecoveryReport {
            messages: state.messages,
            unresolved: state.unresolved,
            restored: state.restored,
        })
    }

    pub(crate) fn bind_entry(&self, path: &Path, create_parent: bool) -> Result<BoundEntry> {
        let (parent_path, leaf) = self.split_parent(path)?;
        let parent = self.open_relative_directory(&parent_path, create_parent)?;
        Ok(BoundEntry {
            path: path.to_path_buf(),
            parent,
            leaf,
        })
    }

    pub(crate) fn bound_entry_identity(&self, entry: &BoundEntry) -> Result<Option<EntryIdentity>> {
        self.validate_bound_parent(&entry.parent)?;
        identity_at(entry.parent.fd.as_ref(), &entry.leaf)
    }

    pub(crate) fn open_bound_file(&self, path: &Path) -> Result<Option<BoundFile>> {
        let entry = self.bind_entry(path, false)?;
        let fd = match rustix::fs::openat(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(err) if err == rustix::io::Errno::NOENT => return Ok(None),
            Err(err) => {
                return Err(errno_to_io(err)).with_context(|| {
                    format!("failed to open bound file {}", entry.path.display())
                });
            }
        };
        let identity = identity_for_fd(&fd)?;
        if !identity.is_file() {
            bail!("bound path is not a regular file: {}", entry.path.display());
        }
        if identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(identity) {
            bail!(
                "bound file identity changed while opening: {}",
                entry.path.display()
            );
        }
        self.validate_parent(&entry.parent)?;
        Ok(Some(BoundFile {
            fd: Arc::new(fd),
            identity,
        }))
    }

    pub(crate) fn open_bound_file_read_write_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<BoundFile> {
        // Protected property: the write-capable descriptor must bind the exact regular-file
        // object selected by the caller while its configured-root access path remains stable.
        self.validate_parent(&entry.parent)?;
        if !expected.is_file()
            || identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(expected)
        {
            bail!(
                "bound file identity changed before read-write open: {}",
                entry.path.display()
            );
        }
        let fd = rustix::fs::openat(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)
        .with_context(|| {
            format!(
                "failed to open bound file for writing {}",
                entry.path.display()
            )
        })?;
        if identity_for_fd(&fd)? != expected
            || identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(expected)
        {
            bail!(
                "bound file identity changed during read-write open: {}",
                entry.path.display()
            );
        }
        self.validate_parent(&entry.parent)?;
        Ok(BoundFile {
            fd: Arc::new(fd),
            identity: expected,
        })
    }

    pub(crate) fn open_bound_directory(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<BoundDirectory> {
        self.validate_bound_parent(&entry.parent)?;
        if identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(expected)
            || !expected.is_dir()
        {
            bail!("bound directory identity changed: {}", entry.path.display());
        }
        let fd = openat_directory(entry.parent.fd.as_ref(), &entry.leaf)
            .with_context(|| format!("failed to open bound directory {}", entry.path.display()))?;
        if identity_for_fd(&fd)? != expected {
            bail!("bound directory identity changed: {}", entry.path.display());
        }
        self.validate_bound_parent(&entry.parent)?;
        Ok(BoundDirectory {
            fd: Arc::new(fd),
            identity: expected,
        })
    }

    pub(crate) fn create_new_bound_file(
        &self,
        path: &Path,
        contents: &[u8],
        mode: u16,
    ) -> Result<(BoundEntry, EntryIdentity)> {
        let entry = self.bind_entry(path, false)?;
        let fd = rustix::fs::openat(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::from_raw_mode(mode),
        )
        .map_err(errno_to_io)
        .with_context(|| format!("failed to create bound file {}", path.display()))?;
        let identity = identity_for_fd(&fd)?;
        let mut file = File::from(fd);
        if let Err(err) = file.write_all(contents).and_then(|()| file.sync_all()) {
            drop(file);
            let cleanup = self.remove_bound_file_if_identity(&entry, identity);
            return Err(with_cleanup_error(
                anyhow!(err).context(format!("failed to persist bound file {}", path.display())),
                cleanup,
                "bound file cleanup",
            ));
        }
        if identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(identity) {
            bail!("created bound file identity changed: {}", path.display());
        }
        self.validate_parent(&entry.parent)?;
        sync_directory(entry.parent.fd.as_ref())?;
        Ok((entry, identity))
    }

    pub(crate) fn replace_bound_file_atomically_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        temp_path: &Path,
        contents: &[u8],
        mode: u16,
    ) -> Result<(BoundEntry, EntryIdentity)> {
        self.replace_bound_file_atomically_if_identity_with_hook(
            entry,
            expected,
            temp_path,
            contents,
            mode,
            &mut |_| Ok(()),
        )
    }

    fn replace_bound_file_atomically_if_identity_with_hook<F>(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        temp_path: &Path,
        contents: &[u8],
        mode: u16,
        hook: &mut F,
    ) -> Result<(BoundEntry, EntryIdentity)>
    where
        F: FnMut(AtomicBoundFileReplaceCheckpoint) -> Result<()>,
    {
        self.validate_bound_parent(&entry.parent)?;
        if identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(expected) {
            bail!(
                "bound file identity changed before atomic replacement: {}",
                entry.path.display()
            );
        }

        let (temp_entry, temp_identity) = self.create_new_bound_file(temp_path, contents, mode)?;
        if entry.parent.identity != temp_entry.parent.identity {
            let cleanup = self.remove_bound_file_if_identity(&temp_entry, temp_identity);
            return Err(with_cleanup_error(
                anyhow!(
                    "atomic replacement requires a temporary file in the destination directory: {}",
                    temp_path.display()
                ),
                cleanup,
                "atomic replacement temp cleanup",
            ));
        }
        if let Err(err) = hook(AtomicBoundFileReplaceCheckpoint::BeforeExchange) {
            let cleanup = self.remove_bound_file_if_identity(&temp_entry, temp_identity);
            return Err(with_cleanup_error(
                err.context("atomic replacement stopped before exchange"),
                cleanup,
                "atomic replacement temp cleanup",
            ));
        }
        if identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(expected) {
            let cleanup = self.remove_bound_file_if_identity(&temp_entry, temp_identity);
            return Err(with_cleanup_error(
                anyhow!(
                    "bound file identities changed during atomic replacement: {}",
                    entry.path.display()
                ),
                cleanup,
                "atomic replacement temp cleanup",
            ));
        }
        if let Err(err) = renameat_exchange(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            temp_entry.parent.fd.as_ref(),
            &temp_entry.leaf,
        ) {
            let cleanup = self.remove_bound_file_if_identity(&temp_entry, temp_identity);
            return Err(with_cleanup_error(
                err.context(format!(
                    "failed to atomically replace bound file {}",
                    entry.path.display()
                )),
                cleanup,
                "atomic replacement temp cleanup",
            ));
        }

        let post_exchange_error = hook(AtomicBoundFileReplaceCheckpoint::AfterExchange).err();
        let destination_identity = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?;
        let displaced_identity = identity_at(temp_entry.parent.fd.as_ref(), &temp_entry.leaf)?;
        if post_exchange_error.is_some()
            || destination_identity != Some(temp_identity)
            || displaced_identity != Some(expected)
        {
            let failure = post_exchange_error.unwrap_or_else(|| {
                anyhow!(
                    "bound file identities changed during atomic replacement: {}",
                    entry.path.display()
                )
            });
            let still_provable = destination_identity == Some(temp_identity)
                && displaced_identity == Some(expected)
                && identity_at(entry.parent.fd.as_ref(), &entry.leaf)? == Some(temp_identity)
                && identity_at(temp_entry.parent.fd.as_ref(), &temp_entry.leaf)? == Some(expected);
            if !still_provable {
                bail!(
                    "{failure:#}; retained both atomic replacement entries because their post-exchange identities could not be proven"
                );
            }
            if let Err(rollback) = renameat_exchange(
                entry.parent.fd.as_ref(),
                &entry.leaf,
                temp_entry.parent.fd.as_ref(),
                &temp_entry.leaf,
            ) {
                bail!(
                    "{failure:#}; failed to roll back the authenticated atomic exchange: {rollback:#}; retained both entries"
                );
            }
            let restored_destination = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?;
            let restored_temp = identity_at(temp_entry.parent.fd.as_ref(), &temp_entry.leaf)?;
            if restored_destination != displaced_identity || restored_temp != Some(temp_identity) {
                bail!(
                    "{failure:#}; atomic exchange rollback outcome could not be authenticated; retained both entries"
                );
            }
            if let Err(sync) = sync_directory(entry.parent.fd.as_ref()) {
                bail!(
                    "{failure:#}; failed to persist the authenticated atomic exchange rollback: {sync:#}; retained the replacement temp"
                );
            }
            let cleanup = self.remove_bound_file_if_identity(&temp_entry, temp_identity);
            return Err(with_cleanup_error(
                failure,
                cleanup,
                "rolled-back atomic replacement temp cleanup",
            ));
        }
        self.validate_bound_parent(&entry.parent)?;
        sync_directory(entry.parent.fd.as_ref())?;
        self.remove_bound_file_if_identity(&temp_entry, expected)?;
        Ok((entry.clone(), temp_identity))
    }

    pub(crate) fn hard_link_via_bound_parents_noreplace_if_identity(
        &self,
        source: &BoundEntry,
        destination: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.validate_bound_parent(&source.parent)?;
        self.validate_bound_parent(&destination.parent)?;
        if identity_at(source.parent.fd.as_ref(), &source.leaf)? != Some(expected) {
            bail!(
                "bound hard-link source identity changed: {}",
                source.path.display()
            );
        }
        if !expected.is_file() {
            bail!(
                "bound hard-link source is not a regular file: {}",
                source.path.display()
            );
        }
        if identity_at(destination.parent.fd.as_ref(), &destination.leaf)?.is_some() {
            bail!(
                "bound hard-link destination already exists: {}",
                destination.path.display()
            );
        }
        rustix::fs::linkat(
            source.parent.fd.as_ref(),
            &source.leaf,
            destination.parent.fd.as_ref(),
            &destination.leaf,
            AtFlags::empty(),
        )
        .map_err(errno_to_io)
        .with_context(|| {
            format!(
                "failed to hard-link {} to {} through bound parents",
                source.path.display(),
                destination.path.display()
            )
        })?;

        let source_current = identity_at(source.parent.fd.as_ref(), &source.leaf)?;
        let destination_current = identity_at(destination.parent.fd.as_ref(), &destination.leaf)?;
        if source_current != Some(expected) || destination_current != Some(expected) {
            let cleanup = if destination_current == Some(expected) {
                self.remove_bound_file_if_identity(destination, expected)
            } else {
                Ok(())
            };
            return Err(with_cleanup_error(
                anyhow!(
                    "bound hard-link identities changed while linking {}",
                    destination.path.display()
                ),
                cleanup,
                "bound hard-link cleanup",
            ));
        }
        self.validate_bound_parent(&source.parent)?;
        self.validate_bound_parent(&destination.parent)?;
        sync_directory(destination.parent.fd.as_ref())
    }

    pub(crate) fn ensure_hard_link_via_bound_parents_if_identity(
        &self,
        source: &BoundEntry,
        destination: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.validate_bound_parent(&source.parent)?;
        self.validate_bound_parent(&destination.parent)?;
        if identity_at(source.parent.fd.as_ref(), &source.leaf)? != Some(expected)
            || !expected.is_file()
        {
            bail!(
                "bound hard-link source identity changed: {}",
                source.path.display()
            );
        }

        match identity_at(destination.parent.fd.as_ref(), &destination.leaf)? {
            Some(current) if current != expected => {
                bail!(
                    "bound hard-link destination has a different identity: {}",
                    destination.path.display()
                );
            }
            Some(_) => {}
            None => match rustix::fs::linkat(
                source.parent.fd.as_ref(),
                &source.leaf,
                destination.parent.fd.as_ref(),
                &destination.leaf,
                AtFlags::empty(),
            ) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(err) => {
                    return Err(errno_to_io(err)).with_context(|| {
                        format!(
                            "failed to hard-link {} to {} through bound parents",
                            source.path.display(),
                            destination.path.display()
                        )
                    });
                }
            },
        }

        if identity_at(source.parent.fd.as_ref(), &source.leaf)? != Some(expected)
            || identity_at(destination.parent.fd.as_ref(), &destination.leaf)? != Some(expected)
        {
            bail!(
                "bound hard-link identities changed while ensuring {}",
                destination.path.display()
            );
        }
        self.validate_bound_parent(&source.parent)?;
        self.validate_bound_parent(&destination.parent)?;
        sync_directory(destination.parent.fd.as_ref())
    }

    pub(crate) fn list_bound_directory(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<Vec<(OsString, EntryIdentity)>> {
        self.validate_bound_parent(&entry.parent)?;
        let current = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?
            .ok_or_else(|| anyhow!("bound directory is missing: {}", entry.path.display()))?;
        if current != expected || !current.is_dir() {
            bail!("bound directory identity changed: {}", entry.path.display());
        }
        let directory = openat_directory(entry.parent.fd.as_ref(), &entry.leaf)
            .with_context(|| format!("failed to open bound directory {}", entry.path.display()))?;
        if identity_for_fd(&directory)? != expected {
            bail!("bound directory identity changed: {}", entry.path.display());
        }
        let names = rustix::fs::Dir::read_from(&directory)
            .map_err(errno_to_io)
            .with_context(|| format!("failed to read bound directory {}", entry.path.display()))?
            .map(|item| item.map_err(errno_to_io))
            .collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
        let mut entries = Vec::new();
        for item in names {
            let name = item.file_name();
            if matches!(name.to_bytes(), b"." | b"..") {
                continue;
            }
            let display_name = name.to_str().with_context(|| {
                format!(
                    "bound directory contains a non-UTF-8 entry: {}",
                    entry.path.display()
                )
            })?;
            let identity = identity_at_cstr(&directory, name)?.ok_or_else(|| {
                anyhow!(
                    "bound directory entry disappeared: {}",
                    entry.path.join(display_name).display()
                )
            })?;
            entries.push((OsString::from(display_name), identity));
        }
        self.validate_bound_parent(&entry.parent)?;
        Ok(entries)
    }

    pub(crate) fn validate_private_bound_directory(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        mode: u16,
    ) -> Result<()> {
        self.validate_bound_parent(&entry.parent)?;
        if identity_at(entry.parent.fd.as_ref(), &entry.leaf)? != Some(expected)
            || !expected.is_dir()
        {
            bail!(
                "private bound directory identity changed: {}",
                entry.path.display()
            );
        }
        let directory =
            openat_directory(entry.parent.fd.as_ref(), &entry.leaf).with_context(|| {
                format!("failed to open private directory {}", entry.path.display())
            })?;
        let stat = rustix::fs::fstat(&directory)
            .map_err(errno_to_io)
            .with_context(|| {
                format!(
                    "failed to inspect private directory {}",
                    entry.path.display()
                )
            })?;
        if identity_for_fd(&directory)? != expected
            || stat.st_mode & 0o777 != mode
            || stat.st_uid != unsafe { libc::geteuid() }
        {
            bail!(
                "private bound directory ownership or permissions changed: {}",
                entry.path.display()
            );
        }
        self.validate_bound_parent(&entry.parent)
    }

    pub(crate) fn rename_via_bound_parents_noreplace_if_identity(
        &self,
        source: &BoundEntry,
        destination: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        // Rollback follows directory-object identity captured beneath the bound root. Path or
        // timestamp changes cannot redirect it to a replacement directory.
        self.validate_bound_parent(&source.parent)?;
        self.validate_bound_parent(&destination.parent)?;
        let source_identity = identity_at(source.parent.fd.as_ref(), &source.leaf)?
            .ok_or_else(|| anyhow!("bound move source is missing: {}", source.path.display()))?;
        if source_identity != expected {
            bail!(
                "bound move source identity changed: {}",
                source.path.display()
            );
        }
        if identity_at(destination.parent.fd.as_ref(), &destination.leaf)?.is_some() {
            bail!(
                "bound move destination already exists: {}",
                destination.path.display()
            );
        }
        renameat_noreplace(
            source.parent.fd.as_ref(),
            &source.leaf,
            destination.parent.fd.as_ref(),
            &destination.leaf,
        )
        .with_context(|| {
            format!(
                "failed to move {} to {} through bound parents",
                source.path.display(),
                destination.path.display()
            )
        })?;
        let validation = self.validate_bound_renamed_destination(destination, expected);
        if let Err(err) = validation {
            let rollback = rollback_bound_rename(source, destination, expected);
            return Err(with_cleanup_error(err, rollback, "bound move rollback"));
        }
        let durability = sync_directory(source.parent.fd.as_ref())
            .and_then(|()| sync_directory(destination.parent.fd.as_ref()));
        if let Err(err) = durability {
            let rollback = rollback_bound_rename(source, destination, expected);
            return Err(with_cleanup_error(
                err.context("failed to persist bound move"),
                rollback,
                "bound move durability rollback",
            ));
        }
        Ok(())
    }

    pub(crate) fn remove_bound_file_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.remove_bound_entry_if_identity(entry, expected, AtFlags::empty())
    }

    pub(crate) fn remove_bound_file_if_identity_with_validation<F>(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        validate_quarantined_identity: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        self.remove_bound_entry_if_identity_with_hooks(
            entry,
            expected,
            AtFlags::empty(),
            RemoveQuarantinePolicy::restore_file(),
            || {},
            validate_quarantined_identity,
        )
    }

    #[cfg(test)]
    pub(crate) fn leave_validated_file_removal_quarantined_for_test(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.validate_bound_parent(&entry.parent)?;
        if self.bound_entry_identity(entry)? != Some(expected) || !expected.is_file() {
            bail!("test validated-removal target identity changed");
        }
        let quarantine = create_private_remove_quarantine(
            &entry.parent,
            &entry.leaf,
            expected,
            RemoveQuarantinePolicy::restore_file(),
        )?;
        renameat_noreplace(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            &quarantine.directory,
            OsStr::new("entry"),
        )?;
        sync_directory(entry.parent.fd.as_ref())?;
        sync_directory(&quarantine.directory)?;
        if identity_at(&quarantine.directory, OsStr::new("entry"))? != Some(expected) {
            bail!("test validated-removal quarantine identity changed");
        }
        Ok(())
    }

    pub(crate) fn remove_bound_dir_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.remove_bound_entry_if_identity(entry, expected, AtFlags::REMOVEDIR)
    }

    pub(crate) fn remove_bound_tree_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.validate_bound_parent(&entry.parent)?;
        let current = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?
            .ok_or_else(|| anyhow!("bound directory is missing: {}", entry.path.display()))?;
        if current != expected || !current.is_dir() {
            bail!("bound directory identity changed: {}", entry.path.display());
        }

        let directory = openat_directory(entry.parent.fd.as_ref(), &entry.leaf)
            .with_context(|| format!("failed to open bound directory {}", entry.path.display()))?;
        if identity_for_fd(&directory)? != expected {
            bail!("bound directory identity changed: {}", entry.path.display());
        }
        remove_directory_contents(&directory, &entry.path)?;

        let current = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?
            .ok_or_else(|| anyhow!("bound directory is missing: {}", entry.path.display()))?;
        if current != expected {
            bail!("bound directory identity changed: {}", entry.path.display());
        }
        rustix::fs::unlinkat(entry.parent.fd.as_ref(), &entry.leaf, AtFlags::REMOVEDIR)
            .map_err(errno_to_io)
            .with_context(|| {
                format!("failed to remove bound directory {}", entry.path.display())
            })?;
        self.validate_bound_parent(&entry.parent)?;
        sync_directory(entry.parent.fd.as_ref())
    }

    pub(crate) fn remove_bound_tree_durably_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.remove_bound_entry_if_identity_with_hooks(
            entry,
            expected,
            AtFlags::REMOVEDIR,
            RemoveQuarantinePolicy::retain(true),
            || {},
            || Ok(()),
        )
    }

    pub(crate) fn entry_identity(&self, path: &Path) -> Result<Option<EntryIdentity>> {
        let (parent_path, leaf) = self.split_parent(path)?;
        let parent = self.open_relative_directory(&parent_path, false)?;
        let identity = identity_at(parent.fd.as_ref(), &leaf)?;
        self.validate_parent(&parent)?;
        Ok(identity)
    }

    pub(crate) fn entry_exists(&self, path: &Path) -> Result<bool> {
        Ok(self.entry_identity(path)?.is_some())
    }

    pub(crate) fn create_dir(&self, path: &Path, mode: u16) -> Result<Option<EntryIdentity>> {
        let (parent_path, leaf) = self.split_parent(path)?;
        let parent = self.open_relative_directory(&parent_path, false)?;
        self.validate_parent(&parent)?;
        match rustix::fs::mkdirat(parent.fd.as_ref(), &leaf, Mode::from_raw_mode(mode)) {
            Ok(()) => {}
            Err(err) if err == rustix::io::Errno::EXIST => return Ok(None),
            Err(err) => {
                return Err(errno_to_io(err))
                    .with_context(|| format!("failed to create directory {}", path.display()));
            }
        }
        let identity = identity_at(parent.fd.as_ref(), &leaf)?
            .ok_or_else(|| anyhow!("created directory disappeared: {}", path.display()))?;
        if !identity.is_dir() {
            bail!("created path is not a directory: {}", path.display());
        }
        if let Err(err) = self.validate_parent(&parent) {
            let cleanup = rustix::fs::unlinkat(parent.fd.as_ref(), &leaf, AtFlags::REMOVEDIR)
                .map_err(errno_to_io);
            return Err(with_cleanup_error(
                err,
                cleanup,
                "created directory rollback",
            ));
        }
        sync_directory(parent.fd.as_ref())?;
        Ok(Some(identity))
    }

    #[cfg(test)]
    pub(crate) fn rename_noreplace(
        &self,
        source: &Path,
        destination: &Path,
        create_destination_parents: bool,
    ) -> Result<EntryIdentity> {
        self.rename_noreplace_inner(source, destination, create_destination_parents)
    }

    #[cfg(test)]
    fn rename_noreplace_inner(
        &self,
        source: &Path,
        destination: &Path,
        create_destination_parents: bool,
    ) -> Result<EntryIdentity> {
        let (source_parent_path, source_leaf) = self.split_parent(source)?;
        let (destination_parent_path, destination_leaf) = self.split_parent(destination)?;
        let source_parent = self.open_relative_directory(&source_parent_path, false)?;
        let destination_parent =
            self.open_relative_directory(&destination_parent_path, create_destination_parents)?;
        self.validate_parent(&source_parent)?;
        self.validate_parent(&destination_parent)?;

        let source_identity = identity_at(source_parent.fd.as_ref(), &source_leaf)?
            .ok_or_else(|| anyhow!("move source is missing: {}", source.display()))?;
        if !source_identity.is_file() {
            bail!("move source is not a regular file: {}", source.display());
        }
        if identity_at(destination_parent.fd.as_ref(), &destination_leaf)?.is_some() {
            bail!("move destination already exists: {}", destination.display());
        }

        renameat_noreplace(
            source_parent.fd.as_ref(),
            &source_leaf,
            destination_parent.fd.as_ref(),
            &destination_leaf,
        )
        .with_context(|| {
            format!(
                "failed to move {} to {} without replacing an existing entry",
                source.display(),
                destination.display()
            )
        })?;

        let validation = self.validate_renamed_destination(
            destination,
            &destination_parent,
            &destination_leaf,
            source_identity,
        );
        if let Err(err) = validation {
            let rollback = renameat_noreplace(
                destination_parent.fd.as_ref(),
                &destination_leaf,
                source_parent.fd.as_ref(),
                &source_leaf,
            );
            return Err(with_cleanup_error(err, rollback, "move rollback"));
        }
        Ok(source_identity)
    }

    #[cfg(test)]
    fn validate_renamed_destination(
        &self,
        destination: &Path,
        bound_parent: &BoundParent,
        destination_leaf: &OsStr,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.validate_root()?;
        self.validate_parent(bound_parent)?;
        let bound_identity = identity_at(bound_parent.fd.as_ref(), destination_leaf)?
            .ok_or_else(|| anyhow!("moved destination disappeared: {}", destination.display()))?;
        if bound_identity != expected {
            bail!(
                "moved destination identity changed before validation: {}",
                destination.display()
            );
        }
        let live_identity = self.entry_identity(destination)?.ok_or_else(|| {
            anyhow!(
                "moved destination is not reachable: {}",
                destination.display()
            )
        })?;
        if live_identity != expected {
            bail!(
                "moved destination path resolves to a different object: {}",
                destination.display()
            );
        }
        Ok(())
    }

    fn validate_bound_renamed_destination(
        &self,
        destination: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.validate_bound_parent(&destination.parent)?;
        let current =
            identity_at(destination.parent.fd.as_ref(), &destination.leaf)?.ok_or_else(|| {
                anyhow!(
                    "bound moved destination disappeared: {}",
                    destination.path.display()
                )
            })?;
        if current != expected {
            bail!(
                "bound moved destination identity changed: {}",
                destination.path.display()
            );
        }
        Ok(())
    }

    fn remove_bound_entry_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        flags: AtFlags,
    ) -> Result<()> {
        self.remove_bound_entry_if_identity_with_hooks(
            entry,
            expected,
            flags,
            RemoveQuarantinePolicy::retain(false),
            || {},
            || Ok(()),
        )
    }

    #[cfg(test)]
    fn remove_bound_entry_if_identity_with_hook<F>(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        flags: AtFlags,
        before_quarantine_move: F,
    ) -> Result<()>
    where
        F: FnOnce(),
    {
        self.remove_bound_entry_if_identity_with_hooks(
            entry,
            expected,
            flags,
            RemoveQuarantinePolicy::retain(false),
            before_quarantine_move,
            || Ok(()),
        )
    }

    fn remove_bound_entry_if_identity_with_hooks<F, G>(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        flags: AtFlags,
        policy: RemoveQuarantinePolicy,
        before_quarantine_move: F,
        after_quarantine_move: G,
    ) -> Result<()>
    where
        F: FnOnce(),
        G: FnOnce() -> Result<()>,
    {
        // Protected property: unlink only the object identity captured by the caller. Moving the
        // live name into a private directory atomically lets us validate that object before any
        // unlink; a racing replacement is restored or retained, never deleted.
        self.validate_bound_parent(&entry.parent)?;
        let current = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?
            .ok_or_else(|| anyhow!("owned bound path is missing: {}", entry.path.display()))?;
        if current != expected {
            bail!(
                "owned bound path identity changed: {}",
                entry.path.display()
            );
        }
        let removes_directory = flags.contains(AtFlags::REMOVEDIR);
        if removes_directory != expected.is_dir() {
            bail!(
                "owned bound path removal type does not match its identity: {}",
                entry.path.display()
            );
        }
        if policy.recursive && !removes_directory {
            bail!(
                "recursive owned-path removal requires a directory: {}",
                entry.path.display()
            );
        }
        if !expected.is_file() && !expected.is_dir() {
            bail!(
                "owned bound path removal supports only regular files and directories: {}",
                entry.path.display()
            );
        }

        let quarantine =
            create_private_remove_quarantine(&entry.parent, &entry.leaf, expected, policy)?;
        let quarantine_path = entry
            .path
            .parent()
            .context("owned bound path has no parent")?
            .join(&quarantine.name);
        let quarantined_leaf = OsStr::new("entry");
        before_quarantine_move();
        if let Err(err) = renameat_noreplace(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            &quarantine.directory,
            quarantined_leaf,
        ) {
            let cleanup = remove_private_remove_quarantine(&entry.parent, &quarantine);
            return Err(with_cleanup_error(
                err.context(format!(
                    "failed to quarantine owned bound path {}",
                    entry.path.display()
                )),
                cleanup,
                "remove quarantine cleanup",
            ));
        }

        sync_directory(entry.parent.fd.as_ref())?;
        sync_directory(&quarantine.directory)?;
        let moved = identity_at(&quarantine.directory, quarantined_leaf)?.ok_or_else(|| {
            anyhow!(
                "quarantined bound path disappeared; retained quarantine {}",
                quarantine_path.display()
            )
        })?;
        if moved != expected {
            let rollback = renameat_noreplace(
                &quarantine.directory,
                quarantined_leaf,
                entry.parent.fd.as_ref(),
                &entry.leaf,
            )
            .and_then(|()| sync_directory(&quarantine.directory))
            .and_then(|()| sync_directory(entry.parent.fd.as_ref()));
            if let Err(rollback) = rollback {
                bail!(
                    "owned bound path changed before quarantine; retained replacement in {}: {rollback:#}",
                    quarantine_path.display()
                );
            }
            remove_private_remove_quarantine(&entry.parent, &quarantine)?;
            bail!(
                "owned bound path identity changed before removal: {}",
                entry.path.display()
            );
        }

        if let Err(validation_error) = after_quarantine_move() {
            let validation_error = validation_error.context(format!(
                "bound path removal validation rejected {}",
                entry.path.display()
            ));
            match policy.post_failure {
                PostQuarantineFailure::Retain => {
                    return Err(validation_error.context(format!(
                        "interrupted after quarantining owned bound path; retained quarantine {}",
                        quarantine_path.display()
                    )));
                }
                PostQuarantineFailure::Restore => {
                    let restore = renameat_noreplace(
                        &quarantine.directory,
                        quarantined_leaf,
                        entry.parent.fd.as_ref(),
                        &entry.leaf,
                    )
                    .and_then(|()| sync_directory(&quarantine.directory))
                    .and_then(|()| sync_directory(entry.parent.fd.as_ref()))
                    .and_then(|()| remove_private_remove_quarantine(&entry.parent, &quarantine))
                    .and_then(|()| self.validate_bound_parent(&entry.parent));
                    return Err(with_cleanup_error(
                        validation_error,
                        restore,
                        "validated bound-path removal rollback",
                    ));
                }
            }
        }

        remove_quarantined_entry(
            &quarantine.directory,
            quarantined_leaf,
            expected,
            flags,
            policy.recursive,
            &quarantine_path,
        )?;
        sync_directory(&quarantine.directory)?;
        remove_private_remove_quarantine(&entry.parent, &quarantine)?;
        self.validate_bound_parent(&entry.parent)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn remove_bound_file_if_identity_with_hook<F>(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        before_quarantine_move: F,
    ) -> Result<()>
    where
        F: FnOnce(),
    {
        self.remove_bound_entry_if_identity_with_hook(
            entry,
            expected,
            AtFlags::empty(),
            before_quarantine_move,
        )
    }

    #[cfg(test)]
    pub(crate) fn remove_bound_file_if_identity_with_post_quarantine_hook<F>(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
        after_quarantine_move: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        self.remove_bound_entry_if_identity_with_hooks(
            entry,
            expected,
            AtFlags::empty(),
            RemoveQuarantinePolicy::retain(false),
            || {},
            after_quarantine_move,
        )
    }

    fn validate_root(&self) -> Result<()> {
        let current = open_directory(&self.canonical_root).with_context(|| {
            format!(
                "failed to re-open output root {}",
                self.canonical_root.display()
            )
        })?;
        if identity_for_fd(&current)? != self.root_identity {
            bail!(
                "configured output root identity changed: {}",
                self.canonical_root.display()
            );
        }
        let logical =
            open_directory_following_final_symlink(&self.logical_root).with_context(|| {
                format!(
                    "failed to resolve configured output root {}",
                    self.logical_root.display()
                )
            })?;
        if identity_for_fd(&logical)? != self.root_identity {
            bail!(
                "configured output root now resolves to a different directory: {}",
                self.logical_root.display()
            );
        }
        Ok(())
    }

    fn validate_bound_parent(&self, parent: &BoundParent) -> Result<()> {
        // Bound recovery protects the captured directory object, not its current reachability
        // through the configured root path. Revalidating that path here would strand backups
        // after the root or an ancestor is renamed or retargeted.
        if identity_for_fd(parent.fd.as_ref())? != parent.identity {
            bail!("bound output parent directory identity changed");
        }
        Ok(())
    }

    fn validate_parent(&self, parent: &BoundParent) -> Result<()> {
        self.validate_root()?;
        let current = self.open_relative_directory_unvalidated(&parent.relative_path, false)?;
        if identity_for_fd(&current)? != parent.identity {
            bail!(
                "output parent directory identity changed: {}",
                self.logical_root.join(&parent.relative_path).display()
            );
        }
        Ok(())
    }

    fn open_relative_directory(&self, path: &Path, create: bool) -> Result<BoundParent> {
        self.validate_root()?;
        let fd = self.open_relative_directory_unvalidated(path, create)?;
        let identity = identity_for_fd(&fd)?;
        if !identity.is_dir() {
            bail!(
                "output path component is not a directory: {}",
                self.logical_root.join(path).display()
            );
        }
        let parent = BoundParent {
            fd: Arc::new(fd),
            relative_path: path.to_path_buf(),
            identity,
        };
        self.validate_parent(&parent)?;
        Ok(parent)
    }

    fn open_relative_directory_unvalidated(&self, path: &Path, create: bool) -> Result<OwnedFd> {
        self.open_relative_directory_unvalidated_with_sync(path, create, &mut sync_directory)
    }

    fn open_relative_directory_unvalidated_with_sync<F>(
        &self,
        path: &Path,
        create: bool,
        sync_created_parent: &mut F,
    ) -> Result<OwnedFd>
    where
        F: FnMut(&OwnedFd) -> Result<()>,
    {
        let mut current = rustix::io::dup(self.root_fd.as_ref())
            .map_err(errno_to_io)
            .context("failed to duplicate output root descriptor")?;
        for component in path.components() {
            let Component::Normal(name) = component else {
                bail!("invalid output-relative directory path: {}", path.display());
            };
            match openat_directory(&current, name) {
                Ok(next) => current = next,
                Err(err) if create && err.kind() == std::io::ErrorKind::NotFound => {
                    match rustix::fs::mkdirat(&current, name, Mode::from_raw_mode(0o755)) {
                        Ok(()) => {}
                        Err(errno) if errno == rustix::io::Errno::EXIST => {}
                        Err(errno) => {
                            return Err(errno_to_io(errno)).with_context(|| {
                                format!(
                                    "failed to create output directory {}",
                                    self.logical_root.join(path).display()
                                )
                            });
                        }
                    }
                    sync_created_parent(&current).with_context(|| {
                        format!(
                            "failed to persist newly created output directory {}",
                            self.logical_root.join(path).display()
                        )
                    })?;
                    current = openat_directory(&current, name).with_context(|| {
                        format!(
                            "failed to open newly created output directory {}",
                            self.logical_root.join(path).display()
                        )
                    })?;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "failed to open output directory without following symlinks: {}",
                            self.logical_root.join(path).display()
                        )
                    });
                }
            }
        }
        Ok(current)
    }

    fn split_parent(&self, path: &Path) -> Result<(PathBuf, OsString)> {
        let relative = path.strip_prefix(&self.logical_root).with_context(|| {
            format!(
                "path is outside the configured output root {}: {}",
                self.logical_root.display(),
                path.display()
            )
        })?;
        let mut components = relative.components().collect::<Vec<_>>();
        let leaf = match components.pop() {
            Some(Component::Normal(leaf)) => leaf.to_os_string(),
            _ => bail!("path does not name an output entry: {}", path.display()),
        };
        let mut parent = PathBuf::new();
        for component in components {
            let Component::Normal(name) = component else {
                bail!(
                    "path contains an invalid output component: {}",
                    path.display()
                );
            };
            parent.push(name);
        }
        Ok((parent, leaf))
    }
}

fn open_directory(path: &Path) -> Result<OwnedFd, std::io::Error> {
    rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(errno_to_io)
}

fn open_directory_following_final_symlink(path: &Path) -> Result<OwnedFd, std::io::Error> {
    rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_to_io)
}

fn openat_directory(parent: &OwnedFd, name: &OsStr) -> Result<OwnedFd, std::io::Error> {
    rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(errno_to_io)
}

fn openat_directory_cstr(parent: &OwnedFd, name: &CStr) -> Result<OwnedFd, std::io::Error> {
    rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(errno_to_io)
}

fn reconcile_remove_quarantines_in_directory<F>(
    directory: &OwnedFd,
    expected_directory: EntryIdentity,
    display_path: &Path,
    state: &mut RemoveQuarantineScanState,
    should_restore: &mut F,
    scan_depth: RemoveQuarantineScanDepth,
) -> Result<()>
where
    F: FnMut(EntryIdentity) -> Result<bool>,
{
    if identity_for_fd(directory)? != expected_directory {
        bail!(
            "remove quarantine scan directory identity changed: {}",
            display_path.display()
        );
    }
    let mut entries = rustix::fs::Dir::read_from(directory)
        .map_err(errno_to_io)
        .with_context(|| {
            format!(
                "failed to scan for interrupted removals in {}",
                display_path.display()
            )
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name().to_owned())
                .map_err(errno_to_io)
        })
        .collect::<std::result::Result<Vec<CString>, std::io::Error>>()
        .with_context(|| {
            format!(
                "failed to read interrupted-removal entry in {}",
                display_path.display()
            )
        })?;
    entries.sort_by_key(|name| !is_remove_quarantine_tombstone_name(name));

    for name in entries {
        if matches!(name.as_bytes(), b"." | b"..") {
            continue;
        }
        let is_managed_name = name
            .as_bytes()
            .starts_with(REMOVE_QUARANTINE_PREFIX.as_bytes());
        if scan_depth == RemoveQuarantineScanDepth::CurrentDirectory && !is_managed_name {
            continue;
        }
        let path = display_path.join(name.to_string_lossy().as_ref());
        let Some(identity) = identity_at_cstr(directory, &name)? else {
            if is_managed_name {
                continue;
            }
            state.unresolved = true;
            state.messages.push(format!(
                "Skipped disappearing entry during interrupted-removal scan: {}",
                path.display()
            ));
            continue;
        };
        if is_remove_quarantine_tombstone_name(&name) {
            match reconcile_private_remove_tombstone(
                directory,
                expected_directory,
                &name,
                &path,
                identity,
            ) {
                Ok(()) => state.messages.push(format!(
                    "Recovered interrupted bound-path cleanup: {}",
                    path.display()
                )),
                Err(err) => {
                    state.unresolved = true;
                    state.messages.push(format!(
                        "Retained unresolved interrupted-removal tombstone {}: {err:#}",
                        path.display()
                    ));
                }
            }
            continue;
        }
        if name
            .as_bytes()
            .starts_with(REMOVE_QUARANTINE_PREFIX.as_bytes())
        {
            match reconcile_private_remove_quarantine(
                directory,
                expected_directory,
                &name,
                &path,
                identity,
                should_restore,
            ) {
                Ok(RemoveQuarantineRecoveryAction::Deleted) => state.messages.push(format!(
                    "Recovered interrupted bound-path removal: {}",
                    path.display()
                )),
                Ok(RemoveQuarantineRecoveryAction::Restored) => {
                    state.restored = true;
                    state.messages.push(format!(
                        "Restored interrupted validated bound-path removal: {}",
                        path.display()
                    ));
                }
                Err(err) => {
                    state.unresolved = true;
                    state.messages.push(format!(
                        "Retained unresolved interrupted-removal quarantine {}: {err:#}",
                        path.display()
                    ));
                }
            }
            continue;
        }
        if scan_depth == RemoveQuarantineScanDepth::CurrentDirectory || !identity.is_dir() {
            continue;
        }
        let child = match openat_directory_cstr(directory, &name) {
            Ok(child) => child,
            Err(err) => {
                state.unresolved = true;
                state.messages.push(format!(
                    "Skipped unreadable directory during interrupted-removal scan {}: {err}",
                    path.display()
                ));
                continue;
            }
        };
        if identity_for_fd(&child)? != identity {
            state.unresolved = true;
            state.messages.push(format!(
                "Skipped replaced directory during interrupted-removal scan: {}",
                path.display()
            ));
            continue;
        }
        if let Err(err) = reconcile_remove_quarantines_in_directory(
            &child,
            identity,
            &path,
            state,
            should_restore,
            RemoveQuarantineScanDepth::Recursive,
        ) {
            state.unresolved = true;
            state.messages.push(format!(
                "Skipped directory during interrupted-removal scan {}: {err:#}",
                path.display()
            ));
        }
    }
    if identity_for_fd(directory)? != expected_directory {
        bail!(
            "remove quarantine scan directory identity changed: {}",
            display_path.display()
        );
    }
    Ok(())
}

fn reconcile_private_remove_quarantine<F>(
    parent: &OwnedFd,
    expected_parent: EntryIdentity,
    name: &CStr,
    display_path: &Path,
    expected_quarantine: EntryIdentity,
    should_restore: &mut F,
) -> Result<RemoveQuarantineRecoveryAction>
where
    F: FnMut(EntryIdentity) -> Result<bool>,
{
    if !expected_quarantine.is_dir() {
        bail!("interrupted-removal quarantine is not a directory");
    }
    if identity_for_fd(parent)? != expected_parent {
        bail!("interrupted-removal quarantine parent identity changed");
    }
    let directory = openat_directory_cstr(parent, name)
        .with_context(|| format!("failed to open quarantine {}", display_path.display()))?;
    if identity_for_fd(&directory)? != expected_quarantine
        || identity_at_cstr(parent, name)? != Some(expected_quarantine)
    {
        bail!("interrupted-removal quarantine identity changed");
    }
    let directory_stat = rustix::fs::fstat(&directory)
        .map_err(errno_to_io)
        .context("failed to inspect interrupted-removal quarantine")?;
    if directory_stat.st_mode & 0o777 != 0o700
        || directory_stat.st_uid != unsafe { libc::geteuid() }
    {
        bail!("interrupted-removal quarantine permissions are not private");
    }

    let entries = rustix::fs::Dir::read_from(&directory)
        .map_err(errno_to_io)
        .context("failed to list interrupted-removal quarantine")?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name().to_owned())
                .map_err(errno_to_io)
        })
        .collect::<std::result::Result<Vec<CString>, std::io::Error>>()
        .context("failed to read interrupted-removal quarantine entry")?;
    for entry in &entries {
        if matches!(entry.as_bytes(), b"." | b".." | b"entry" | b"manifest.json") {
            continue;
        }
        bail!(
            "interrupted-removal quarantine contains an unexpected entry: {}",
            entry.to_string_lossy()
        );
    }

    let (manifest, manifest_identity) = read_remove_quarantine_manifest_at(
        &directory,
        OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME),
        "interrupted-removal manifest",
    )?;
    validate_remove_quarantine_manifest(
        &manifest,
        name,
        expected_parent,
        expected_quarantine,
        false,
    )?;
    let expected_entry = EntryIdentity {
        device: manifest.entry_device,
        inode: manifest.entry_inode,
        file_type: if manifest.entry_is_directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
    };
    let original_name = manifest
        .original_name_hex
        .as_deref()
        .map(decode_remove_quarantine_original_name)
        .transpose()?;
    let mut action = RemoveQuarantineRecoveryAction::Deleted;
    if let Some(current) = identity_at(&directory, OsStr::new("entry"))? {
        if current != expected_entry {
            bail!("interrupted-removal entry identity does not match its manifest");
        }
        if manifest.restore_requires_revalidation
            && should_restore(expected_entry)
                .context("failed to revalidate interrupted bound-path removal")?
        {
            restore_quarantined_entry(
                parent,
                expected_parent,
                &directory,
                expected_quarantine,
                original_name
                    .as_deref()
                    .context("validated removal has no original path")?,
                expected_entry,
            )?;
            action = RemoveQuarantineRecoveryAction::Restored;
        } else {
            let flags = if expected_entry.is_dir() {
                AtFlags::REMOVEDIR
            } else {
                AtFlags::empty()
            };
            remove_quarantined_entry(
                &directory,
                OsStr::new("entry"),
                expected_entry,
                flags,
                manifest.recursive,
                display_path,
            )?;
            sync_directory(&directory)?;
        }
    } else if manifest.restore_requires_revalidation {
        let original_name = original_name
            .as_deref()
            .context("validated removal has no original path")?;
        match identity_at(parent, original_name)? {
            Some(current) if current == expected_entry => {
                action = RemoveQuarantineRecoveryAction::Restored;
            }
            Some(_) => {
                bail!("interrupted validated-removal destination contains a replacement");
            }
            None => {}
        }
    }
    finish_remove_quarantine_cleanup(
        parent,
        expected_parent,
        name,
        &directory,
        expected_quarantine,
        &manifest,
        Some(manifest_identity),
    )?;
    Ok(action)
}

fn restore_quarantined_entry(
    parent: &OwnedFd,
    expected_parent: EntryIdentity,
    quarantine: &OwnedFd,
    expected_quarantine: EntryIdentity,
    original_name: &OsStr,
    expected_entry: EntryIdentity,
) -> Result<()> {
    // Protected property: a crash-interrupted validated removal may restore only the exact selected
    // file into its persisted original leaf. Parent, quarantine, and entry identities prevent an
    // occupied replacement or another quarantined object from receiving that authorization.
    if identity_for_fd(parent)? != expected_parent
        || identity_for_fd(quarantine)? != expected_quarantine
        || identity_at(quarantine, OsStr::new("entry"))? != Some(expected_entry)
    {
        bail!("interrupted validated-removal identities changed before restoration");
    }
    match identity_at(parent, original_name)? {
        None => {
            renameat_noreplace(quarantine, OsStr::new("entry"), parent, original_name)
                .context("failed to restore interrupted validated removal")?;
            sync_directory(quarantine)?;
            sync_directory(parent)?;
        }
        Some(current) if current == expected_entry => {
            remove_quarantined_entry(
                quarantine,
                OsStr::new("entry"),
                expected_entry,
                AtFlags::empty(),
                false,
                Path::new(original_name),
            )?;
            sync_directory(quarantine)?;
        }
        Some(_) => {
            bail!("interrupted validated-removal destination is occupied by a replacement");
        }
    }
    if identity_at(parent, original_name)? != Some(expected_entry)
        || identity_at(quarantine, OsStr::new("entry"))?.is_some()
    {
        bail!("interrupted validated-removal restoration identity changed");
    }
    Ok(())
}

fn reconcile_private_remove_tombstone(
    parent: &OwnedFd,
    expected_parent: EntryIdentity,
    tombstone_name: &CStr,
    display_path: &Path,
    expected_tombstone: EntryIdentity,
) -> Result<()> {
    if !expected_tombstone.is_file() || identity_for_fd(parent)? != expected_parent {
        bail!("interrupted-removal tombstone parent or type changed");
    }
    let tombstone_os_name = OsStr::new(
        tombstone_name
            .to_str()
            .context("interrupted-removal tombstone name is not valid UTF-8")?,
    );
    let (manifest, tombstone_identity) = read_remove_quarantine_manifest_at(
        parent,
        tombstone_os_name,
        "interrupted-removal tombstone",
    )?;
    if tombstone_identity != expected_tombstone {
        bail!("interrupted-removal tombstone identity changed");
    }
    let quarantine_name = CString::new(manifest.quarantine_name.as_bytes())
        .context("interrupted-removal tombstone contains an invalid quarantine name")?;
    if tombstone_name.to_bytes() != remove_quarantine_tombstone_name(&quarantine_name).as_bytes() {
        bail!("interrupted-removal tombstone name does not match its manifest");
    }
    let expected_quarantine = manifest_quarantine_identity(&manifest)?;
    validate_remove_quarantine_manifest(
        &manifest,
        &quarantine_name,
        expected_parent,
        expected_quarantine,
        true,
    )?;
    complete_remove_quarantine_from_tombstone(
        parent,
        expected_parent,
        &quarantine_name,
        expected_quarantine,
        &manifest,
        tombstone_name,
        tombstone_identity,
        display_path,
    )
}

fn is_remove_quarantine_tombstone_name(name: &CStr) -> bool {
    let bytes = name.to_bytes();
    bytes.starts_with(REMOVE_QUARANTINE_PREFIX.as_bytes())
        && bytes.ends_with(REMOVE_QUARANTINE_TOMBSTONE_SUFFIX.as_bytes())
}

fn remove_quarantine_tombstone_name(name: &CStr) -> CString {
    let mut bytes = name.to_bytes().to_vec();
    bytes.extend_from_slice(REMOVE_QUARANTINE_TOMBSTONE_SUFFIX.as_bytes());
    CString::new(bytes).expect("generated remove-quarantine tombstone name contains no NUL")
}

fn manifest_quarantine_identity(manifest: &RemoveQuarantineManifest) -> Result<EntryIdentity> {
    Ok(EntryIdentity {
        device: manifest
            .quarantine_device
            .context("interrupted-removal tombstone has no quarantine device")?,
        inode: manifest
            .quarantine_inode
            .context("interrupted-removal tombstone has no quarantine inode")?,
        file_type: FileType::Directory,
    })
}

fn validate_remove_quarantine_manifest(
    manifest: &RemoveQuarantineManifest,
    quarantine_name: &CStr,
    expected_parent: EntryIdentity,
    expected_quarantine: EntryIdentity,
    require_quarantine_identity: bool,
) -> Result<()> {
    if !matches!(
        manifest.version,
        REMOVE_QUARANTINE_LEGACY_MANIFEST_VERSION
            | REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION
            | REMOVE_QUARANTINE_MANIFEST_VERSION
    ) || (require_quarantine_identity
        && manifest.version < REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION)
        || manifest.quarantine_name != quarantine_name.to_string_lossy()
        || manifest.parent_device != expected_parent.device
        || manifest.parent_inode != expected_parent.inode
    {
        bail!("interrupted-removal manifest does not describe this quarantine");
    }
    if manifest.version >= REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION
        && (manifest.quarantine_device != Some(expected_quarantine.device)
            || manifest.quarantine_inode != Some(expected_quarantine.inode))
    {
        bail!("interrupted-removal manifest quarantine identity changed");
    }
    if manifest.recursive && !manifest.entry_is_directory {
        bail!("interrupted-removal manifest requests recursive non-directory removal");
    }
    if manifest.version == REMOVE_QUARANTINE_MANIFEST_VERSION {
        let original_name = manifest
            .original_name_hex
            .as_deref()
            .context("interrupted-removal manifest has no original name")?;
        decode_remove_quarantine_original_name(original_name)?;
        if manifest.restore_requires_revalidation
            && (manifest.entry_is_directory || manifest.recursive)
        {
            bail!("interrupted-removal restore policy is only valid for one regular file");
        }
    } else if manifest.original_name_hex.is_some() || manifest.restore_requires_revalidation {
        bail!("legacy interrupted-removal manifest contains unsupported recovery policy");
    }
    Ok(())
}

fn read_remove_quarantine_manifest_at(
    parent: &OwnedFd,
    name: &OsStr,
    label: &str,
) -> Result<(RemoveQuarantineManifest, EntryIdentity)> {
    let fd = rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(errno_to_io)
    .with_context(|| format!("failed to open {label}"))?;
    let identity = identity_for_fd(&fd)?;
    let stat = rustix::fs::fstat(&fd)
        .map_err(errno_to_io)
        .with_context(|| format!("failed to inspect {label}"))?;
    if !identity.is_file()
        || stat.st_mode & 0o777 != 0o600
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_nlink != 1
        || identity_at(parent, name)? != Some(identity)
    {
        bail!("{label} ownership, permissions, or identity changed");
    }
    let mut reader = File::from(fd).take((REMOVE_QUARANTINE_MANIFEST_LIMIT + 1) as u64);
    let mut contents = Vec::new();
    reader
        .read_to_end(&mut contents)
        .with_context(|| format!("failed to read {label}"))?;
    if contents.len() > REMOVE_QUARANTINE_MANIFEST_LIMIT {
        bail!("{label} exceeds its size limit");
    }
    let manifest =
        serde_json::from_slice(&contents).with_context(|| format!("failed to parse {label}"))?;
    Ok((manifest, identity))
}

fn remove_quarantined_entry(
    quarantine: &OwnedFd,
    entry_name: &OsStr,
    expected: EntryIdentity,
    flags: AtFlags,
    recursive: bool,
    display_path: &Path,
) -> Result<()> {
    if identity_at(quarantine, entry_name)? != Some(expected) {
        bail!("quarantined bound path identity changed");
    }
    if recursive {
        let directory = openat_directory(quarantine, entry_name).with_context(|| {
            format!(
                "failed to open recursively quarantined directory {}",
                display_path.display()
            )
        })?;
        if identity_for_fd(&directory)? != expected {
            bail!("recursively quarantined directory identity changed");
        }
        remove_directory_contents(&directory, display_path)?;
        if identity_at(quarantine, entry_name)? != Some(expected) {
            bail!("recursively quarantined directory identity changed before removal");
        }
    }
    rustix::fs::unlinkat(quarantine, entry_name, flags)
        .map_err(errno_to_io)
        .with_context(|| {
            format!(
                "failed to remove quarantined bound path; retained quarantine {}",
                display_path.display()
            )
        })
}

fn finish_remove_quarantine_cleanup(
    parent: &OwnedFd,
    expected_parent: EntryIdentity,
    quarantine_name: &CStr,
    directory: &OwnedFd,
    expected_quarantine: EntryIdentity,
    manifest: &RemoveQuarantineManifest,
    manifest_identity: Option<EntryIdentity>,
) -> Result<()> {
    if identity_for_fd(parent)? != expected_parent
        || identity_for_fd(directory)? != expected_quarantine
        || identity_at_cstr(parent, quarantine_name)? != Some(expected_quarantine)
    {
        bail!("remove quarantine changed before terminal cleanup");
    }
    if identity_at(directory, OsStr::new("entry"))?.is_some() {
        bail!("remove quarantine still contains its protected entry");
    }
    if let Some(expected_manifest) = manifest_identity
        && identity_at(directory, OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME))?
            != Some(expected_manifest)
    {
        bail!("remove quarantine manifest changed before terminal cleanup");
    }

    let tombstone_manifest =
        build_terminal_remove_quarantine_manifest(manifest, expected_quarantine);
    let tombstone_name = remove_quarantine_tombstone_name(quarantine_name);
    let tombstone_identity = create_or_validate_remove_quarantine_tombstone(
        parent,
        &tombstone_name,
        &tombstone_manifest,
    )?;
    complete_remove_quarantine_from_tombstone(
        parent,
        expected_parent,
        quarantine_name,
        expected_quarantine,
        &tombstone_manifest,
        &tombstone_name,
        tombstone_identity,
        Path::new(&tombstone_manifest.quarantine_name),
    )
}

fn build_terminal_remove_quarantine_manifest(
    manifest: &RemoveQuarantineManifest,
    expected_quarantine: EntryIdentity,
) -> RemoveQuarantineManifest {
    let mut terminal = manifest.clone();
    // A v1 quarantine needs the v2 identity fields to become a standalone tombstone. Neither v1
    // nor v2 persisted the original leaf required by v3, so upgrading either schema would create
    // an invalid terminal record after the quarantine directory disappears.
    terminal.version = if manifest.version == REMOVE_QUARANTINE_MANIFEST_VERSION {
        REMOVE_QUARANTINE_MANIFEST_VERSION
    } else {
        REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION
    };
    terminal.quarantine_device = Some(expected_quarantine.device);
    terminal.quarantine_inode = Some(expected_quarantine.inode);
    terminal
}

fn create_or_validate_remove_quarantine_tombstone(
    parent: &OwnedFd,
    name: &CStr,
    manifest: &RemoveQuarantineManifest,
) -> Result<EntryIdentity> {
    let contents =
        serde_json::to_vec(manifest).context("failed to encode interrupted-removal tombstone")?;
    if contents.len() > REMOVE_QUARANTINE_MANIFEST_LIMIT {
        bail!("interrupted-removal tombstone exceeds its size limit");
    }
    match rustix::fs::openat(
        parent,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    ) {
        Ok(fd) => {
            let identity = identity_for_fd(&fd)?;
            let stat = rustix::fs::fstat(&fd)
                .map_err(errno_to_io)
                .context("failed to inspect interrupted-removal tombstone")?;
            if !identity.is_file()
                || stat.st_mode & 0o777 != 0o600
                || stat.st_uid != unsafe { libc::geteuid() }
                || stat.st_nlink != 1
            {
                bail!("interrupted-removal tombstone ownership or permissions changed");
            }
            let mut file = File::from(fd);
            file.write_all(&contents)
                .and_then(|()| file.sync_all())
                .context("failed to persist interrupted-removal tombstone")?;
            if identity_at_cstr(parent, name)? != Some(identity) {
                bail!("interrupted-removal tombstone identity changed while creating it");
            }
            sync_directory(parent)?;
            Ok(identity)
        }
        Err(err) if err == rustix::io::Errno::EXIST => {
            let (existing, identity) = read_remove_quarantine_manifest_at(
                parent,
                OsStr::new(name.to_str().context("invalid tombstone name")?),
                "interrupted-removal tombstone",
            )?;
            if existing != *manifest {
                bail!("interrupted-removal tombstone contents changed");
            }
            Ok(identity)
        }
        Err(err) => Err(errno_to_io(err)).context("failed to create interrupted-removal tombstone"),
    }
}

#[allow(clippy::too_many_arguments)]
fn complete_remove_quarantine_from_tombstone(
    parent: &OwnedFd,
    expected_parent: EntryIdentity,
    quarantine_name: &CStr,
    expected_quarantine: EntryIdentity,
    manifest: &RemoveQuarantineManifest,
    tombstone_name: &CStr,
    tombstone_identity: EntryIdentity,
    display_path: &Path,
) -> Result<()> {
    if identity_for_fd(parent)? != expected_parent {
        bail!("interrupted-removal tombstone parent identity changed");
    }
    if let Some(current) = identity_at_cstr(parent, quarantine_name)? {
        if current != expected_quarantine || !current.is_dir() {
            bail!("interrupted-removal quarantine changed during terminal cleanup");
        }
        let directory = openat_directory_cstr(parent, quarantine_name)
            .context("failed to open terminal remove quarantine")?;
        let stat = rustix::fs::fstat(&directory)
            .map_err(errno_to_io)
            .context("failed to inspect terminal remove quarantine")?;
        if identity_for_fd(&directory)? != expected_quarantine
            || stat.st_mode & 0o777 != 0o700
            || stat.st_uid != unsafe { libc::geteuid() }
        {
            bail!("terminal remove quarantine ownership or identity changed");
        }
        if identity_at(&directory, OsStr::new("entry"))?.is_some() {
            bail!("terminal remove quarantine unexpectedly contains its protected entry");
        }
        if let Some(inner_identity) =
            identity_at(&directory, OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME))?
        {
            let (inner_manifest, opened_identity) = read_remove_quarantine_manifest_at(
                &directory,
                OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME),
                "interrupted-removal manifest",
            )?;
            if opened_identity != inner_identity
                || inner_manifest.quarantine_name != manifest.quarantine_name
                || inner_manifest.parent_device != manifest.parent_device
                || inner_manifest.parent_inode != manifest.parent_inode
                || inner_manifest.entry_device != manifest.entry_device
                || inner_manifest.entry_inode != manifest.entry_inode
                || inner_manifest.entry_is_directory != manifest.entry_is_directory
                || inner_manifest.recursive != manifest.recursive
                || inner_manifest.original_name_hex != manifest.original_name_hex
                || inner_manifest.restore_requires_revalidation
                    != manifest.restore_requires_revalidation
            {
                bail!("interrupted-removal manifest changed before terminal cleanup");
            }
            rustix::fs::unlinkat(
                &directory,
                REMOVE_QUARANTINE_MANIFEST_NAME,
                AtFlags::empty(),
            )
            .map_err(errno_to_io)
            .context("failed to remove interrupted-removal manifest")?;
            sync_directory(&directory)?;
        }
        let entries = rustix::fs::Dir::read_from(&directory)
            .map_err(errno_to_io)
            .context("failed to inspect terminal remove quarantine")?
            .map(|entry| {
                entry
                    .map(|entry| entry.file_name().to_owned())
                    .map_err(errno_to_io)
            })
            .collect::<std::result::Result<Vec<_>, std::io::Error>>()
            .context("failed to read terminal remove quarantine entry")?;
        let unexpected = entries
            .into_iter()
            .find(|name| !matches!(name.as_bytes(), b"." | b".."));
        if let Some(name) = unexpected {
            bail!(
                "terminal remove quarantine contains an unexpected entry: {}",
                name.to_string_lossy()
            );
        }
        if identity_at_cstr(parent, quarantine_name)? != Some(expected_quarantine) {
            bail!("terminal remove quarantine changed before directory removal");
        }
        rustix::fs::unlinkat(parent, quarantine_name, AtFlags::REMOVEDIR)
            .map_err(errno_to_io)
            .with_context(|| {
                format!(
                    "failed to remove terminal quarantine {}",
                    display_path.display()
                )
            })?;
        sync_directory(parent)?;
    }
    if identity_at_cstr(parent, tombstone_name)? != Some(tombstone_identity) {
        bail!("interrupted-removal tombstone changed before cleanup");
    }
    rustix::fs::unlinkat(parent, tombstone_name, AtFlags::empty())
        .map_err(errno_to_io)
        .context("failed to remove interrupted-removal tombstone")?;
    sync_directory(parent)
}

fn encode_remove_quarantine_original_name(name: &OsStr) -> Result<String> {
    validate_remove_quarantine_original_name(name)?;
    let bytes = remove_quarantine_name_bytes(name)?;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

fn decode_remove_quarantine_original_name(encoded: &str) -> Result<OsString> {
    if encoded.is_empty() || !encoded.len().is_multiple_of(2) {
        bail!("interrupted-removal original name has invalid hex framing");
    }
    let encoded = encoded.as_bytes();
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.chunks_exact(2) {
        let high = decode_lower_hex_digit(pair[0])?;
        let low = decode_lower_hex_digit(pair[1])?;
        bytes.push((high << 4) | low);
    }
    let name = remove_quarantine_os_string_from_bytes(bytes)?;
    validate_remove_quarantine_original_name(&name)?;
    Ok(name)
}

fn decode_lower_hex_digit(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => bail!("interrupted-removal original name contains non-canonical hex"),
    }
}

fn validate_remove_quarantine_original_name(name: &OsStr) -> Result<()> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        bail!("interrupted-removal original name is not one path component");
    }
    if remove_quarantine_name_bytes(name)?.contains(&0) {
        bail!("interrupted-removal original name contains NUL");
    }
    Ok(())
}

#[cfg(unix)]
fn remove_quarantine_name_bytes(name: &OsStr) -> Result<&[u8]> {
    Ok(name.as_bytes())
}

#[cfg(not(unix))]
fn remove_quarantine_name_bytes(name: &OsStr) -> Result<&[u8]> {
    Ok(name
        .to_str()
        .context("interrupted-removal original name is not UTF-8")?
        .as_bytes())
}

#[cfg(unix)]
fn remove_quarantine_os_string_from_bytes(bytes: Vec<u8>) -> Result<OsString> {
    Ok(OsString::from_vec(bytes))
}

#[cfg(not(unix))]
fn remove_quarantine_os_string_from_bytes(bytes: Vec<u8>) -> Result<OsString> {
    Ok(OsString::from(String::from_utf8(bytes).context(
        "interrupted-removal original name is not UTF-8",
    )?))
}

fn create_private_remove_quarantine(
    parent: &BoundParent,
    original_name: &OsStr,
    expected: EntryIdentity,
    policy: RemoveQuarantinePolicy,
) -> Result<PrivateRemoveQuarantine> {
    let original_name_hex = encode_remove_quarantine_original_name(original_name)?;
    for _ in 0..128 {
        let counter = REMOVE_QUARANTINE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = OsString::from(format!(
            "{REMOVE_QUARANTINE_PREFIX}-{}-{counter:016x}",
            std::process::id()
        ));
        match rustix::fs::mkdirat(parent.fd.as_ref(), &name, Mode::from_raw_mode(0o700)) {
            Ok(()) => {}
            Err(err) if err == rustix::io::Errno::EXIST => continue,
            Err(err) => {
                return Err(errno_to_io(err)).context("failed to create remove quarantine");
            }
        }
        let directory = match openat_directory(parent.fd.as_ref(), &name) {
            Ok(directory) => directory,
            Err(err) => {
                let cleanup = rustix::fs::unlinkat(parent.fd.as_ref(), &name, AtFlags::REMOVEDIR)
                    .map_err(errno_to_io);
                return Err(with_cleanup_error(
                    anyhow!(err).context("failed to open remove quarantine"),
                    cleanup,
                    "remove quarantine cleanup",
                ));
            }
        };
        let identity = identity_for_fd(&directory)?;
        if !identity.is_dir()
            || identity_at(parent.fd.as_ref(), &name)? != Some(identity)
            || identity_for_fd(parent.fd.as_ref())? != parent.identity
        {
            bail!("remove quarantine identity changed while creating it");
        }

        let manifest = RemoveQuarantineManifest {
            version: REMOVE_QUARANTINE_MANIFEST_VERSION,
            quarantine_name: name.to_string_lossy().into_owned(),
            quarantine_device: Some(identity.device),
            quarantine_inode: Some(identity.inode),
            parent_device: parent.identity.device,
            parent_inode: parent.identity.inode,
            entry_device: expected.device,
            entry_inode: expected.inode,
            entry_is_directory: expected.is_dir(),
            recursive: policy.recursive,
            original_name_hex: Some(original_name_hex.clone()),
            restore_requires_revalidation: policy.post_failure == PostQuarantineFailure::Restore,
        };
        let manifest_identity = match write_remove_quarantine_manifest(&directory, &manifest) {
            Ok(identity) => identity,
            Err(err) => {
                let cleanup =
                    cleanup_private_remove_quarantine_creation(parent, &name, &directory, identity);
                return Err(with_cleanup_error(
                    err,
                    cleanup,
                    "remove quarantine creation cleanup",
                ));
            }
        };
        sync_directory(&directory)?;
        sync_directory(parent.fd.as_ref())?;
        return Ok(PrivateRemoveQuarantine {
            name,
            directory,
            identity,
            manifest,
            manifest_identity,
        });
    }
    bail!("failed to allocate a unique remove quarantine")
}

fn write_remove_quarantine_manifest(
    directory: &OwnedFd,
    manifest: &RemoveQuarantineManifest,
) -> Result<EntryIdentity> {
    let contents =
        serde_json::to_vec(manifest).context("failed to encode remove quarantine manifest")?;
    if contents.len() > REMOVE_QUARANTINE_MANIFEST_LIMIT {
        bail!("remove quarantine manifest exceeds its size limit");
    }
    let fd = rustix::fs::openat(
        directory,
        REMOVE_QUARANTINE_MANIFEST_NAME,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    )
    .map_err(errno_to_io)
    .context("failed to create remove quarantine manifest")?;
    let identity = identity_for_fd(&fd)?;
    if !identity.is_file() {
        bail!("remove quarantine manifest is not a regular file");
    }
    let mut file = File::from(fd);
    file.write_all(&contents)
        .context("failed to write remove quarantine manifest")?;
    file.sync_all()
        .context("failed to persist remove quarantine manifest")?;
    if identity_at(directory, OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME))? != Some(identity) {
        bail!("remove quarantine manifest identity changed while creating it");
    }
    Ok(identity)
}

fn cleanup_private_remove_quarantine_creation(
    parent: &BoundParent,
    name: &OsStr,
    directory: &OwnedFd,
    expected: EntryIdentity,
) -> Result<()> {
    if let Some(identity) = identity_at(directory, OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME))? {
        if !identity.is_file() {
            bail!("remove quarantine creation left a non-file manifest");
        }
        rustix::fs::unlinkat(directory, REMOVE_QUARANTINE_MANIFEST_NAME, AtFlags::empty())
            .map_err(errno_to_io)
            .context("failed to clean remove quarantine manifest")?;
        sync_directory(directory)?;
    }
    remove_empty_private_remove_quarantine(parent, name, expected)
}

fn remove_private_remove_quarantine(
    parent: &BoundParent,
    quarantine: &PrivateRemoveQuarantine,
) -> Result<()> {
    if identity_for_fd(parent.fd.as_ref())? != parent.identity {
        bail!("remove quarantine parent identity changed")
    }
    if identity_for_fd(&quarantine.directory)? != quarantine.identity
        || identity_at(parent.fd.as_ref(), &quarantine.name)? != Some(quarantine.identity)
    {
        bail!("remove quarantine path identity changed")
    }
    if identity_at(
        &quarantine.directory,
        OsStr::new(REMOVE_QUARANTINE_MANIFEST_NAME),
    )? != Some(quarantine.manifest_identity)
    {
        bail!("remove quarantine manifest identity changed")
    }
    let name = CString::new(quarantine.name.to_string_lossy().as_bytes())
        .context("remove quarantine name contains an invalid NUL")?;
    finish_remove_quarantine_cleanup(
        parent.fd.as_ref(),
        parent.identity,
        &name,
        &quarantine.directory,
        quarantine.identity,
        &quarantine.manifest,
        Some(quarantine.manifest_identity),
    )
}

fn remove_empty_private_remove_quarantine(
    parent: &BoundParent,
    name: &OsStr,
    expected: EntryIdentity,
) -> Result<()> {
    if identity_for_fd(parent.fd.as_ref())? != parent.identity {
        bail!("remove quarantine parent identity changed")
    }
    if identity_at(parent.fd.as_ref(), name)? != Some(expected) {
        bail!("remove quarantine path identity changed")
    }
    rustix::fs::unlinkat(parent.fd.as_ref(), name, AtFlags::REMOVEDIR)
        .map_err(errno_to_io)
        .context("failed to remove private remove quarantine")?;
    sync_directory(parent.fd.as_ref())
}

fn rollback_bound_rename(
    source: &BoundEntry,
    destination: &BoundEntry,
    expected: EntryIdentity,
) -> Result<()> {
    let current =
        identity_at(destination.parent.fd.as_ref(), &destination.leaf)?.ok_or_else(|| {
            anyhow!(
                "bound rollback object is missing: {}",
                destination.path.display()
            )
        })?;
    if current != expected {
        bail!(
            "bound rollback object identity changed; retained current paths instead of moving a replacement: {}",
            destination.path.display()
        );
    }
    if identity_at(source.parent.fd.as_ref(), &source.leaf)?.is_some() {
        bail!(
            "bound rollback destination is occupied: {}",
            source.path.display()
        );
    }
    renameat_noreplace(
        destination.parent.fd.as_ref(),
        &destination.leaf,
        source.parent.fd.as_ref(),
        &source.leaf,
    )?;
    sync_directory(destination.parent.fd.as_ref())?;
    sync_directory(source.parent.fd.as_ref())?;
    Ok(())
}

fn sync_directory(directory: &OwnedFd) -> Result<()> {
    rustix::fs::fsync(directory)
        .map_err(errno_to_io)
        .context("failed to sync bound directory")
}

fn identity_for_fd<Fd: AsFd>(fd: Fd) -> Result<EntryIdentity> {
    let stat = rustix::fs::fstat(fd)
        .map_err(errno_to_io)
        .context("failed to inspect bound filesystem object")?;
    Ok(identity_from_stat(&stat))
}

pub(crate) fn identity_for_open_file(file: &std::fs::File) -> Result<EntryIdentity> {
    identity_for_fd(file)
}

fn identity_at(parent: &OwnedFd, name: &OsStr) -> Result<Option<EntryIdentity>> {
    match rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(identity_from_stat(&stat))),
        Err(err) if err == rustix::io::Errno::NOENT => Ok(None),
        Err(err) => Err(errno_to_io(err)).context("failed to inspect output entry"),
    }
}

fn identity_at_cstr(parent: &OwnedFd, name: &CStr) -> Result<Option<EntryIdentity>> {
    match rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(identity_from_stat(&stat))),
        Err(err) if err == rustix::io::Errno::NOENT => Ok(None),
        Err(err) => Err(errno_to_io(err)).context("failed to inspect output entry"),
    }
}

fn remove_directory_contents(directory: &OwnedFd, display_path: &Path) -> Result<()> {
    let entries = rustix::fs::Dir::read_from(directory)
        .map_err(errno_to_io)
        .with_context(|| format!("failed to read bound directory {}", display_path.display()))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name().to_owned())
                .map_err(errno_to_io)
        })
        .collect::<std::result::Result<Vec<CString>, std::io::Error>>()
        .with_context(|| format!("failed to read bound directory {}", display_path.display()))?;

    for name in entries {
        if matches!(name.as_bytes(), b"." | b"..") {
            continue;
        }
        let child_path = display_path.join(name.to_string_lossy().as_ref());
        let expected = identity_at_cstr(directory, &name)?
            .ok_or_else(|| anyhow!("bound cleanup entry is missing: {}", child_path.display()))?;
        if expected.is_dir() {
            let child = openat_directory_cstr(directory, &name).with_context(|| {
                format!(
                    "failed to open bound cleanup directory {}",
                    child_path.display()
                )
            })?;
            if identity_for_fd(&child)? != expected {
                bail!(
                    "bound cleanup entry identity changed: {}",
                    child_path.display()
                );
            }
            remove_directory_contents(&child, &child_path)?;
            let current = identity_at_cstr(directory, &name)?.ok_or_else(|| {
                anyhow!("bound cleanup entry is missing: {}", child_path.display())
            })?;
            if current != expected {
                bail!(
                    "bound cleanup entry identity changed: {}",
                    child_path.display()
                );
            }
            rustix::fs::unlinkat(directory, &name, AtFlags::REMOVEDIR)
                .map_err(errno_to_io)
                .with_context(|| {
                    format!(
                        "failed to remove bound cleanup directory {}",
                        child_path.display()
                    )
                })?;
        } else {
            let current = identity_at_cstr(directory, &name)?.ok_or_else(|| {
                anyhow!("bound cleanup entry is missing: {}", child_path.display())
            })?;
            if current != expected {
                bail!(
                    "bound cleanup entry identity changed: {}",
                    child_path.display()
                );
            }
            rustix::fs::unlinkat(directory, &name, AtFlags::empty())
                .map_err(errno_to_io)
                .with_context(|| {
                    format!(
                        "failed to remove bound cleanup entry {}",
                        child_path.display()
                    )
                })?;
        }
    }
    Ok(())
}

fn identity_from_stat(stat: &rustix::fs::Stat) -> EntryIdentity {
    EntryIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino,
        file_type: FileType::from_raw_mode(stat.st_mode),
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "linux"))]
fn renameat_noreplace(
    source_parent: &OwnedFd,
    source: &OsStr,
    destination_parent: &OwnedFd,
    destination: &OsStr,
) -> Result<()> {
    rustix::fs::renameat_with(
        source_parent,
        source,
        destination_parent,
        destination,
        RenameFlags::NOREPLACE,
    )
    .map_err(errno_to_io)
    .context("atomic no-replace rename failed")
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
fn renameat_noreplace(
    source_parent: &OwnedFd,
    source: &OsStr,
    destination_parent: &OwnedFd,
    destination: &OsStr,
) -> Result<()> {
    rustix::fs::linkat(
        source_parent,
        source,
        destination_parent,
        destination,
        AtFlags::empty(),
    )
    .map_err(errno_to_io)
    .context("atomic no-replace link failed")?;
    if let Err(err) = rustix::fs::unlinkat(source_parent, source, AtFlags::empty()) {
        let cleanup = rustix::fs::unlinkat(destination_parent, destination, AtFlags::empty())
            .map_err(errno_to_io);
        return Err(with_cleanup_error(
            anyhow!(errno_to_io(err)).context("failed to unlink move source"),
            cleanup,
            "no-replace link rollback",
        ));
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "linux"))]
fn renameat_exchange(
    first_parent: &OwnedFd,
    first: &OsStr,
    second_parent: &OwnedFd,
    second: &OsStr,
) -> Result<()> {
    rustix::fs::renameat_with(
        first_parent,
        first,
        second_parent,
        second,
        RenameFlags::EXCHANGE,
    )
    .map_err(errno_to_io)
    .context("atomic exchange rename failed")
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
fn renameat_exchange(
    _first_parent: &OwnedFd,
    _first: &OsStr,
    _second_parent: &OwnedFd,
    _second: &OsStr,
) -> Result<()> {
    bail!("atomic exchange rename is unavailable on this platform")
}

fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}

fn with_cleanup_error<E>(
    primary: anyhow::Error,
    cleanup: std::result::Result<(), E>,
    operation: &str,
) -> anyhow::Error
where
    E: std::fmt::Display,
{
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => anyhow!("{primary:#}; {operation} failed: {cleanup}"),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "telegram-video-downloader-safe-fs-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    #[test]
    fn atomic_move_never_replaces_an_existing_entry() {
        let root = temp_dir("noreplace");
        fs::create_dir_all(root.join("staging")).expect("staging should create");
        fs::write(root.join("staging/source"), b"new").expect("source should write");
        fs::write(root.join("target"), b"old").expect("target should write");
        let rooted = RootedFs::new(&root).expect("root should open");

        let error = rooted
            .rename_noreplace(&root.join("staging/source"), &root.join("target"), false)
            .expect_err("existing destination should reject the move");

        assert!(error.to_string().contains("destination already exists"));
        assert_eq!(fs::read(root.join("staging/source")).unwrap(), b"new");
        assert_eq!(fs::read(root.join("target")).unwrap(), b"old");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nested_destination_creation_syncs_each_new_parent_before_descent() {
        let root = temp_dir("nested-parent-sync");
        fs::create_dir_all(&root).expect("root should create");
        let rooted = RootedFs::new(&root).expect("root should open");
        let mut synced_parents = Vec::new();

        let opened = rooted
            .open_relative_directory_unvalidated_with_sync(
                Path::new("library/season-01"),
                true,
                &mut |parent| {
                    synced_parents.push(identity_for_fd(parent)?);
                    sync_directory(parent)
                },
            )
            .expect("nested destination should create durably");

        let library_identity = rooted
            .entry_identity(&root.join("library"))
            .expect("library identity should read")
            .expect("library should exist");
        assert_eq!(
            synced_parents,
            vec![rooted.root_identity(), library_identity]
        );
        assert_eq!(
            identity_for_fd(&opened).expect("opened directory identity should read"),
            rooted
                .entry_identity(&root.join("library/season-01"))
                .expect("season identity should read")
                .expect("season should exist")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn atomic_bound_file_replacement_never_exposes_partial_contents() {
        let root = temp_dir("atomic-bound-replacement");
        fs::create_dir_all(&root).expect("root should create");
        let manifest = root.join("manifest.json");
        let temp = root.join("manifest.next.json");
        fs::write(&manifest, b"old-complete-manifest").expect("manifest should write");
        let rooted = RootedFs::new(&root).expect("root should open");
        let entry = rooted
            .bind_entry(&manifest, false)
            .expect("manifest should bind");
        let old_identity = rooted
            .bound_entry_identity(&entry)
            .expect("manifest identity should read")
            .expect("manifest should exist");

        let (_, new_identity) = rooted
            .replace_bound_file_atomically_if_identity(
                &entry,
                old_identity,
                &temp,
                b"new-complete-manifest",
                0o600,
            )
            .expect("manifest replacement should succeed");

        assert_eq!(fs::read(&manifest).unwrap(), b"new-complete-manifest");
        assert!(!temp.exists());
        assert_ne!(old_identity, new_identity);
        assert_eq!(
            rooted.entry_identity(&manifest).unwrap(),
            Some(new_identity)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn atomic_bound_file_replacement_restores_a_raced_destination() {
        let root = temp_dir("atomic-bound-replacement-race");
        fs::create_dir_all(&root).expect("root should create");
        let manifest = root.join("manifest.json");
        let saved_manifest = root.join("manifest.saved.json");
        let temp = root.join("manifest.next.json");
        fs::write(&manifest, b"old-complete-manifest").expect("manifest should write");
        let rooted = RootedFs::new(&root).expect("root should open");
        let entry = rooted
            .bind_entry(&manifest, false)
            .expect("manifest should bind");
        let old_identity = rooted
            .bound_entry_identity(&entry)
            .expect("manifest identity should read")
            .expect("manifest should exist");

        let error = rooted
            .replace_bound_file_atomically_if_identity_with_hook(
                &entry,
                old_identity,
                &temp,
                b"new-complete-manifest",
                0o600,
                &mut |checkpoint| {
                    if checkpoint == AtomicBoundFileReplaceCheckpoint::BeforeExchange {
                        fs::rename(&manifest, &saved_manifest)
                            .context("failed to preserve raced manifest")?;
                        fs::write(&manifest, b"racer-owned-manifest")
                            .context("failed to install raced manifest")?;
                    }
                    Ok(())
                },
            )
            .expect_err("a raced destination must reject replacement");

        assert!(
            format!("{error:#}")
                .contains("bound file identities changed during atomic replacement")
        );
        assert_eq!(fs::read(&manifest).unwrap(), b"racer-owned-manifest");
        assert_eq!(fs::read(&saved_manifest).unwrap(), b"old-complete-manifest");
        assert!(!temp.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn atomic_bound_file_replacement_rolls_back_a_post_exchange_failure() {
        let root = temp_dir("atomic-bound-replacement-post-exchange");
        fs::create_dir_all(&root).expect("root should create");
        let manifest = root.join("manifest.json");
        let temp = root.join("manifest.next.json");
        fs::write(&manifest, b"old-complete-manifest").expect("manifest should write");
        let rooted = RootedFs::new(&root).expect("root should open");
        let entry = rooted
            .bind_entry(&manifest, false)
            .expect("manifest should bind");
        let old_identity = rooted
            .bound_entry_identity(&entry)
            .expect("manifest identity should read")
            .expect("manifest should exist");

        let error = rooted
            .replace_bound_file_atomically_if_identity_with_hook(
                &entry,
                old_identity,
                &temp,
                b"new-complete-manifest",
                0o600,
                &mut |checkpoint| {
                    if checkpoint == AtomicBoundFileReplaceCheckpoint::AfterExchange {
                        bail!("injected post-exchange failure");
                    }
                    Ok(())
                },
            )
            .expect_err("post-exchange failure must roll the exchange back");

        assert!(format!("{error:#}").contains("injected post-exchange failure"));
        assert_eq!(fs::read(&manifest).unwrap(), b"old-complete-manifest");
        assert!(!temp.exists());
        assert_eq!(
            rooted.entry_identity(&manifest).unwrap(),
            Some(old_identity)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn atomic_bound_file_replacement_never_rolls_a_raced_temp_into_destination() {
        let root = temp_dir("atomic-bound-replacement-raced-temp");
        fs::create_dir_all(&root).expect("root should create");
        let manifest = root.join("manifest.json");
        let saved_manifest = root.join("manifest.saved.json");
        let temp = root.join("manifest.next.json");
        fs::write(&manifest, b"old-complete-manifest").expect("manifest should write");
        let rooted = RootedFs::new(&root).expect("root should open");
        let entry = rooted
            .bind_entry(&manifest, false)
            .expect("manifest should bind");
        let old_identity = rooted
            .bound_entry_identity(&entry)
            .expect("manifest identity should read")
            .expect("manifest should exist");

        let error = rooted
            .replace_bound_file_atomically_if_identity_with_hook(
                &entry,
                old_identity,
                &temp,
                b"new-complete-manifest",
                0o600,
                &mut |checkpoint| {
                    if checkpoint == AtomicBoundFileReplaceCheckpoint::AfterExchange {
                        fs::rename(&temp, &saved_manifest)
                            .context("failed to preserve displaced manifest")?;
                        fs::write(&temp, b"racer-owned-manifest")
                            .context("failed to install raced temp")?;
                    }
                    Ok(())
                },
            )
            .expect_err("a raced temp must prevent exchange rollback");

        assert!(
            format!("{error:#}")
                .contains("retained both atomic replacement entries because their post-exchange identities could not be proven")
        );
        assert_eq!(fs::read(&manifest).unwrap(), b"new-complete-manifest");
        assert_eq!(fs::read(&saved_manifest).unwrap(), b"old-complete-manifest");
        assert_eq!(fs::read(&temp).unwrap(), b"racer-owned-manifest");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn hard_link_anchor_prevents_committed_inode_reuse() {
        let root = temp_dir("hard-link-anchor");
        fs::create_dir_all(root.join("transaction")).expect("transaction should create");
        let output = root.join("video.mkv");
        let anchor = root.join("transaction/anchor");
        fs::write(&output, b"committed-video").expect("output should write");
        let rooted = RootedFs::new(&root).expect("root should open");
        let output_entry = rooted
            .bind_entry(&output, false)
            .expect("output should bind");
        let anchor_entry = rooted
            .bind_entry(&anchor, false)
            .expect("anchor should bind");
        let committed_identity = rooted
            .bound_entry_identity(&output_entry)
            .expect("output identity should read")
            .expect("output should exist");
        rooted
            .hard_link_via_bound_parents_noreplace_if_identity(
                &output_entry,
                &anchor_entry,
                committed_identity,
            )
            .expect("anchor should link");

        fs::remove_file(&output).expect("output should unlink");
        fs::write(&output, b"unrelated-replacement").expect("replacement should write");

        assert_eq!(fs::read(&anchor).unwrap(), b"committed-video");
        assert_eq!(
            rooted.entry_identity(&anchor).unwrap(),
            Some(committed_identity)
        );
        assert_ne!(
            rooted.entry_identity(&output).unwrap(),
            Some(committed_identity)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_is_an_existing_destination() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("dangling-symlink");
        fs::create_dir_all(root.join("staging")).expect("staging should create");
        fs::write(root.join("staging/source"), b"new").expect("source should write");
        symlink("missing", root.join("target")).expect("symlink should create");
        let rooted = RootedFs::new(&root).expect("root should open");

        rooted
            .rename_noreplace(&root.join("staging/source"), &root.join("target"), false)
            .expect_err("dangling symlink should reject the move");

        assert_eq!(
            fs::read_link(root.join("target")).unwrap(),
            PathBuf::from("missing")
        );
        assert_eq!(fs::read(root.join("staging/source")).unwrap(), b"new");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn replaced_parent_symlink_cannot_redirect_a_move() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("parent-replacement");
        let external = temp_dir("parent-replacement-external");
        fs::create_dir_all(root.join("staging")).expect("staging should create");
        fs::create_dir_all(root.join("library")).expect("library should create");
        fs::create_dir_all(&external).expect("external should create");
        fs::write(root.join("staging/source"), b"new").expect("source should write");
        fs::write(external.join("video"), b"external").expect("external file should write");
        let rooted = RootedFs::new(&root).expect("root should open");
        fs::rename(root.join("library"), root.join("library-original"))
            .expect("library should move");
        symlink(&external, root.join("library")).expect("replacement symlink should create");

        rooted
            .rename_noreplace(
                &root.join("staging/source"),
                &root.join("library/video"),
                false,
            )
            .expect_err("symlinked destination parent should reject the move");

        assert_eq!(fs::read(external.join("video")).unwrap(), b"external");
        assert_eq!(fs::read(root.join("staging/source")).unwrap(), b"new");
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(external);
    }

    #[cfg(unix)]
    #[test]
    fn retargeted_configured_root_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let parent = temp_dir("root-symlink-retarget");
        let original = parent.join("original");
        let replacement = parent.join("replacement");
        let configured = parent.join("configured");
        fs::create_dir_all(original.join("staging")).expect("original staging should create");
        fs::create_dir_all(&replacement).expect("replacement should create");
        fs::write(original.join("staging/source"), b"original")
            .expect("original source should write");
        fs::write(replacement.join("source"), b"replacement")
            .expect("replacement source should write");
        symlink(&original, &configured).expect("configured root symlink should create");
        let rooted = RootedFs::new(&configured).expect("configured root should bind");

        fs::remove_file(&configured).expect("configured symlink should remove");
        symlink(&replacement, &configured).expect("configured symlink should retarget");

        let error = rooted
            .rename_noreplace(
                &configured.join("staging/source"),
                &configured.join("target"),
                false,
            )
            .expect_err("retargeted configured root must reject mutations");

        assert!(error.to_string().contains("different directory"));
        assert_eq!(
            fs::read(original.join("staging/source")).unwrap(),
            b"original"
        );
        assert_eq!(
            fs::read(replacement.join("source")).unwrap(),
            b"replacement"
        );
        assert!(!replacement.join("target").exists());
        let _ = fs::remove_dir_all(parent);
    }

    #[cfg(unix)]
    #[test]
    fn bound_tree_cleanup_uses_the_original_root_after_symlink_retargeting() {
        use std::os::unix::fs::symlink;

        let parent = temp_dir("bound-tree-root-symlink-retarget");
        let original = parent.join("original");
        let replacement = parent.join("replacement");
        let configured = parent.join("configured");
        let relative_tree = Path::new("staging/job-1");
        fs::create_dir_all(original.join(relative_tree).join("nested"))
            .expect("original staging tree should create");
        fs::write(
            original.join(relative_tree).join("nested/video"),
            b"original",
        )
        .expect("original staged file should write");
        fs::create_dir_all(replacement.join(relative_tree))
            .expect("replacement staging tree should create");
        fs::write(
            replacement.join(relative_tree).join("sentinel"),
            b"replacement",
        )
        .expect("replacement sentinel should write");
        symlink(&original, &configured).expect("configured root symlink should create");
        let rooted = RootedFs::new(&configured).expect("configured root should bind");
        let logical_tree = configured.join(relative_tree);
        let entry = rooted
            .bind_entry(&logical_tree, false)
            .expect("staging tree should bind");
        let identity = rooted
            .bound_entry_identity(&entry)
            .expect("staging identity should read")
            .expect("staging tree should exist");

        fs::remove_file(&configured).expect("configured symlink should remove");
        symlink(&replacement, &configured).expect("configured symlink should retarget");
        rooted
            .remove_bound_tree_if_identity(&entry, identity)
            .expect("bound cleanup should remove only the original staging tree");

        assert!(!original.join(relative_tree).exists());
        assert_eq!(
            fs::read(replacement.join(relative_tree).join("sentinel")).unwrap(),
            b"replacement"
        );
        let _ = fs::remove_dir_all(parent);
    }

    #[cfg(unix)]
    #[test]
    fn bound_recovery_survives_configured_root_symlink_retargeting() {
        use std::os::unix::fs::symlink;

        let parent = temp_dir("bound-root-symlink-retarget");
        let original = parent.join("original");
        let replacement = parent.join("replacement");
        let configured = parent.join("configured");
        fs::create_dir_all(original.join("backup")).expect("backup directory should create");
        fs::create_dir_all(original.join("library")).expect("library directory should create");
        fs::create_dir_all(replacement.join("library")).expect("replacement library should create");
        fs::write(original.join("backup/video.mkv"), b"original").expect("backup should write");
        symlink(&original, &configured).expect("configured root symlink should create");
        let rooted = RootedFs::new(&configured).expect("configured root should bind");
        let source = rooted
            .bind_entry(&configured.join("backup/video.mkv"), false)
            .expect("backup should bind");
        let destination = rooted
            .bind_entry(&configured.join("library/video.mkv"), false)
            .expect("restore destination should bind");
        let identity = rooted
            .bound_entry_identity(&source)
            .expect("backup identity should read")
            .expect("backup should exist");

        fs::remove_file(&configured).expect("configured symlink should remove");
        symlink(&replacement, &configured).expect("configured symlink should retarget");

        rooted
            .rename_via_bound_parents_noreplace_if_identity(&source, &destination, identity)
            .expect("bound recovery should use the captured directory objects");

        assert_eq!(
            fs::read(original.join("library/video.mkv")).unwrap(),
            b"original"
        );
        assert!(!original.join("backup/video.mkv").exists());
        assert!(!replacement.join("library/video.mkv").exists());
        let _ = fs::remove_dir_all(parent);
    }

    #[test]
    fn bound_validation_rollback_never_moves_a_replacement_object() {
        let root = temp_dir("bound-rollback-replacement");
        fs::create_dir_all(root.join("staging")).expect("staging should create");
        fs::create_dir_all(root.join("library")).expect("library should create");
        let source_path = root.join("staging/video.mkv");
        let destination_path = root.join("library/video.mkv");
        fs::write(&destination_path, b"owned").expect("owned destination should write");
        let rooted = RootedFs::new(&root).expect("root should bind");
        let source = rooted
            .bind_entry(&source_path, false)
            .expect("source should bind");
        let destination = rooted
            .bind_entry(&destination_path, false)
            .expect("destination should bind");
        let expected = rooted
            .bound_entry_identity(&destination)
            .expect("destination identity should read")
            .expect("destination should exist");

        fs::remove_file(&destination_path).expect("owned destination should remove");
        fs::write(&destination_path, b"replacement").expect("replacement should write");
        let error = rollback_bound_rename(&source, &destination, expected)
            .expect_err("replacement object must not be rolled back");

        assert!(error.to_string().contains("identity changed"));
        assert!(!source_path.exists());
        assert_eq!(fs::read(destination_path).unwrap(), b"replacement");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn identity_bound_removal_never_unlinks_a_racing_replacement() {
        let root = temp_dir("bound-remove-replacement");
        fs::create_dir_all(&root).expect("root should create");
        let owned_path = root.join("owned.txt");
        let retained_path = root.join("retained-owned.txt");
        fs::write(&owned_path, b"owned").expect("owned file should write");
        let rooted = RootedFs::new(&root).expect("root should bind");
        let entry = rooted
            .bind_entry(&owned_path, false)
            .expect("owned file should bind");
        let expected = rooted
            .bound_entry_identity(&entry)
            .expect("owned identity should read")
            .expect("owned file should exist");

        let error = rooted
            .remove_bound_file_if_identity_with_hook(&entry, expected, || {
                fs::rename(&owned_path, &retained_path).expect("owned file should move aside");
                fs::write(&owned_path, b"replacement").expect("replacement should write");
            })
            .expect_err("racing replacement must not be removed");

        assert!(
            error
                .to_string()
                .contains("identity changed before removal")
        );
        assert_eq!(fs::read(&owned_path).unwrap(), b"replacement");
        assert_eq!(fs::read(&retained_path).unwrap(), b"owned");
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(REMOVE_QUARANTINE_PREFIX)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validated_removal_restores_the_quarantined_object_on_rejection() {
        let root = temp_dir("bound-remove-validation-rollback");
        fs::create_dir_all(&root).expect("root should create");
        let owned_path = root.join("owned.txt");
        fs::write(&owned_path, b"owned").expect("owned file should write");
        let rooted = RootedFs::new(&root).expect("root should bind");
        let entry = rooted
            .bind_entry(&owned_path, false)
            .expect("owned file should bind");
        let expected = rooted
            .bound_entry_identity(&entry)
            .expect("owned identity should read")
            .expect("owned file should exist");

        let error = rooted
            .remove_bound_file_if_identity_with_validation(&entry, expected, || {
                bail!("active credential identity appeared")
            })
            .expect_err("rejected removal must restore its selected object");

        assert!(format!("{error:#}").contains("active credential identity appeared"));
        assert_eq!(fs::read(&owned_path).unwrap(), b"owned");
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(REMOVE_QUARANTINE_PREFIX)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn interrupted_validated_removal_requires_an_explicit_recovery_decision() {
        let root = temp_dir("validated-remove-recovery-decision");
        fs::create_dir_all(&root).expect("root should create");
        let owned_path = root.join("owned.txt");
        fs::write(&owned_path, b"owned").expect("owned file should write");
        let rooted = RootedFs::new(&root).expect("root should bind");
        let entry = rooted
            .bind_entry(&owned_path, false)
            .expect("owned file should bind");
        let expected = rooted
            .bound_entry_identity(&entry)
            .expect("owned identity should read")
            .expect("owned file should exist");
        rooted
            .leave_validated_file_removal_quarantined_for_test(&entry, expected)
            .expect("interrupted validated removal should persist");

        let report = rooted
            .reconcile_remove_quarantines_with_status()
            .expect("unresolved recovery should be reported");

        assert!(report.unresolved);
        assert!(!report.restored);
        assert!(!owned_path.exists());
        assert!(report.messages.iter().any(|message| {
            message.contains("interrupted validated removal requires caller revalidation")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn interrupted_validated_removal_can_be_restored_or_deleted_after_revalidation() {
        for should_restore in [true, false] {
            let root = temp_dir(if should_restore {
                "validated-remove-recovery-restore"
            } else {
                "validated-remove-recovery-delete"
            });
            fs::create_dir_all(&root).expect("root should create");
            let owned_path = root.join("owned.txt");
            fs::write(&owned_path, b"owned").expect("owned file should write");
            let rooted = RootedFs::new(&root).expect("root should bind");
            let entry = rooted
                .bind_entry(&owned_path, false)
                .expect("owned file should bind");
            let expected = rooted
                .bound_entry_identity(&entry)
                .expect("owned identity should read")
                .expect("owned file should exist");
            rooted
                .leave_validated_file_removal_quarantined_for_test(&entry, expected)
                .expect("interrupted validated removal should persist");

            let report = rooted
                .reconcile_remove_quarantines_with_status_and_restore_decider(|candidate| {
                    assert_eq!(candidate, expected);
                    Ok(should_restore)
                })
                .expect("validated removal should reconcile");

            assert!(!report.unresolved);
            assert_eq!(report.restored, should_restore);
            assert_eq!(owned_path.exists(), should_restore);
            if should_restore {
                assert_eq!(fs::read(&owned_path).unwrap(), b"owned");
                assert!(report.messages.iter().any(|message| {
                    message.contains("Restored interrupted validated bound-path removal")
                }));
            } else {
                assert!(report.messages.iter().any(|message| {
                    message.contains("Recovered interrupted bound-path removal")
                }));
            }
            assert!(fs::read_dir(&root).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(REMOVE_QUARANTINE_PREFIX)
            }));
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn interrupted_identity_bound_removal_is_reconciled_from_its_manifest() {
        let root = temp_dir("bound-remove-recovery");
        let backup = root.join("backup");
        fs::create_dir_all(&backup).expect("backup directory should create");
        let owned_path = backup.join("owned.txt");
        fs::write(&owned_path, b"owned").expect("owned file should write");
        let rooted = RootedFs::new(&root).expect("root should bind");
        let entry = rooted
            .bind_entry(&owned_path, false)
            .expect("owned file should bind");
        let expected = rooted
            .bound_entry_identity(&entry)
            .expect("owned identity should read")
            .expect("owned file should exist");

        let error = rooted
            .remove_bound_file_if_identity_with_post_quarantine_hook(&entry, expected, || {
                bail!("simulated process interruption")
            })
            .expect_err("simulated interruption should retain the quarantine");

        assert!(error.to_string().contains("retained quarantine"));
        assert!(!owned_path.exists());
        assert!(fs::read_dir(&backup).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(REMOVE_QUARANTINE_PREFIX)
        }));

        let reports = rooted
            .reconcile_remove_quarantines()
            .expect("interrupted removal should reconcile");

        assert!(
            reports
                .iter()
                .any(|report| report.contains("Recovered interrupted bound-path removal"))
        );
        assert!(!owned_path.exists());
        assert!(fs::read_dir(&backup).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(REMOVE_QUARANTINE_PREFIX)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ordinary_cleanup_json_file_does_not_poison_recovery() {
        let root = temp_dir("ordinary-cleanup-json");
        let library = root.join("library");
        fs::create_dir_all(&library).expect("library should create");
        let ordinary = library.join("movie.cleanup.json");
        fs::write(&ordinary, b"ordinary metadata").expect("ordinary file should write");
        let rooted = RootedFs::new(&root).expect("root should bind");

        let report = rooted
            .reconcile_remove_quarantines_with_status()
            .expect("ordinary cleanup JSON should be ignored");

        assert!(!report.unresolved);
        assert!(report.messages.is_empty());
        assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary metadata");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_remove_quarantine_tombstone_recovers_both_cleanup_windows() {
        for remove_inner_manifest in [false, true] {
            let root = temp_dir(if remove_inner_manifest {
                "remove-tombstone-after-manifest"
            } else {
                "remove-tombstone-before-manifest"
            });
            fs::create_dir_all(&root).expect("root should create");
            let owned_path = root.join("owned.txt");
            fs::write(&owned_path, b"owned").expect("owned file should write");
            let rooted = RootedFs::new(&root).expect("root should bind");
            let entry = rooted
                .bind_entry(&owned_path, false)
                .expect("owned file should bind");
            let expected = rooted
                .bound_entry_identity(&entry)
                .expect("owned identity should read")
                .expect("owned file should exist");
            let quarantine = create_private_remove_quarantine(
                &entry.parent,
                &entry.leaf,
                expected,
                RemoveQuarantinePolicy::retain(false),
            )
            .expect("quarantine should create");
            renameat_noreplace(
                entry.parent.fd.as_ref(),
                &entry.leaf,
                &quarantine.directory,
                OsStr::new("entry"),
            )
            .expect("owned file should move into quarantine");
            rustix::fs::unlinkat(&quarantine.directory, "entry", AtFlags::empty())
                .expect("quarantined file should remove");
            sync_directory(&quarantine.directory).expect("quarantine should sync");

            let quarantine_name = CString::new(quarantine.name.to_string_lossy().as_bytes())
                .expect("generated quarantine name should be valid");
            let tombstone_name = remove_quarantine_tombstone_name(&quarantine_name);
            create_or_validate_remove_quarantine_tombstone(
                entry.parent.fd.as_ref(),
                &tombstone_name,
                &quarantine.manifest,
            )
            .expect("terminal tombstone should persist");
            if remove_inner_manifest {
                rustix::fs::unlinkat(
                    &quarantine.directory,
                    REMOVE_QUARANTINE_MANIFEST_NAME,
                    AtFlags::empty(),
                )
                .expect("inner manifest should remove");
                sync_directory(&quarantine.directory).expect("quarantine should sync");
            }

            let reports = rooted
                .reconcile_remove_quarantines()
                .expect("terminal tombstone should reconcile");

            assert!(
                reports
                    .iter()
                    .any(|report| report.contains("Recovered interrupted bound-path cleanup"))
            );
            assert!(fs::read_dir(&root).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(REMOVE_QUARANTINE_PREFIX)
            }));
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn previous_version_remove_tombstone_remains_recoverable() {
        let root = temp_dir("previous-remove-tombstone");
        fs::create_dir_all(&root).expect("root should create");
        let owned_path = root.join("owned.txt");
        fs::write(&owned_path, b"owned").expect("owned file should write");
        let rooted = RootedFs::new(&root).expect("root should bind");
        let entry = rooted
            .bind_entry(&owned_path, false)
            .expect("owned file should bind");
        let expected = rooted
            .bound_entry_identity(&entry)
            .expect("owned identity should read")
            .expect("owned file should exist");
        let quarantine = create_private_remove_quarantine(
            &entry.parent,
            &entry.leaf,
            expected,
            RemoveQuarantinePolicy::retain(false),
        )
        .expect("quarantine should create");
        renameat_noreplace(
            entry.parent.fd.as_ref(),
            &entry.leaf,
            &quarantine.directory,
            OsStr::new("entry"),
        )
        .expect("owned file should move into quarantine");
        rustix::fs::unlinkat(&quarantine.directory, "entry", AtFlags::empty())
            .expect("quarantined file should remove");
        sync_directory(&quarantine.directory).expect("quarantine should sync");

        let mut previous_manifest = quarantine.manifest.clone();
        previous_manifest.version = REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION;
        previous_manifest.original_name_hex = None;
        previous_manifest.restore_requires_revalidation = false;
        let manifest_contents =
            serde_json::to_vec(&previous_manifest).expect("previous manifest should encode");
        fs::write(
            root.join(&quarantine.name)
                .join(REMOVE_QUARANTINE_MANIFEST_NAME),
            &manifest_contents,
        )
        .expect("previous inner manifest should write");
        let quarantine_name = CString::new(quarantine.name.to_string_lossy().as_bytes())
            .expect("generated quarantine name should be valid");
        let tombstone_name = remove_quarantine_tombstone_name(&quarantine_name);
        create_or_validate_remove_quarantine_tombstone(
            entry.parent.fd.as_ref(),
            &tombstone_name,
            &previous_manifest,
        )
        .expect("previous terminal tombstone should persist");

        let reports = rooted
            .reconcile_remove_quarantines()
            .expect("previous terminal tombstone should reconcile");

        assert!(
            reports
                .iter()
                .any(|report| report.contains("Recovered interrupted bound-path cleanup"))
        );
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(REMOVE_QUARANTINE_PREFIX)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_quarantine_tombstones_survive_a_restart_after_directory_removal() {
        for legacy_version in [
            REMOVE_QUARANTINE_LEGACY_MANIFEST_VERSION,
            REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION,
        ] {
            let root = temp_dir(&format!("legacy-remove-tombstone-v{legacy_version}"));
            fs::create_dir_all(&root).expect("root should create");
            let owned_path = root.join("owned.txt");
            fs::write(&owned_path, b"owned").expect("owned file should write");
            let rooted = RootedFs::new(&root).expect("root should bind");
            let entry = rooted
                .bind_entry(&owned_path, false)
                .expect("owned file should bind");
            let expected = rooted
                .bound_entry_identity(&entry)
                .expect("owned identity should read")
                .expect("owned file should exist");
            let quarantine = create_private_remove_quarantine(
                &entry.parent,
                &entry.leaf,
                expected,
                RemoveQuarantinePolicy::retain(false),
            )
            .expect("quarantine should create");
            renameat_noreplace(
                entry.parent.fd.as_ref(),
                &entry.leaf,
                &quarantine.directory,
                OsStr::new("entry"),
            )
            .expect("owned file should move into quarantine");

            let mut legacy_manifest = quarantine.manifest.clone();
            legacy_manifest.version = legacy_version;
            legacy_manifest.original_name_hex = None;
            legacy_manifest.restore_requires_revalidation = false;
            if legacy_version == REMOVE_QUARANTINE_LEGACY_MANIFEST_VERSION {
                legacy_manifest.quarantine_device = None;
                legacy_manifest.quarantine_inode = None;
            }
            let inner_manifest_path = root
                .join(&quarantine.name)
                .join(REMOVE_QUARANTINE_MANIFEST_NAME);
            fs::write(
                &inner_manifest_path,
                serde_json::to_vec(&legacy_manifest).expect("legacy manifest should encode"),
            )
            .expect("legacy manifest should replace current contents");
            File::open(&inner_manifest_path)
                .expect("legacy manifest should reopen")
                .sync_all()
                .expect("legacy manifest should sync");
            rustix::fs::unlinkat(&quarantine.directory, "entry", AtFlags::empty())
                .expect("quarantined file should remove");
            sync_directory(&quarantine.directory).expect("quarantine should sync");

            let quarantine_name = CString::new(quarantine.name.to_string_lossy().as_bytes())
                .expect("generated quarantine name should be valid");
            let terminal_manifest =
                build_terminal_remove_quarantine_manifest(&legacy_manifest, quarantine.identity);
            assert_eq!(
                terminal_manifest.version,
                REMOVE_QUARANTINE_PREVIOUS_MANIFEST_VERSION
            );
            assert!(terminal_manifest.original_name_hex.is_none());
            let tombstone_name = remove_quarantine_tombstone_name(&quarantine_name);
            create_or_validate_remove_quarantine_tombstone(
                entry.parent.fd.as_ref(),
                &tombstone_name,
                &terminal_manifest,
            )
            .expect("legacy terminal tombstone should persist");

            rustix::fs::unlinkat(
                &quarantine.directory,
                REMOVE_QUARANTINE_MANIFEST_NAME,
                AtFlags::empty(),
            )
            .expect("inner legacy manifest should remove");
            sync_directory(&quarantine.directory).expect("quarantine should sync");
            rustix::fs::unlinkat(
                entry.parent.fd.as_ref(),
                &quarantine.name,
                AtFlags::REMOVEDIR,
            )
            .expect("empty quarantine directory should remove");
            sync_directory(entry.parent.fd.as_ref()).expect("quarantine parent should sync");
            drop(quarantine);

            let reports = rooted
                .reconcile_remove_quarantines()
                .expect("standalone legacy tombstone should recover after restart");

            assert!(
                reports
                    .iter()
                    .any(|report| report.contains("Recovered interrupted bound-path cleanup"))
            );
            assert!(fs::read_dir(&root).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(REMOVE_QUARANTINE_PREFIX)
            }));
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn unowned_remove_quarantine_name_is_retained() {
        let root = temp_dir("bound-remove-unowned-quarantine");
        fs::create_dir_all(&root).expect("root should create");
        let quarantine = root.join(format!("{REMOVE_QUARANTINE_PREFIX}-forged"));
        fs::create_dir(&quarantine).expect("unowned quarantine should create");
        fs::write(quarantine.join("entry"), b"user-owned")
            .expect("unowned quarantine entry should write");
        let rooted = RootedFs::new(&root).expect("root should bind");

        let reports = rooted
            .reconcile_remove_quarantines()
            .expect("unowned quarantine should be reported");

        assert!(
            reports
                .iter()
                .any(|report| report.contains("Retained unresolved"))
        );
        assert_eq!(fs::read(quarantine.join("entry")).unwrap(), b"user-owned");
        let _ = fs::remove_dir_all(root);
    }
}
