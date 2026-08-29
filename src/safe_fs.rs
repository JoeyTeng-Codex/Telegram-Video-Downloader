use std::ffi::{OsStr, OsString};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, FileType, Mode, OFlags, RenameFlags};

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

    fn is_dir(self) -> bool {
        self.file_type == FileType::Directory
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
            let rollback = renameat_noreplace(
                destination.parent.fd.as_ref(),
                &destination.leaf,
                source.parent.fd.as_ref(),
                &source.leaf,
            );
            return Err(with_cleanup_error(err, rollback, "bound move rollback"));
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

    pub(crate) fn remove_bound_dir_if_identity(
        &self,
        entry: &BoundEntry,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.remove_bound_entry_if_identity(entry, expected, AtFlags::REMOVEDIR)
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
        Ok(Some(identity))
    }

    pub(crate) fn rename_noreplace(
        &self,
        source: &Path,
        destination: &Path,
        create_destination_parents: bool,
    ) -> Result<EntryIdentity> {
        self.rename_noreplace_inner(source, destination, create_destination_parents, None)
    }

    pub(crate) fn rename_noreplace_if_identity(
        &self,
        source: &Path,
        destination: &Path,
        create_destination_parents: bool,
        expected: EntryIdentity,
    ) -> Result<()> {
        self.rename_noreplace_inner(
            source,
            destination,
            create_destination_parents,
            Some(expected),
        )?;
        Ok(())
    }

    fn rename_noreplace_inner(
        &self,
        source: &Path,
        destination: &Path,
        create_destination_parents: bool,
        expected: Option<EntryIdentity>,
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
        if expected.is_some_and(|expected| expected != source_identity) {
            bail!("move source identity changed: {}", source.display());
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
        self.validate_bound_parent(&entry.parent)?;
        let current = identity_at(entry.parent.fd.as_ref(), &entry.leaf)?
            .ok_or_else(|| anyhow!("owned bound path is missing: {}", entry.path.display()))?;
        if current != expected {
            bail!(
                "owned bound path identity changed: {}",
                entry.path.display()
            );
        }
        rustix::fs::unlinkat(entry.parent.fd.as_ref(), &entry.leaf, flags)
            .map_err(errno_to_io)
            .with_context(|| {
                format!("failed to remove owned bound path {}", entry.path.display())
            })?;
        self.validate_bound_parent(&entry.parent)
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

fn identity_for_fd(fd: &OwnedFd) -> Result<EntryIdentity> {
    let stat = rustix::fs::fstat(fd)
        .map_err(errno_to_io)
        .context("failed to inspect bound filesystem object")?;
    Ok(identity_from_stat(&stat))
}

fn identity_at(parent: &OwnedFd, name: &OsStr) -> Result<Option<EntryIdentity>> {
    match rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(identity_from_stat(&stat))),
        Err(err) if err == rustix::io::Errno::NOENT => Ok(None),
        Err(err) => Err(errno_to_io(err)).context("failed to inspect output entry"),
    }
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
}
