//! Atomic checkpoint files using advisory locks, fsync and rename.

use std::{fs::{self, File, OpenOptions}, io::{Read, Write}, path::{Path, PathBuf}, sync::atomic::{AtomicU64, Ordering}};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crate::{Checkpoint, DiscoveryCheckpoint, Error, Result, types::{ChainIdentity, PoolSpec}};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Synchronous checkpoint storage for local filesystems.
/// Each write acquires an exclusive advisory lock.
#[derive(Debug, Clone)]
pub struct FileStore {
    path: PathBuf,
    max_bytes: usize,
}

impl FileStore {
    /// Chooses a path without creating files. Default size limit: 128 MiB.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), max_bytes: 128 * 1024 * 1024 }
    }

    /// Sets a positive serialized-checkpoint cap for load/save.
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Result<Self> {
        if max_bytes == 0 {
            return Err(Error::InvalidConfig("checkpoint max_bytes must be positive".into()));
        }
        self.max_bytes = max_bytes;
        Ok(self)
    }

    /// Checkpoint path.
    pub fn path(&self) -> &Path { &self.path }

    /// Loads and validates a checkpoint; returns `None` if absent.
    /// Rejects symlinks and nonregular files. Restore and sync before using the state.
    pub fn load(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<Option<Checkpoint>> {
        let Some(bytes) = self.read_bytes()? else { return Ok(None); };
        let checkpoint: Checkpoint = serde_json::from_slice(&bytes)?;
        checkpoint.validate(identity, spec)?;
        Ok(Some(checkpoint))
    }

    /// Moves a corrupt or mismatched SDK checkpoint to a unique mode-0600 backup (Unix).
    ///
    /// Revalidates under the writer lock. Valid or absent files return `None`;
    /// I/O errors, size-limit errors, symlinks and nonregular files remain errors.
    /// Rebuild from the trusted deployment floor after quarantine. Use only for
    /// reconstructible SDK state, never a transaction journal.
    pub fn quarantine_invalid(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<Option<PathBuf>> {
        self.quarantine_bytes(|| self.load(identity, spec).map(|_| ()))
    }

    pub(crate) fn quarantine_bytes(&self, validate: impl FnOnce() -> Result<()>) -> Result<Option<PathBuf>> {
        #[cfg(not(unix))]
        return Err(Error::InvalidConfig("checkpoint quarantine requires Unix inode identity".into()));
        reject_non_regular(&self.path)?;
        if !self.path.try_exists()? { return Ok(None); }
        let parent = self.path.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or(Path::new("."));
        let name = self.path.file_name().ok_or_else(|| Error::InvalidConfig("checkpoint path has no filename".into()))?;
        let _lock = acquire_checkpoint_lock(parent, name)?;
        reject_non_regular(&self.path)?;
        let source = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let source_metadata = source.metadata()?;
        if !source_metadata.is_file() { return Err(Error::Checkpoint("quarantine source is not a regular file".into())); }
        ensure_same_inode(&source_metadata, &fs::symlink_metadata(&self.path)?)?;
        match validate() {
            Ok(()) => return Ok(None),
            Err(Error::Json(_) | Error::Checkpoint(_) | Error::IdentityMismatch(_)) => {},
            Err(error) => return Err(error),
        }
        // Own a unique destination before rename; a failed create_new never removes
        // another file. Tighten source permissions through its checked descriptor.
        let (backup_path, backup) = create_quarantine_backup(parent, name)?;
        let mut guard = RemoveOnDrop(Some(backup_path.clone()));
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            source.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        source.sync_all()?;
        reject_non_regular(&self.path)?;
        ensure_same_inode(&source_metadata, &fs::symlink_metadata(&self.path)?)?;
        ensure_same_inode(&backup.metadata()?, &fs::symlink_metadata(&backup_path)?)?;
        fs::rename(&self.path, &backup_path)?;
        guard.0 = None; // The backup now owns preserved cache bytes, even if directory sync fails.
        File::open(parent)?.sync_all()?;
        Ok(Some(backup_path))
    }

    pub(crate) fn read_bytes(&self) -> Result<Option<Vec<u8>>> {
        reject_non_regular(&self.path)?;
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !file.metadata()?.is_file() {
            return Err(Error::Checkpoint("checkpoint path is not a regular file".into()));
        }
        let mut bytes = Vec::new();
        file.take((self.max_bytes as u64).saturating_add(1)).read_to_end(&mut bytes)?;
        if bytes.len() > self.max_bytes {
            return Err(Error::ResourceLimit("checkpoint exceeds max_bytes".into()));
        }
        Ok(Some(bytes))
    }

    /// Validates and atomically persists a checkpoint under an advisory writer lock.
    ///
    /// Rejects older/conflicting cursors and foreign identities. Uses mode-0600
    /// temporary files and retains the `.lock` file after releasing ownership.
    /// A failed save may have replaced the file; retry or reload before continuing.
    pub fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        checkpoint.validate(None, None)?;
        let bytes = self.encode(checkpoint)?;
        self.save_bytes(&bytes, || {
            if let Some(previous) = self.load(None, None)? {
                if previous.snapshot.identity != checkpoint.snapshot.identity
                    || previous.snapshot.spec != checkpoint.snapshot.spec
                    || previous.snapshot.pool != checkpoint.snapshot.pool
                {
                    return Err(Error::IdentityMismatch("checkpoint file belongs to another chain/deployment".into()));
                }
                if previous.snapshot.epoch > checkpoint.snapshot.epoch
                    || previous.snapshot.epoch == checkpoint.snapshot.epoch
                        && (previous.snapshot.as_of.number > checkpoint.snapshot.as_of.number
                            || previous.snapshot.as_of.number == checkpoint.snapshot.as_of.number
                                && previous.snapshot.as_of.hash != checkpoint.snapshot.as_of.hash)
                {
                    return Err(Error::Checkpoint("refusing to overwrite a newer/conflicting cursor".into()));
                }
            }
            Ok(())
        })
    }

    pub(crate) fn encode(&self, value: &impl serde::Serialize) -> Result<Vec<u8>> {
        crate::encoding::encode(value, self.max_bytes)
    }

    pub(crate) fn save_bytes(&self, bytes: &[u8], check_previous: impl FnOnce() -> Result<()>) -> Result<()> {
        if bytes.len() > self.max_bytes {
            return Err(Error::ResourceLimit("checkpoint exceeds max_bytes".into()));
        }
        let parent = self.path.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        reject_non_regular(&self.path)?;
        let name = self.path.file_name().ok_or_else(|| Error::InvalidConfig("checkpoint path has no filename".into()))?;
        let mut lock = acquire_checkpoint_lock(parent, name)?;
        // The stable lock inode is never removed: unlinking it could create two independent locks.
        // OS releases the advisory lock even after SIGKILL; file existence is not ownership.
        check_previous()?;
        lock.set_len(0)?;
        writeln!(lock, "{}", std::process::id())?;
        lock.sync_all()?;
        let (temp_path, mut temp) = create_temporary(parent, name)?;
        // Own the path before installing cleanup: create_new failure must never unlink
        // another writer's/stale file. Reused process IDs may leave matching old names.
        let mut temp_guard = RemoveOnDrop(Some(temp_path.clone()));
        temp.write_all(bytes)?;
        temp.sync_all()?;
        fs::rename(&temp_path, &self.path)?;
        // The temporary name no longer belongs to this write after rename. A new
        // file appearing at that name must not be removed during later cleanup.
        temp_guard.0 = None;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

/// Atomic storage for unfinished discovery, separate from complete state.
#[derive(Debug, Clone)]
pub struct DiscoveryFileStore { inner: FileStore }

impl DiscoveryFileStore {
    /// Chooses an independent path without creating it; default cap128MiB.
    pub fn new(path: impl Into<PathBuf>) -> Self { Self { inner: FileStore::new(path) } }
    /// Sets the positive bound for serialized progress reads/writes.
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Result<Self> {
        self.inner = self.inner.with_max_bytes(max_bytes)?;
        Ok(self)
    }
    /// Progress path. Must differ from the complete-state checkpoint path.
    pub fn path(&self) -> &Path { self.inner.path() }

    /// Loads and validates bounded local progress; the next engine step checks live anchors.
    pub fn load(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<Option<DiscoveryCheckpoint>> {
        let Some(bytes) = self.inner.read_bytes()? else { return Ok(None); };
        let progress: DiscoveryCheckpoint = serde_json::from_slice(&bytes)?;
        progress.validate(identity, spec)?;
        Ok(Some(progress))
    }

    /// Preserves invalid discovery using the policy of `FileStore::quarantine_invalid`.
    pub fn quarantine_invalid(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<Option<PathBuf>> {
        self.inner.quarantine_bytes(|| self.load(identity, spec).map(|_| ()))
    }

    /// Atomically saves validated progress. Same-base older/conflicting cursors are rejected;
    /// a higher generation permits an explicit canonicality reset to rewind history.
    pub fn save(&self, progress: &DiscoveryCheckpoint) -> Result<()> {
        progress.validate(None, None)?;
        let bytes = self.inner.encode(progress)?;
        self.inner.save_bytes(&bytes, || {
            if let Some(previous) = self.load(None, None)? {
                if previous.identity != progress.identity || previous.spec != progress.spec {
                    return Err(Error::IdentityMismatch("discovery file belongs to another chain/deployment".into()));
                }
                if previous.base_checksum == progress.base_checksum {
                    if previous.generation == progress.generation && previous.target == progress.target
                        && previous.through == progress.through && previous.getters.as_ref().is_some_and(|old|
                            progress.getters.as_ref().is_none_or(|next| !next.continues(old)))
                    {
                        return Err(Error::Checkpoint("refusing to overwrite newer/conflicting getter progress".into()));
                    }
                    let previous_height = previous.through.map(|cursor| cursor.number);
                    let next_height = progress.through.map(|cursor| cursor.number);
                    if previous.generation > progress.generation || previous.generation == progress.generation
                        && (previous_height > next_height || previous_height == next_height
                            && previous.through.is_some() && previous.through != progress.through)
                    {
                        return Err(Error::Checkpoint("refusing to overwrite newer/conflicting discovery".into()));
                    }
                }
            }
            Ok(())
        })
    }

    /// Removes discovery after saving the complete state checkpoint.
    /// If cleanup is interrupted, `restore_discovery` discards obsolete progress.
    pub fn remove(&self) -> Result<()> { self.remove_for(None, None) }

    /// Removes only progress matching the expected deployment, validated under the writer lock.
    /// A valid file for another chain/pool is preserved and returns IdentityMismatch.
    pub fn remove_for(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<()> {
        let path = self.inner.path();
        reject_non_regular(path)?;
        let parent = path.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or(Path::new("."));
        if !parent.exists() { return Ok(()); }
        let name = path.file_name().ok_or_else(|| Error::InvalidConfig("discovery path has no filename".into()))?;
        let _lock = acquire_checkpoint_lock(parent, name)?;
        // Validate before removal, including schema, so a misconfigured StateCheckpoint
        // path is not accidentally deleted by progress cleanup.
        if self.load(identity, spec)?.is_none() { return Ok(()); }
        fs::remove_file(path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn acquire_checkpoint_lock(parent: &Path, name: &std::ffi::OsStr) -> Result<File> {
    let path = parent.join(format!("{}.lock", name.to_string_lossy()));
    reject_non_regular(&path)?;
    let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
    let metadata = lock.metadata()?;
    if !metadata.is_file() { return Err(Error::Checkpoint("checkpoint lock path is not a regular file".into())); }
    lock.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => Error::Checkpoint("checkpoint is locked by another writer".into()),
        std::fs::TryLockError::Error(error) => Error::Io(error),
    })?;
    reject_non_regular(&path)?;
    ensure_same_inode(&metadata, &fs::symlink_metadata(path)?)?;
    Ok(lock)
}

fn ensure_same_inode(opened: &fs::Metadata, current: &fs::Metadata) -> Result<()> {
    #[cfg(unix)] {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() == current.dev() && opened.ino() == current.ino() { return Ok(()); }
        Err(Error::Checkpoint("checkpoint inode changed during locked operation".into()))
    }
    #[cfg(not(unix))] {
        let _ = (opened, current);
        Ok(()) // Preserve the pre-existing non-Unix save/remove lock contract.
    }
}

fn create_quarantine_backup(parent: &Path, name: &std::ffi::OsStr) -> Result<(PathBuf, File)> {
    for _ in 0..128 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".{}.quarantine.{}.{}.json", name.to_string_lossy(), std::process::id(), sequence));
        let mut options = OpenOptions::new(); options.write(true).create_new(true);
        #[cfg(unix)] options.mode(0o600);
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::ResourceLimit("checkpoint quarantine name collision budget exhausted".into()))
}

// The writer lock reserves this exact derived-cache scratch name. Reusing one
// slot prevents repeated SIGKILLs from retaining a new large file every restart.
// Legacy PID-named scratch/quarantine files are deliberately left untouched.
fn create_temporary(parent: &Path, name: &std::ffi::OsStr) -> Result<(PathBuf, File)> {
    let path = parent.join(format!(".{}.write.tmp", name.to_string_lossy()));
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() {return Err(Error::Checkpoint("unsafe checkpoint scratch file".into()));}
            #[cfg(unix)] {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink()!=1 || metadata.mode() & 0o077 != 0 {return Err(Error::Checkpoint("unsafe checkpoint scratch file".into()));}
            }
            // Unlink the reserved scratch inode, never open/truncate a possible alias.
            fs::remove_file(&path)?;
        }
        Err(error) if error.kind()==std::io::ErrorKind::NotFound => {},
        Err(error) => return Err(error.into()),
    }
    let mut options=OpenOptions::new(); options.write(true).create_new(true);
    #[cfg(unix)] options.mode(0o600);
    Ok((path.clone(),options.open(&path)?))
}

fn reject_non_regular(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::Checkpoint("checkpoint path is a symlink".into())),
        Ok(metadata) if !metadata.is_file() => Err(Error::Checkpoint("checkpoint path is not a regular file".into())),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

struct RemoveOnDrop(Option<PathBuf>);
impl Drop for RemoveOnDrop {
    fn drop(&mut self) { if let Some(path) = &self.0 { let _ = fs::remove_file(path); } }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantine_io_and_size_failures_preserve_original_and_create_no_backup() {
        let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("state.json");
        fs::write(&path, b"invalid cache bytes").unwrap(); let store = FileStore::new(&path);
        assert!(matches!(store.quarantine_bytes(|| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into())), Err(Error::Io(_))));
        assert!(matches!(store.quarantine_bytes(|| Err(Error::ResourceLimit("configured cap".into()))), Err(Error::ResourceLimit(_))));
        assert_eq!(fs::read(&path).unwrap(), b"invalid cache bytes");
        assert!(directory.path().read_dir().unwrap().all(|entry| !entry.unwrap().file_name().to_string_lossy().contains("quarantine")));
        // A writer fixing the file before locked revalidation must win: no forced rename.
        assert!(store.quarantine_bytes(|| Ok(())).unwrap().is_none()); assert!(path.exists());
    }

    #[test]
    fn quarantine_rejects_writer_lock_and_inode_replacement_without_losing_either_file() {
        let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("state.json");
        fs::write(&path, b"original").unwrap();
        let lock = acquire_checkpoint_lock(directory.path(), path.file_name().unwrap()).unwrap();
        let store = FileStore::new(&path);
        assert!(store.quarantine_bytes(|| Err(Error::Checkpoint("bad checksum".into()))).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"original"); drop(lock);
        let moved = directory.path().join("old.json");
        assert!(store.quarantine_bytes(|| {
            fs::rename(&path, &moved)?; fs::write(&path, b"concurrent replacement")?;
            Err(Error::Checkpoint("bad checksum".into()))
        }).is_err());
        assert_eq!(fs::read(moved).unwrap(), b"original"); assert_eq!(fs::read(path).unwrap(), b"concurrent replacement");
        assert!(directory.path().read_dir().unwrap().all(|entry| !entry.unwrap().file_name().to_string_lossy().contains("quarantine")));
    }

    #[test]
    #[ignore = "subprocess SIGKILL fixture; run explicitly with --test-threads=1"]
    fn killed_checkpoint_writer_reuses_one_scratch_slot_and_preserves_committed_state() {
        const ENV:&str="BL_STATE_TEST_CRASH_DIRECTORY";
        let name=std::ffi::OsStr::new("checkpoint.json");
        if let Some(directory)=std::env::var_os(ENV) {
            let directory=PathBuf::from(directory);
            let _lock=acquire_checkpoint_lock(&directory,name).unwrap();
            let (_,mut file)=create_temporary(&directory,name).unwrap();
            file.write_all(&vec![7;1024*1024]).unwrap();file.sync_all().unwrap();
            fs::write(directory.join("ready"),b"ready").unwrap();
            loop {std::thread::sleep(std::time::Duration::from_secs(1));}
        }
        let directory=tempfile::tempdir().unwrap();
        let committed=directory.path().join(name);fs::write(&committed,b"previous complete checkpoint").unwrap();
        for _ in 0..32 {
            let ready=directory.path().join("ready");let _=fs::remove_file(&ready);
            let mut child=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","storage::tests::killed_checkpoint_writer_reuses_one_scratch_slot_and_preserves_committed_state","--ignored","--nocapture"])
                .env(ENV,directory.path()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
            let until=std::time::Instant::now()+std::time::Duration::from_secs(5);
            while !ready.exists() && std::time::Instant::now()<until {std::thread::sleep(std::time::Duration::from_millis(5));}
            let was_ready=ready.exists();let _=child.kill();child.wait().unwrap();assert!(was_ready);
            assert_eq!(fs::read(&committed).unwrap(),b"previous complete checkpoint");
            let scratch:Vec<_>=directory.path().read_dir().unwrap().filter_map(std::result::Result::ok)
                .filter(|f|f.file_name().to_string_lossy().ends_with(".tmp")).collect();
            assert_eq!(scratch.len(),1);assert_eq!(scratch[0].metadata().unwrap().len(),1024*1024);
        }
        let store=FileStore::new(&committed);
        store.save_bytes(b"new complete checkpoint",||Ok(())).unwrap();
        assert_eq!(fs::read(&committed).unwrap(),b"new complete checkpoint");
        assert!(!directory.path().join(".checkpoint.json.write.tmp").exists());
    }

    #[test]
    #[cfg(unix)]
    fn scratch_aliases_are_rejected_without_modifying_their_targets() {
        use std::os::unix::fs::{symlink,PermissionsExt};
        let directory=tempfile::tempdir().unwrap();let name=std::ffi::OsStr::new("checkpoint.json");
        let target=directory.path().join("preserved");fs::write(&target,b"do not touch").unwrap();
        fs::set_permissions(&target,fs::Permissions::from_mode(0o600)).unwrap();
        let scratch=directory.path().join(".checkpoint.json.write.tmp");
        symlink(&target,&scratch).unwrap();assert!(create_temporary(directory.path(),name).is_err());fs::remove_file(&scratch).unwrap();
        fs::hard_link(&target,&scratch).unwrap();assert!(create_temporary(directory.path(),name).is_err());
        assert_eq!(fs::read(target).unwrap(),b"do not touch");
    }

    #[test]
    fn temporary_name_collision_preserves_existing_file_and_selects_owned_path() {
        let directory = tempfile::tempdir().unwrap();
        let name = std::ffi::OsStr::new("checkpoint.json");
        let existing = directory.path().join(format!(".checkpoint.json.{}.0.tmp", std::process::id()));
        fs::write(&existing, b"older writer evidence").unwrap();
        let (path, mut file) = create_temporary(directory.path(), name).unwrap();
        assert_ne!(path, existing);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        }
        let guard = RemoveOnDrop(Some(path.clone()));
        file.write_all(b"new owned temporary file").unwrap();
        assert_eq!(fs::read(&existing).unwrap(), b"older writer evidence");
        drop(file);
        drop(guard);
        assert!(!path.exists());
        assert_eq!(fs::read(&existing).unwrap(), b"older writer evidence");

        let (path, file) = create_temporary(directory.path(), name).unwrap();
        let destination = directory.path().join("committed.json");
        let mut guard = RemoveOnDrop(Some(path.clone()));
        drop(file);
        fs::rename(&path, destination).unwrap();
        guard.0 = None;
        fs::write(&path, b"later writer owns this name").unwrap();
        drop(guard);
        assert_eq!(fs::read(&path).unwrap(), b"later writer owns this name");
    }
}
