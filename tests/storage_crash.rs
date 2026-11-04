use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, U256};
use barterlend_state::{
    *,
    abi::{ABI_FINGERPRINT, PROTOCOL_VERSION},
    checkpoint::ReconciliationCursor,
    risk::UNIT,
    storage::FileStore,
};

const WRITER_PATH: &str = "BARTERLEND_STATE_CRASH_WRITER_PATH";
#[cfg(unix)]
const NONREGULAR_PATH: &str = "BARTERLEND_STATE_NONREGULAR_PATH";
#[cfg(unix)]
const NONREGULAR_OPERATION: &str = "BARTERLEND_STATE_NONREGULAR_OPERATION";

fn empty_checkpoint(epoch: u64, market_count: u64) -> Checkpoint {
    let pool = Address::repeat_byte(1);
    let snapshot = StateSnapshot {
        identity: ChainIdentity { chain_id: 143, genesis_hash: B256::repeat_byte(2) },
        spec: PoolSpec {
            address: pool,
            deployment_block: 0,
            expected_version: PROTOCOL_VERSION,
            deployment_block_hash: Some(B256::repeat_byte(2)),
        },
        pool: PoolDescriptor {
            address: pool,
            version: PROTOCOL_VERSION,
            asset: TokenDescriptor { address: Address::repeat_byte(3), decimals: 6 },
            collateral: TokenDescriptor { address: Address::repeat_byte(4), decimals: 18 },
            oracle: Address::repeat_byte(5),
            code_hash: B256::repeat_byte(6),
        },
        abi_fingerprint: ABI_FINGERPRINT,
        epoch,
        as_of: BlockRef {
            number: 100,
            hash: B256::repeat_byte(7),
            parent_hash: B256::repeat_byte(8),
            timestamp: 1_000,
            commitment: Commitment::Finalized,
        },
        globals: PoolRiskConfig {
            liquidation_ltv: U256::from(UNIT * 8 / 10),
            stale_window: 60,
            min_debt_remaining: 10,
        },
        markets: (0..market_count).map(|market| (market, MarketRiskConfig {
            grace_period: 10,
            discount_min: U256::from(UNIT / 100),
            discount_max: U256::from(UNIT / 5),
        })).collect(),
        oracle: OracleStatus::Unavailable(OracleFailure::Rpc("offline test fixture".into())),
        positions: BTreeMap::new(),
    };
    Checkpoint::new(snapshot, ReconciliationCursor::default()).unwrap()
}

#[test]
#[cfg(unix)]
fn storage_nonregular_path_process() {
    let Some(path) = std::env::var_os(NONREGULAR_PATH) else { return; };
    let store = FileStore::new(path);
    let result = match std::env::var(NONREGULAR_OPERATION).unwrap().as_str() {
        "load" => store.load(None, None).map(|_| ()),
        "save" => store.save(&empty_checkpoint(1, 8)),
        _ => panic!("unknown nonregular storage operation"),
    };
    assert!(matches!(result, Err(Error::Checkpoint(_))), "nonregular path was not rejected: {result:?}");
}

#[test]
#[cfg(unix)]
fn special_checkpoint_and_lock_paths_are_rejected_without_blocking() {
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = empty_checkpoint(1, 8);
    let directory_store = FileStore::new(directory.path());
    assert!(matches!(directory_store.load(None, None), Err(Error::Checkpoint(_))));
    assert!(matches!(directory_store.save(&checkpoint), Err(Error::Checkpoint(_))));
    assert!(matches!(FileStore::new("/dev/null").load(None, None), Err(Error::Checkpoint(_))));
    for operation in ["load", "save"] {
        let path = directory.path().join(format!("fifo-{operation}.json"));
        assert!(Command::new("mkfifo").arg(&path).status().unwrap().success());
        check_nonregular_child(&path, operation);
    }
    let path = directory.path().join("lock-fifo.json");
    FileStore::new(&path).save(&checkpoint).unwrap();
    let lock_path = directory.path().join("lock-fifo.json.lock");
    fs::remove_file(&lock_path).unwrap();
    assert!(Command::new("mkfifo").arg(&lock_path).status().unwrap().success());
    check_nonregular_child(&path, "save");
    assert_eq!(FileStore::new(&path).load(None, None).unwrap(), Some(checkpoint));
}

#[cfg(unix)]
fn check_nonregular_child(path: &Path, operation: &str) {
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "storage_nonregular_path_process", "--nocapture", "--test-threads=1"])
        .env(NONREGULAR_PATH, path)
        .env(NONREGULAR_OPERATION, operation)
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().unwrap();
    let mut process = WriterProcess(Some(child));
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = process.0.as_mut().unwrap().try_wait().unwrap() {
            process.0 = None;
            assert!(status.success(), "nonregular storage helper failed");
            break;
        }
        assert!(Instant::now() < deadline, "FileStore blocked on a special filesystem path");
        thread::sleep(Duration::from_millis(1));
    }
}

/// The same test executable is a real writer in the child process. No storage hooks
/// or synthetic lock ownership are used; the parent stops actual FileStore.save calls.
#[test]
fn storage_crash_writer_process() {
    let Some(path) = std::env::var_os(WRITER_PATH) else { return; };
    let store = FileStore::new(path);
    let mut checkpoint = store.load(None, None).unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        checkpoint.snapshot.epoch = checkpoint.snapshot.epoch.checked_add(1).unwrap();
        checkpoint = Checkpoint::new(checkpoint.snapshot, checkpoint.reconciliation).unwrap();
        store.save(&checkpoint).unwrap();
    }
    panic!("crash-test writer was not stopped by its parent within 30 seconds");
}

struct WriterProcess(Option<Child>);

impl WriterProcess {
    fn spawn(path: &Path) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "storage_crash_writer_process", "--nocapture", "--test-threads=1"])
            .env(WRITER_PATH, path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self(Some(child))
    }

    fn id(&self) -> u32 { self.0.as_ref().unwrap().id() }

    fn assert_running(&mut self) {
        assert!(self.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "checkpoint writer exited before the requested crash point");
    }

    fn kill_and_reap(&mut self) -> ExitStatus {
        let child = self.0.as_mut().unwrap();
        child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(child.try_wait().unwrap(), Some(status), "writer was not reaped");
                self.0 = None;
                return status;
            }
            assert!(Instant::now() < deadline, "killed checkpoint writer did not exit");
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for WriterProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CrashPoint {
    WriterLockMetadata,
    TemporaryFile,
    CommittedWrite,
}

fn writer_temp_exists(directory: &Path) -> bool {
    directory.join(".checkpoint.json.write.tmp").exists()
}

#[test]
fn real_process_crashes_leave_a_valid_checkpoint_and_release_writer_lock() {
    for crash_point in [CrashPoint::WriterLockMetadata, CrashPoint::TemporaryFile, CrashPoint::CommittedWrite] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("checkpoint.json");
        let store = FileStore::new(&path);
        // A larger empty-position snapshot gives the real tmp write a measurable
        // observation window without artificial delays inside the implementation.
        let market_count = if matches!(crash_point, CrashPoint::TemporaryFile) { 16_384 } else { 8 };
        let seed = empty_checkpoint(1, market_count);
        store.save(&seed).unwrap();
        let lock_path = directory.path().join("checkpoint.json.lock");
        let mut writer = WriterProcess::spawn(&path);
        let child_id = writer.id();
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            writer.assert_running();
            let observed = match crash_point {
                CrashPoint::WriterLockMetadata => fs::read_to_string(&lock_path)
                    .is_ok_and(|owner| owner.trim() == child_id.to_string()),
                CrashPoint::TemporaryFile => writer_temp_exists(directory.path()),
                CrashPoint::CommittedWrite => store.load(None, None).unwrap().unwrap().snapshot.epoch > 1,
            };
            if observed { break; }
            assert!(Instant::now() < deadline, "did not observe real writer activity at {crash_point:?}");
            thread::sleep(Duration::from_micros(100));
        }
        let status = writer.kill_and_reap();
        assert!(!status.success(), "writer was not terminated during its save loop");

        // No tmp or persistent lock file is removed before these checks. load must
        // accept exactly the old snapshot or one fully committed later epoch.
        let recovered = store.load(Some(&seed.snapshot.identity), Some(&seed.snapshot.spec)).unwrap().unwrap();
        assert!(recovered.snapshot.epoch >= seed.snapshot.epoch);
        assert_eq!(recovered.reconciliation, seed.reconciliation);
        let mut expected_snapshot = seed.snapshot.clone();
        expected_snapshot.epoch = recovered.snapshot.epoch;
        assert_eq!(recovered.snapshot, expected_snapshot);
        recovered.validate(None, None).unwrap();

        // Advisory ownership must disappear with the process even though the
        // stable lock inode remains. A new writer proceeds without stale-lock cleanup.
        let mut next_snapshot = recovered.snapshot.clone();
        next_snapshot.epoch = next_snapshot.epoch.checked_add(1).unwrap();
        let next = Checkpoint::new(next_snapshot, recovered.reconciliation).unwrap();
        store.save(&next).unwrap();
        assert_eq!(store.load(None, None).unwrap(), Some(next));
        assert!(lock_path.exists(), "the stable lock inode must not be unlinked");
        // The child has already been reaped; TempDir cleanup now owns all leftovers.
        drop(directory);
    }
}
