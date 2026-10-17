//! Runs openraft's storage conformance suite against the RocksDB store.

use fabercp::raft::store::{self, LogStore, StateMachineStore};
use fabercp::raft::types::{StorageError, TypeConfig};
use openraft::testing::{StoreBuilder, Suite};
use tempfile::TempDir;

struct RocksBuilder;

impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, TempDir> for RocksBuilder {
    async fn build(&self) -> Result<(TempDir, LogStore, StateMachineStore), StorageError> {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = store::open(dir.path())?;
        // The guard (TempDir) is dropped after the stores, so on Windows the
        // database files are closed before the directory is removed.
        Ok((dir, s.log_store, s.state_machine))
    }
}

// A plain #[test]: the suite creates its own tokio runtime per case.
#[test]
fn rocksdb_store_passes_openraft_suite() {
    Suite::test_all(RocksBuilder).expect("openraft storage suite");
}
