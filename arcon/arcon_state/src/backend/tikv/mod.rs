use crate::{
    data::{Key, Metakey, Value},
    error::*,
    Aggregator, AggregatorState, Backend, Handle, MapState, Reducer, ReducerState, ValueState,
    VecState,
};

use tikv_client::{RawClient, TransactionClient, Error};

use tokio::runtime::Runtime;

use std::{
    cell::UnsafeCell,
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct Tikv {
    // txn_client: TransactionClient,
    raw_client: RawClient,
    restored: bool,
    name: String,
}

// Create the tokio runtime which will be used to block on the async
// tikv operations
let rt  = Runtime::new().unwrap();


// What is the Handle? What info the meta key contains here?
// How this state be used in arcon maybe a brief overview of arcon will help?
// !!! use just raw tikv client as it support cf?
// create with path but for tikv it should be a vector of IP addresses?

impl Tikv {
    #[inline]
    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<DBPinnableSlice>> {
        let cf = self.get_cf_handle(cf_name)?;
        Ok(self.db().get_pinned_cf(cf, key)?)
    }

    #[inline]
    fn put(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let cf = self.get_cf_handle(cf_name)?;
        Ok(self
            .db()
            .put_cf_opt(cf, key, value, &default_write_opts())?)
    }

    #[inline]
    fn remove(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<()> {
        let cf = self.get_cf_handle(cf)?;
        Ok(self.db().delete_cf_opt(cf, key, &default_write_opts())?)
    }

    fn remove_prefix(&self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

        let cf = self.get_cf_handle(cf_name)?;

        // NOTE: this only works assuming the column family is lexicographically ordered (which is
        // the default, so we don't explicitly set it, see Options::set_comparator)
        let start = prefix;
        // delete_range deletes all the entries in [start, end) range, so we can just increment the
        // least significant byte of the prefix
        let mut end = start.to_vec();
        *end.last_mut()
            .expect("unreachable, the empty case is covered a few lines above") += 1;

        let mut wb = WriteBatch::default();
        wb.delete_range_cf(cf, start, &end);

        self.db().write_opt(wb, &default_write_opts())?;

        Ok(())
    }

    #[inline]
    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<bool> {
        let cf = self.get_cf_handle(cf.as_ref())?;
        Ok(self.db().get_pinned_cf(cf, key)?.is_some())
    }
}

fn common_options<IK, N>() -> Options
where
    IK: Metakey,
    N: Metakey,
{
    let prefix_size = IK::SIZE + N::SIZE;

    let mut opts = Options::default();
    // for map state to work properly, but useful for all the states, so the bloom filters get
    // populated
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_size as usize));

    opts
}

impl Backend for Tikv {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn create(path: &Path, name: String) -> Result<Self>
    where
        Self: Sized,
    {
        // For Tikv it is IP addresses here, use path string store the IP
        let client = rt.block_on(
            RawClient::new(vec![path.to_str().unwrap()]))?;

        Ok(Tikv {
            client,
            restored: false,
            name,
        })

    fn restore(live_path: &Path, checkpoint_path: &Path, name: String) -> Result<Self>
    where
        Self: Sized,
    {
        // This method is ignored for TiKV
        Ok(self.create(live_path, name))
    }

    fn was_restored(&self) -> bool {
        // This method is ignored for TiKV
        self.restored
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {       
        // This method is ignored for TiKV 
        Ok(())
    }

    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        handle.registered = true;
    }
}

mod aggregator_ops;
mod map_ops;
mod reducer_ops;
mod value_ops;
mod vec_ops;

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::{
        ops::{Deref, DerefMut},
        sync::Arc,
    };
    use tempfile::TempDir;

    #[derive(Debug)]
    pub struct TestDb {
        rocks: Arc<Rocks>,
        dir: TempDir,
    }

    impl TestDb {
        #[allow(clippy::new_without_default)]
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            fs::create_dir(&dir_path).unwrap();
            let rocks = Rocks::create(&dir_path, "testDB".to_string()).unwrap();
            TestDb {
                rocks: Arc::new(rocks),
                dir,
            }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir: PathBuf = self.dir.path().into();
            checkpoint_dir.push("checkpoint");
            self.rocks.checkpoint(&checkpoint_dir).unwrap();
            checkpoint_dir
        }

        pub fn from_checkpoint(checkpoint_dir: &str) -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            let rocks =
                Rocks::restore(&dir_path, checkpoint_dir.as_ref(), "testDB".to_string()).unwrap();
            TestDb {
                rocks: Arc::new(rocks),
                dir,
            }
        }
    }

    impl Deref for TestDb {
        type Target = Arc<Rocks>;

        fn deref(&self) -> &Self::Target {
            &self.rocks
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.rocks
        }
    }

    #[test]
    fn simple_rocksdb_test() {
        let db = TestDb::new();

        let key = "key";
        let value = "test";
        let column_family = "default";

        db.put(column_family, key.as_bytes(), value.as_bytes())
            .expect("put");

        {
            let v = db.get(column_family, key.as_bytes()).unwrap().unwrap();
            assert_eq!(value, String::from_utf8_lossy(&v));
        }

        db.remove(column_family, key.as_bytes()).expect("remove");
        let v = db.get(column_family, key.as_bytes()).unwrap();
        assert!(v.is_none());
    }

    #[test]
    fn checkpoint_rocksdb_raw_test() {
        let tmp_dir = TempDir::new().unwrap();
        let checkpoints_dir = TempDir::new().unwrap();
        let restore_dir = TempDir::new().unwrap();

        let dir_path = tmp_dir.path();

        let mut checkpoints_dir_path = checkpoints_dir.path().to_path_buf();
        checkpoints_dir_path.push("chkp0");

        let mut restore_dir_path = restore_dir.path().to_path_buf();
        restore_dir_path.push("chkp0");

        let db = Rocks::create(dir_path, "testDB".to_string()).unwrap();

        let key: &[u8] = b"key";
        let initial_value: &[u8] = b"value";
        let new_value: &[u8] = b"new value";
        let column_family = "default";

        db.put(column_family, key, initial_value)
            .expect("put failed");
        db.checkpoint(&checkpoints_dir_path)
            .expect("checkpoint failed");
        db.put(column_family, key, new_value)
            .expect("second put failed");

        let db_from_checkpoint = Rocks::restore(
            &restore_dir_path,
            &checkpoints_dir_path,
            "testDB".to_string(),
        )
        .expect("Could not open checkpointed db");

        assert_eq!(
            new_value,
            db.get(column_family, key)
                .expect("Could not get from the original db")
                .unwrap()
                .as_ref()
        );
        assert_eq!(
            initial_value,
            db_from_checkpoint
                .get(column_family, key)
                .expect("Could not get from the checkpoint")
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn checkpoint_restore_state_test() {
        let mut original_test = TestDb::new();
        let mut a_handle = Handle::value("a");
        original_test.register_value_handle(&mut a_handle);

        let checkpoint_dir = {
            let mut a = a_handle.activate(original_test.clone());

            a.set(420).unwrap();

            let checkpoint_dir = original_test.checkpoint();
            assert_eq!(a.get().unwrap().unwrap(), 420);
            a.set(69).unwrap();
            assert_eq!(a.get().unwrap().unwrap(), 69);
            checkpoint_dir
        };

        let restored = TestDb::from_checkpoint(&checkpoint_dir.to_string_lossy());

        {
            let mut a_handle = Handle::value("a");
            restored.register_value_handle(&mut a_handle);
            let mut a_restored = a_handle.activate(restored.clone());
            // TODO: serialize value state metadata (type names, serialization, etc.) into rocksdb, so
            //   that type mismatches are caught early. Right now it would be possible to, let's say,
            //   store an integer, and then read a float from the restored state backend
            assert_eq!(a_restored.get().unwrap().unwrap(), 420);

            a_restored.set(1337).unwrap();
            assert_eq!(a_restored.get().unwrap().unwrap(), 1337);
        }
    }

    common_state_tests!(TestDb::new());
}
