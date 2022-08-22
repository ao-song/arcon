use crate::{
    data::{Key, Metakey, Value},
    error::*,
    Aggregator, AggregatorState, Backend, Handle, MapState, Reducer, ReducerState, ValueState,
    VecState,
};

use tikv_client::{KvPair, RawClient};

use tokio::runtime::Runtime;

use lru::LruCache;

use rocksdb::{
    checkpoint::Checkpoint, ColumnFamily, ColumnFamilyDescriptor, DBPinnableSlice,
    DBWithThreadMode, MultiThreaded, Options, SliceTransform, WriteBatch, WriteOptions,
    BlockBasedOptions
};
use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    cell::UnsafeCell,
    collections::{HashSet, VecDeque},
    env, fs, io, mem,
    ops::Deref,
    path::{Path, PathBuf},
    ptr,
    sync::{Arc, Mutex},
    thread,
};

extern crate fastrand;
extern crate rand;

use core::time;
use std::io::Write;
use std::{error::Error, iter};

unsafe impl Send for Tiered {}
unsafe impl Sync for Tiered {}

type DB = DBWithThreadMode<MultiThreaded>;

pub struct Tiered {
    inner: UnsafeCell<Arc<DB>>,
    restored: bool,
    name: String,
    pub tikv: RawClient,
    pub rt: Runtime,
    pub activecache: RefCell<LruCache<Vec<u8>, Vec<u8>>>,
    cachelist: Arc<Mutex<VecDeque<LruCache<Vec<u8>, Vec<u8>>>>>,
}

// we use epochs, so WAL is useless for us
fn default_write_opts() -> WriteOptions {
    let mut res = WriteOptions::default();
    res.disable_wal(true);
    res
}

fn make_key(i: usize, key_size: usize) -> Vec<u8> {
    i.to_le_bytes()
        .iter()
        .copied()
        .cycle()
        .take(key_size)
        .collect()
}

fn make_value(value_size: usize, rng: &fastrand::Rng) -> Vec<u8> {
    iter::repeat_with(|| rng.u8(..)).take(value_size).collect()
}

fn measure(
    mut out: Box<dyn Write>,
    mut f: impl FnMut() -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    println!("Measurement started... ");
    // let num_ops = 1_000_000;
    let num_ops = 10_000;

    let start = std::time::Instant::now();

    let mut ops_done = 0usize;
    let mut hit = 0usize;
    for i in 0..num_ops {
        f()?;
        ops_done += 1;
    }

    let elapsed = start.elapsed();
    println!("Done! {:?}", elapsed);
    writeln!(
        out,
        "{},{}",
        elapsed.as_nanos() / (ops_done as u128),
        ops_done
    )?;

    Ok(())
}

impl Tiered {
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    pub fn db_mut(&self) -> &mut Arc<DB> {
        unsafe { &mut *(*self.inner.get().borrow_mut()) }
    }

    #[inline(always)]
    fn db(&self) -> &DB {
        unsafe { &(*self.inner.get()) }
    }

    // #[inline]
    // fn get_cf_handle(&self, cf_name: impl AsRef<str>) -> Result<&ColumnFamily> {
    //     let cf_name = cf_name.as_ref();
    //     self.db()
    //         .cf_handle(cf_name)
    //         .with_context(|| RocksMissingColumnFamily {
    //             cf_name: cf_name.to_string(),
    //         })
    // }

    #[inline]
    pub fn flush_rocks(&self) -> Result<(), Box<dyn Error>> {
        unsafe {
            self.db_mut().flush();
            Ok(())
        }
    }

    #[inline]
    pub fn flush(&self) -> Result<(), Box<dyn Error>> {
        let mut cache = self.activecache.borrow_mut();
        let mut list = self.cachelist.lock().unwrap();
        let mut newcache: LruCache<Vec<u8>, Vec<u8>> = LruCache::new(cache_size);
        for (kk, vv) in cache.iter() {
            newcache.put(kk.to_owned(), vv.to_owned());
        }
        list.push_back(newcache);
        mem::drop(list);

        loop {
            let mut list = self.cachelist.lock().unwrap();

            if list.is_empty() {
                mem::drop(list);
                break;
            }

            println!("list not empty!");

            mem::drop(list);
        }

        Ok(())
    }

    #[inline]
    pub fn layers_bench(&self) -> Result<(), Box<dyn Error>> {
        let rng = fastrand::Rng::new();
        rng.seed(6);
        let key_size = 80;
        let value_size = 320;
        let out = Box::new(std::io::stdout());

        println!("Now benchmark on cache layer...");
        let mut layer1 = LruCache::new(5000);
        for i in 0..5000 {
            let key = make_key(rng.usize(0..5000), key_size);
            let value = make_value(value_size, &rng);
            layer1.put(key, value);
        }

        let _ret = measure(out, || {
            let key = make_key(rng.usize(0..5000), key_size);
            layer1.get(&key);
            Ok(())
        });

        println!("Now benchmark on embedded layer...");
        let mut opts = Options::default();
        let mut block_opts = BlockBasedOptions::default();
        block_opts.disable_cache();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&block_opts);

        let savedpath = "/home/ao/bench";

        let path: PathBuf = savedpath.into();
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        let layer2 = DB::open(&opts, &path).unwrap();
        for i in 0..5000 {
            let key = make_key(rng.usize(0..5000), key_size);
            let value = make_value(value_size, &rng);
            layer2.put(&key, &value);
        }

        layer2.flush();

        let out = Box::new(std::io::stdout());

        let _ret = measure(out, || {
            let key = make_key(rng.usize(0..5000), key_size);
            layer2.get_pinned(&key);
            Ok(())
        });


        println!("Now benchmark on external layer...");
        let rt = Runtime::new().unwrap();
        let addr = env::var("TIKV_ADDR").unwrap_or("10.166.0.5:2379".to_string());
        let layer3 = rt.block_on(RawClient::new(vec![addr])).unwrap();

        for i in 0..5000 {
            let key = make_key(rng.usize(0..5000), key_size);
            let value = make_value(value_size, &rng);
            rt.block_on(async { layer3.put(key.to_owned(), value.to_owned()).await.unwrap() });
        }

        let out = Box::new(std::io::stdout());

        let _ret = measure(out, || {
            let key = make_key(rng.usize(0..5000), key_size);
            rt.block_on(async { layer3.get(key.clone().to_owned()).await.unwrap() });
            Ok(())
        });

        Ok(())

    }

    #[inline]
    pub fn get(&self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        // let cf = self.get_cf_handle(cf_name)?;
        // // Ok(self.db().get_pinned_cf(cf, key)?)
        // Ok(self.db().get_pinned(key)?)

        if let Some(value) = self.activecache.borrow_mut().get(key.as_ref()) {
            let l1_hit = env::var("TIERED_LAYER1").unwrap().parse::<u32>().unwrap() + 1;
            env::set_var("TIERED_LAYER1", l1_hit.to_string());
            Ok(Some(value.to_owned()))
        } else {
            for cache in self.cachelist.lock().unwrap().iter_mut() {
                if let Some(value) = cache.get(key.as_ref()) {
                    let l1_hit = env::var("TIERED_LAYER1").unwrap().parse::<u32>().unwrap() + 1;
                    env::set_var("TIERED_LAYER1", l1_hit.to_string());
                    return Ok(Some(value.to_owned()));
                }
            }

            if let Ok(Some(value)) = self.db().get_pinned(key.as_ref().clone()) {
                let l1_hit = env::var("TIERED_LAYER2").unwrap().parse::<u32>().unwrap() + 1;
                env::set_var("TIERED_LAYER2", l1_hit.to_string());
                return Ok(Some(value.to_vec()));
            }

            let l1_hit = env::var("TIERED_LAYER3").unwrap().parse::<u32>().unwrap() + 1;
            env::set_var("TIERED_LAYER3", l1_hit.to_string());

            Ok(self
                .rt
                .block_on(async { self.tikv.get(key.as_ref().to_owned()).await.unwrap() }))
        }
    }

    #[inline]
    pub fn put(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        // let cf = self.get_cf_handle(cf_name)?;
        // // Ok(self
        // //     .db()
        // //     .put_cf_opt(cf, key, value, &default_write_opts())?)
        // Ok(self.db().put_opt(key, value, &default_write_opts())?)

        let mut cache = self.activecache.borrow_mut();
        if cache.len() < cache.cap() {
            cache.put(key.as_ref().to_owned(), value.as_ref().to_owned());
            Ok(())
        } else {
            let mut list = self.cachelist.lock().unwrap();

            println!(
                "Active cache is full, append to immutable cache list! List len is {}",
                list.len()
            );

            let cache_size: usize = env::var("CACHE_SIZE")
                .unwrap_or("10_000".to_string())
                .parse()
                .unwrap_or(10_000);

            let mut newcache: LruCache<Vec<u8>, Vec<u8>> = LruCache::new(cache_size);
            for (kk, vv) in cache.iter() {
                newcache.put(kk.to_owned(), vv.to_owned());
            }

            list.push_back(newcache);

            mem::drop(list);

            cache.clear();
            cache.put(key.as_ref().to_owned(), value.as_ref().to_owned());
            Ok(())
        }
    }

    #[inline]
    fn remove(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<()> {
        // let cf = self.get_cf_handle(cf)?;
        // Ok(self.db().delete_cf_opt(cf, key, &default_write_opts())?)
        self.db().delete_opt(key, &default_write_opts());
        Ok(())
    }

    fn remove_prefix(&self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

        // let cf = self.get_cf_handle(cf_name)?;

        // NOTE: this only works assuming the column family is lexicographically ordered (which is
        // the default, so we don't explicitly set it, see Options::set_comparator)
        let start = prefix;
        // delete_range deletes all the entries in [start, end) range, so we can just increment the
        // least significant byte of the prefix
        let mut end = start.to_vec();
        *end.last_mut()
            .expect("unreachable, the empty case is covered a few lines above") += 1;

        let mut wb = WriteBatch::default();
        // wb.delete_range_cf(cf, start, &end);
        wb.delete_range(start, &end);

        self.db().write_opt(wb, &default_write_opts());

        Ok(())
    }

    #[inline]
    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<bool> {
        // let cf = self.get_cf_handle(cf.as_ref())?;
        // Ok(self.db().get_pinned_cf(cf, key)?.is_some())
        Ok(self.db().get_pinned(key).unwrap_or_default().is_some())
    }

    fn create_column_family(&self, cf_name: &str, opts: Options) -> Result<()> {
        if self.db().cf_handle(cf_name).is_none() {
            self.db_mut().create_cf(cf_name, &opts);
        }
        Ok(())
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

impl Backend for Tiered {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn create(path: &Path, name: String) -> Result<Self>
    where
        Self: Sized,
    {
        // Create the tokio runtime which will be used to block on the async
        // tikv operations
        let rt = Runtime::new().unwrap();

        let addr = env::var("TIKV_ADDR").unwrap_or("10.166.0.5:2379".to_string());
        let tikv = rt.block_on(RawClient::new(vec![addr])).unwrap();

        let mut opts = Options::default();
        let mut block_opts = BlockBasedOptions::default();
        block_opts.disable_cache();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&block_opts);

        let savedpath = path.clone();

        let path: PathBuf = path.into();
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }

        let cache_size: usize = env::var("CACHE_SIZE")
            .unwrap_or("10_000".to_string())
            .parse()
            .unwrap_or(10_000);
        let activecache = RefCell::new(LruCache::new(cache_size));

        let cachelist: Arc<Mutex<VecDeque<LruCache<Vec<u8>, Vec<u8>>>>> =
            Arc::new(Mutex::new(VecDeque::new()));

        let mut cl = Arc::clone(&cachelist);

        let arcdb = Arc::new(DB::open(&opts, &path).unwrap());
        let mut tdb = Arc::clone(&arcdb);

        thread::spawn(move || {
            let addr = env::var("TIKV_ADDR").unwrap_or("10.166.0.5:2379".to_string());
            println!("Trying to start a background thread!!!!!!!");
            let mut dbdb = tdb;
            println!("=================Thread started!=============");
            let t_db = dbdb;
            let t_rt = Runtime::new().unwrap();
            let t_tikv = t_rt.block_on(RawClient::new(vec![addr])).unwrap();

            loop {
                let mut cl = cl.lock().unwrap();
                let mut t_cl = cl;
                while !t_cl.is_empty() {
                    println!("DEBUG: start to dump data to db!");
                    if let Some(f) = t_cl.pop_front() {
                        let mut batch = WriteBatch::default();
                        let mut vec_batch = vec![];
                        for (t_k, t_v) in f.iter() {
                            // let tkptr = t_k.as_ptr();
                            // let tvptr = t_v.as_ptr();
                            batch.put(t_k, t_v);
                            vec_batch.push((t_k.to_owned(), t_v.to_owned()));
                        }
                        t_db.write(batch);
                        t_rt.block_on(async { t_tikv.batch_put(vec_batch).await.unwrap() })
                    }
                }
                mem::drop(t_cl);
            }
        });

        // let column_families: HashSet<String> = match DB::list_cf(&opts, &path) {
        //     Ok(cfs) => cfs.into_iter().filter(|n| n != "default").collect(),
        //     // TODO: possibly platform-dependant error message check
        //     Err(e) if e.to_string().contains("No such file or directory") => HashSet::new(),
        //     Err(e) => return Err(e.into()),
        // };

        // let cfds = if !column_families.is_empty() {
        //     column_families
        //         .into_iter()
        //         .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        //         .collect()
        // } else {
        //     vec![ColumnFamilyDescriptor::new("default", Options::default())]
        // };

        // let mut cl = Arc::clone(&cachelist);
        // let mut opts = Options::default();
        // opts.create_if_missing(true);

        let path: PathBuf = savedpath.into();
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        Ok(Tiered {
            // inner: UnsafeCell::new(DB::open_cf_descriptors(&opts, &path, cfds)?),
            inner: UnsafeCell::new(arcdb),
            restored: false,
            name,
            tikv,
            rt,
            activecache,
            cachelist: Arc::clone(&cachelist),
        })
    }

    fn restore(live_path: &Path, checkpoint_path: &Path, name: String) -> Result<Self>
    where
        Self: Sized,
    {
        fs::create_dir_all(live_path)?;

        // ensure!(
        //     fs::read_dir(live_path)?.next().is_none(),
        //     RocksRestoreDirNotEmpty { dir: &(*live_path) }
        // );

        let mut target_path: PathBuf = live_path.into();
        target_path.push("__DUMMY"); // the file name is replaced inside the loop below
        for entry in fs::read_dir(checkpoint_path)? {
            let entry = entry?;

            assert!(entry
                .file_type()
                .expect("Cannot read entry metadata")
                .is_file());

            let source_path = entry.path();
            // replaces the __DUMMY from above the loop
            target_path.set_file_name(
                source_path
                    .file_name()
                    .expect("directory entry with no name?"),
            );

            fs::copy(&source_path, &target_path)?;
        }

        Tiered::create(live_path, name).map(|mut r| {
            r.restored = true;
            r
        })
    }

    fn was_restored(&self) -> bool {
        self.restored
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        unimplemented!();
        // let db = self.db();
        // db.flush();

        // let checkpointer = Checkpoint::new(db);

        // if checkpoint_path.exists() {
        //     // TODO: add a warning log here
        //     // warn!(logger, "Checkpoint path {:?} exists, deleting");
        //     fs::remove_dir_all(checkpoint_path)?
        // }

        // // checkpointer.create_checkpoint(checkpoint_path);
        // Ok(())
    }

    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        handle.registered = true;
        let opts = common_options::<IK, N>();
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        handle.registered = true;
        let opts = common_options::<IK, N>();
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        opts.set_merge_operator_associative("vec_merge", vec_ops::vec_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        let reducer_merge = reducer_ops::make_reducer_merge(handle.extra_data.clone());
        opts.set_merge_operator_associative("reducer_merge", reducer_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        let aggregator_merge = aggregator_ops::make_aggregator_merge(handle.extra_data.clone());
        opts.set_merge_operator_associative("aggregator_merge", aggregator_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
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

    pub struct TestDb {
        tiered: Arc<Tiered>,
        dir: TempDir,
    }

    impl TestDb {
        #[allow(clippy::new_without_default)]
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("tiered");
            fs::create_dir(&dir_path).unwrap();
            let tiered = Tiered::create(&dir_path, "testDB".to_string()).unwrap();
            TestDb {
                tiered: Arc::new(tiered),
                dir,
            }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir: PathBuf = self.dir.path().into();
            checkpoint_dir.push("checkpoint");
            self.tiered.checkpoint(&checkpoint_dir).unwrap();
            checkpoint_dir
        }

        pub fn from_checkpoint(checkpoint_dir: &str) -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("tiered");
            let tiered =
                Tiered::restore(&dir_path, checkpoint_dir.as_ref(), "testDB".to_string()).unwrap();
            TestDb {
                tiered: Arc::new(tiered),
                dir,
            }
        }
    }

    impl Deref for TestDb {
        type Target = Arc<Tiered>;

        fn deref(&self) -> &Self::Target {
            &self.tiered
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.tiered
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

        let db = Tiered::create(dir_path, "testDB".to_string()).unwrap();

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

        let db_from_checkpoint = Tiered::restore(
            &restore_dir_path,
            &checkpoints_dir_path,
            "testDB".to_string(),
        )
        .expect("Could not open checkpointed db");

        // assert_eq!(
        //     new_value,
        //     db.get(column_family, key)
        //         .expect("Could not get from the original db")
        //         .unwrap()
        //         .as_ref()
        // );
        // assert_eq!(
        //     initial_value,
        //     db_from_checkpoint
        //         .get(column_family, key)
        //         .expect("Could not get from the checkpoint")
        //         .unwrap()
        //         .as_ref()
        // );
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
