use crate::{
    data::{Key, Metakey, Value},
    error::*,
    Aggregator, AggregatorState, Backend, Handle, MapState, Reducer, ReducerState, ValueState,
    VecState,
};

use tikv_client::{ColumnFamily, RawClient, KvPair};

use tokio::runtime::Runtime;

use std::{
    cell::RefCell,
    convert::TryFrom,
    path::{Path, PathBuf},
    rc::Rc,
};

use std::collections::HashMap;

use lru::LruCache;
use cascara::Cache;

pub struct CacheBundle {
    size: u32,
    hash: RefCell<HashMap<Vec<u8>, Vec<u8>>>,
    lru: RefCell<LruCache<Vec<u8>, Vec<u8>>>,
    tinylfu: RefCell<Cache<Vec<u8>, Vec<u8>>>,
}

pub struct Tikv {
    client: RawClient,
    restored: bool,
    name: String,
    rt: Runtime,
    cache: CacheBundle,
}

impl Tikv {
    #[inline]
    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<tikv_client::Value>> {
        // let cf = ColumnFamily::try_from(cf_name.as_ref()).unwrap();
        // let client_with_cf = self.client.with_cf(cf);

        Ok(self
            .rt
            .block_on(async { self.client.get(key.as_ref().to_owned()).await.unwrap() }))
    }

    #[inline]
    fn put(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        // let cf = ColumnFamily::try_from(cf_name.as_ref()).unwrap();
        // let client_with_cf = self.client.with_cf(cf);

        Ok(self.rt.block_on(async {
            self.client
                .put(key.as_ref().to_owned(), value.as_ref().to_owned())
                .await
                .unwrap()
        }))
    }

    #[inline]
    fn batch_put(
        &self,
        cf_name: impl AsRef<str>,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>
    ) -> Result<()>
    {
        // let cf = ColumnFamily::try_from(cf_name.as_ref()).unwrap();
        // let client_with_cf = self.client.with_cf(cf);

        Ok(self.rt.block_on(async {
            self.client
                .batch_put(pairs)
                .await
                .unwrap()
        }))
    }

    #[inline]
    fn remove(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<()> {
        // let cf = ColumnFamily::try_from(cf.as_ref()).unwrap();
        // let client_with_cf = self.client.with_cf(cf);

        Ok(self
            .rt
            .block_on(async { self.client.delete(key.as_ref().to_owned()).await.unwrap() }))
    }

    fn remove_prefix(&self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> Result<()> {
        unimplemented!();
    }

    #[inline]
    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<bool> {
        // let cf = ColumnFamily::try_from(cf.as_ref()).unwrap();
        // let client_with_cf = self.client.with_cf(cf);

        Ok(self
            .rt
            .block_on(async { self.client.get(key.as_ref().to_owned()).await.unwrap() })
            .is_some())
    }
}

impl Backend for Tikv {
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

        // For Tikv it is IP addresses here, use path string store the IP
        print!("{}", path.to_str().unwrap());
        let client = rt
            .block_on(RawClient::new(vec![path.to_str().unwrap()]))
            .unwrap();

        let cache_size = 500_000;

        let cb = CacheBundle {
            hash: RefCell::new(HashMap::new()),
            lru: RefCell::new(LruCache::new(cache_size)),
            tinylfu: RefCell::new(Cache::new(cache_size)),
            size: cache_size as u32,
        };

        Ok(Tikv {
            client,
            restored: false,
            name,
            rt,
            cache: cb,
        })
    }

    fn restore(live_path: &Path, checkpoint_path: &Path, name: String) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn was_restored(&self) -> bool {
        // This method is ignored for TiKV
        self.restored
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        unimplemented!();
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
pub mod cache_ops;
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

    pub struct TestDb {
        tikv: Arc<Tikv>,
    }

    impl TestDb {
        #[allow(clippy::new_without_default)]
        pub fn new() -> TestDb {
            let dir_path = Path::new("127.0.0.1:2379");
            let tikv = Tikv::create(&dir_path, "testDB".to_string()).unwrap();
            TestDb {
                tikv: Arc::new(tikv),
            }
        }
    }

    impl Deref for TestDb {
        type Target = Arc<Tikv>;

        fn deref(&self) -> &Self::Target {
            &self.tikv
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.tikv
        }
    }

    #[test]
    fn simple_tikv_test() {
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

    common_state_tests!(TestDb::new());
}
