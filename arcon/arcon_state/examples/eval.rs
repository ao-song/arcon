extern crate arcon_state;
extern crate fastrand;
extern crate rand;
extern crate zipf;

use arcon_state::*;
use std::io::Write;
use std::{
    error::Error,
    iter,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use rand::distributions::Distribution;

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
    mut f: impl FnMut() -> Result<bool, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    println!("Measurement started... ");
    // let num_ops = 1_000_000;
    let num_ops = 10_000;

    let start = std::time::Instant::now();

    let mut ops_done = 0usize;
    let mut hit = 0usize;
    for i in 0..num_ops {
        match f() {
            Ok(h) => {
                if h {
                    hit += 1;
                }
                ops_done += 1;
            },
            Err(_) => {},
        }
        // let h = f()?;
        // ops_done += 1;
        // if h {
        //     hit += 1;
        // }
    }

    let elapsed = start.elapsed();
    println!("Done! {:?}", elapsed);
    println!("Hit rate! {:?}", hit as f64 / ops_done as f64);
    writeln!(
        out,
        "{},{}",
        elapsed.as_nanos() / (ops_done as u128),
        ops_done
    )?;

    Ok(())
}

fn main() {
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

    let tikv = TestDb::new();
    let mut eval_map: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
        Handle::map("map").with_item_key(0).with_namespace(0);
    tikv.register_map_handle(&mut eval_map);
    let map = eval_map.activate(tikv.clone());

    // let entry_num = 1_000_000usize;
    let entry_num = 100_000usize;
    let key_size = 8;
    let value_size = 32;

    let rng = fastrand::Rng::new();
    rng.seed(6);

    let out = Box::new(std::io::stdout());

    // // generate data in db
    // {
    //     for i in 0..entry_num {
    //         let key = make_key(i, key_size);
    //         let value = make_value(value_size, &rng);
    //         let _ret = map.fast_insert(key, value);
    //     }
    // }

    println!("Now measure random read on tikv...");
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        map.get(&key)?;
        Ok(true)
    });

    println!("Now measure random read on hashmap...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let ret = map.backend.hashmap_get(&map.inner, &key)?;
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    println!("Now measure random read on lru...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let ret = map.backend.lru_get(&map.inner, &key)?;
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    println!("Now measure random read on tiny lfu...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let ret = map.backend.tiny_lfu_get(&map.inner, &key)?;
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    let mut rng = rand::thread_rng();
    let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();

    println!("Now measure zipf read on hashmap...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    let _ret = measure(out, || {
        // let mut rng = rand::thread_rng();
        // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();
        let key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        let ret = map.backend.hashmap_get(&map.inner, &key)?;
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    println!("Now measure zipf read on lru...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    let _ret = measure(out, || {
        // let mut rng = rand::thread_rng();
        // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();
        let key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        let ret = map.backend.lru_get(&map.inner, &key)?;
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    println!("Now measure zipf read on tiny lfu...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    let _ret = measure(out, || {
        // let mut rng = rand::thread_rng();
        // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();
        let key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        let ret = map.backend.tiny_lfu_get(&map.inner, &key)?;
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    // cargo run --example eval
}
