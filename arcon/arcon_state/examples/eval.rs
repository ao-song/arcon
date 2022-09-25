extern crate arcon_state;
extern crate fastrand;
extern crate rand;
extern crate zipf;

use core::time;
use std::borrow::Borrow;
use std::env;
use std::io::Write;
use std::thread;
use std::{error::Error, iter, path::Path};

use rand::distributions::Distribution;

use arcon_state::*;

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
    let num_ops = 1_000_000;
    // let num_ops = 10_000;

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

fn main() {
    let path = Path::new("/home/ao/tiered");
    let tiered = Tiered::create(&path, "test".to_string()).unwrap();

    // variables
    let entry_num = 1_000_000;
    let key_size = 80;
    let value_size = 320;

    let rng = fastrand::Rng::new();
    rng.seed(6);

    let out = Box::new(std::io::stdout());

    env::set_var("TIERED_LAYER1", "0");
    env::set_var("TIERED_LAYER2", "0");
    env::set_var("TIERED_LAYER3", "0");

    ///////////////////////////////////////// WARM UP EVAL !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // fill the tikv with data
    {
        println!("Fill in data in TiKV..");
        for i in 0..entry_num {
            let key = make_key(i, key_size);
            let value = make_value(value_size, &rng);
            let _ret = tiered.rt.block_on(async {
                tiered
                    .tikv
                    .put(key.to_owned(), key.to_owned())
                    .await
                    .unwrap()
            });
        }
    }

    // // let mut count = 0;
    // // let mut timepassed = 0;
    // // for i in 0..entry_num {
    // //     let start = std::time::Instant::now();
    // //     let key = make_key(rng.usize(0..entry_num), key_size);
    // //     let value = make_value(value_size, &rng);
    // //     tiered.get("test".to_string(), key.clone());
    // //     tiered.put("test".to_string(), key.clone(), value.clone());
    // //     let elapsed = start.elapsed();

    // //     timepassed += elapsed.as_nanos();

    // //     if count % 100 == 0 {
    // //         println!("{}", timepassed / 100);
    // //         timepassed = 0;
    // //     }

    // //     count += 1;
    // // }

    // let mut rng = rand::thread_rng();
    // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();

    // let fast_rng = fastrand::Rng::new();
    // fast_rng.seed(6);

    // let mut count = 0;
    // let mut timepassed = 0;
    // for i in 0..100_000 {
    //     let start = std::time::Instant::now();
    //     let key = make_key(zipf.sample(&mut rng), key_size);
    //     let value = make_value(value_size, &fast_rng);
    //     tiered.get("test".to_string(), key.clone());
    //     tiered.put("test".to_string(), key.clone(), value.clone());
    //     let elapsed = start.elapsed();

    //     timepassed += elapsed.as_nanos();

    //     if count % 100 == 0 {
    //         println!("{}", timepassed / 100);
    //         timepassed = 0;
    //     }

    //     count += 1;
    // }

    // // let mut count = 0;
    // // for i in 0..100_000 {
    // //     if count % 10 == 0 {
    // //         let start = std::time::Instant::now();
    // //         let key = make_key(zipf.sample(&mut rng), key_size);
    // //         let value = make_value(value_size, &fast_rng);
    // //         tiered.get("test".to_string(), key.clone());
    // //         tiered.put("test".to_string(), key.clone(), value.clone());
    // //         let elapsed = start.elapsed();
    // //         println!("{}", elapsed.as_nanos());
    // //     } else {
    // //         let key = make_key(zipf.sample(&mut rng), key_size);
    // //         let value = make_value(value_size, &fast_rng);
    // //         tiered.get("test".to_string(), key.clone());
    // //         tiered.put("test".to_string(), key.clone(), value.clone());
    // //     }

    // //     count += 1;
    // // }

    // // println!("Testing basic get..");

    // // let key = make_key(100, key_size);
    // // println!("{:?}", key);
    // // println!("{:?}", tiered.get("test".to_string(), key.clone()));
    // // println!("{:?}", tiered.activecache.borrow_mut().get(&key));

    // // println!("Testing basic put..");
    // // let key = make_key(100, key_size);
    // // let value = make_value(value_size, &rng);
    // // println!("Key is {:?}", key);
    // // println!("Value is {:?}", value);
    // // tiered.put("test".to_string(), key.clone(), value);
    // // println!("{:?}", tiered.activecache.borrow_mut().get(&key));

    // println!("Testing massive read/write..");
    // let mut c = 0;
    ///////////////////////////////////////// EVALUATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // println!("Init storage with {:?} entries", entry_num);
    // for i in 0..entry_num {
    //     let key = make_key(i, key_size);
    //     let value = make_value(value_size, &rng);
    //     tiered.put("test".to_string(), key.clone(), value.clone());
    // }

    // println!("Now measure tiered persist time!");
    // let start = std::time::Instant::now();

    // for i in 0..entry_num {
    //     let key = make_key(i, key_size);
    //     let value = make_value(value_size, &rng);
    //     tiered.put("test".to_string(), key.clone(), value.clone());
    // }
    // tiered.flush();

    // let elapsed = start.elapsed();

    // println!("Done! {:?}", elapsed.as_nanos() / (entry_num as u128));

    // let cache_size: usize = env::var("CACHE_SIZE")
    //     .unwrap_or("10_000".to_string())
    //     .parse()
    //     .unwrap_or(10_000);

    // let mut vec_batch = vec![];
    // for i in 0..cache_size {
    //     let key = make_key(i, key_size);
    //     let value = make_value(value_size, &rng);
    //     vec_batch.push((key.to_owned(), value.to_owned()));
    // }

    // let iteration = entry_num / cache_size;

    // println!("Now measure tikv batch time!");
    // let start = std::time::Instant::now();

    // for i in 0..iteration {
    //     tiered.rt.block_on(async { tiered.tikv.batch_put(vec_batch.clone()).await.unwrap() });
    // }

    // let elapsed = start.elapsed();

    // println!("Done! {:?}", elapsed.as_nanos() / (entry_num as u128));

    // println!("Random read/write to prepare the system...");
    // {
    //     for i in 0..entry_num {
    //         let key = make_key(rng.usize(0..entry_num), key_size);
    //         let value = make_value(value_size, &rng);
    //         tiered.put("test".to_string(), key.clone(), value.clone());
    //     }
    // }

    thread::sleep(time::Duration::from_millis(5000));

    println!("Flush the rocksdb memtable...");
    tiered.flush_rocks();

    thread::sleep(time::Duration::from_millis(10000));

    env::set_var("TIERED_LAYER1", "0");
    env::set_var("TIERED_LAYER2", "0");
    env::set_var("TIERED_LAYER3", "0");

    println!("Now measure on random read on tiered system...");
    let _ret = measure(out, || {
        let key = make_key(rng.usize(0..entry_num), key_size);
        tiered.get("test".to_string(), key.clone());
        Ok(())
    });

    println!("layer1 hit times: {:?}", env::var("TIERED_LAYER1").unwrap());
    println!("layer2 hit times: {:?}", env::var("TIERED_LAYER2").unwrap());
    println!("layer3 hit times: {:?}", env::var("TIERED_LAYER3").unwrap());

    println!("Now measure on random read on rocksdb...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(rng.usize(0..entry_num), key_size);
        tiered.db_mut().get_pinned(&key);
        Ok(())
    });

    println!("Now measure on random read on tikv...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(rng.usize(0..entry_num), key_size);
        tiered
            .rt
            .block_on(async { tiered.tikv.get(key.clone().to_owned()).await.unwrap() });
        Ok(())
    });

    println!("Now measure on random write on tiered system...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(rng.usize(0..entry_num), key_size);
        let value = make_value(value_size, &rng);
        tiered.put("test".to_string(), key.clone(), value);
        Ok(())
    });

    println!("Now measure on random write on rocksdb...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(rng.usize(0..entry_num), key_size);
        let value = make_value(value_size, &rng);
        tiered.db_mut().put(&key, &value);
        Ok(())
    });

    println!("Now measure on random write on tikv...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(rng.usize(0..entry_num), key_size);
        let value = make_value(value_size, &rng);
        tiered.rt.block_on(async {
            tiered
                .tikv
                .put(key.to_owned(), value.to_owned())
                .await
                .unwrap()
        });
        Ok(())
    });

    // ========================================ZIPF==================================================

    println!("=========================NOW IT IS HOTKEY!!!===========================");

    println!("Prepare the system for hotkey read/write...");
    let mut rng = rand::thread_rng();
    let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();

    let fast_rng = fastrand::Rng::new();
    fast_rng.seed(6);
    {
        for i in 0..entry_num {
            let key = make_key(zipf.sample(&mut rng), key_size);
            let value = make_value(value_size, &fast_rng);
            tiered.put("test".to_string(), key.clone(), value.clone());
        }
    }

    thread::sleep(time::Duration::from_millis(5000));

    println!("Flush the rocksdb memtable...");
    tiered.flush_rocks();

    thread::sleep(time::Duration::from_millis(10000));

    env::set_var("TIERED_LAYER1", "0");
    env::set_var("TIERED_LAYER2", "0");
    env::set_var("TIERED_LAYER3", "0");

    println!("Now measure on zipf read on tiered system...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(zipf.sample(&mut rng), key_size);
        tiered.get("test".to_string(), key.clone());
        Ok(())
    });

    println!("layer1 hit times: {:?}", env::var("TIERED_LAYER1").unwrap());
    println!("layer2 hit times: {:?}", env::var("TIERED_LAYER2").unwrap());
    println!("layer3 hit times: {:?}", env::var("TIERED_LAYER3").unwrap());

    println!("Now measure on zipf read on rocksdb...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(zipf.sample(&mut rng), key_size);
        tiered.db_mut().get_pinned(&key);
        Ok(())
    });

    println!("Now measure on zipf read on tikv...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(zipf.sample(&mut rng), key_size);
        tiered
            .rt
            .block_on(async { tiered.tikv.get(key.clone().to_owned()).await.unwrap() });
        Ok(())
    });

    println!("Now measure on zipf write on tiered system...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(zipf.sample(&mut rng), key_size);
        let value = make_value(value_size, &fast_rng);
        tiered.put("test".to_string(), key.clone(), value);
        Ok(())
    });

    println!("Now measure on zipf write on rocksdb...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(zipf.sample(&mut rng), key_size);
        let value = make_value(value_size, &fast_rng);
        tiered.db_mut().put(&key, &value);
        Ok(())
    });

    println!("Now measure on zipf write on tikv...");
    let out = Box::new(std::io::stdout());
    let _ret = measure(out, || {
        let key = make_key(zipf.sample(&mut rng), key_size);
        let value = make_value(value_size, &fast_rng);
        tiered.rt.block_on(async {
            tiered
                .tikv
                .put(key.to_owned(), value.to_owned())
                .await
                .unwrap()
        });
        Ok(())
    });

    // // tikv, rocksdb equal
    // println!("Testing tikv, rocksdb equal");
    // for i in 0..entry_num {
    //     let key = make_key(i, key_size);

    //     if let Some(ret) = tiered
    //         .rt
    //         .block_on(async { tiered.tikv.get(key.clone().to_owned()).await.unwrap() })
    //     {
    //         println!("rettttt is {:?}", ret);
    //         let ac = tiered.db_mut();
    //         if let Ok(cret) = ac.get_pinned(&key) {
    //             let cretunwrap = cret.unwrap();
    //             println!("Get value from rocks as well {:?}!", cretunwrap.to_vec());
    //             if *(cretunwrap.to_vec()) != ret.clone() {
    //                 println!("ret is {:?}", ret);
    //             }
    //         }
    //     }
    // }
}

// cargo run --example eval
