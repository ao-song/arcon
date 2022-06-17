extern crate arcon_state;
extern crate fastrand;
extern crate rand;
extern crate zipf;

use core::time;
use std::borrow::Borrow;
use std::io::Write;
use std::thread;
use std::{error::Error, iter, path::Path};

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
            }
            Err(_) => {}
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
    let path = Path::new("/home/ao/tiered");
    let tiered = Tiered::create(&path, "test".to_string()).unwrap();

    // variables
    let entry_num = 100_000;
    let key_size = 8;
    let value_size = 32;

    let rng = fastrand::Rng::new();
    rng.seed(6);

    // // fill the tikv with data
    // {
    //     println!("Fill in data..");
    //     for i in 0..entry_num {
    //         let key = make_key(i, key_size);
    //         let value = make_value(value_size, &rng);
    //         let _ret = tiered.rt.block_on(async {
    //             tiered
    //                 .tikv
    //                 .put(key.to_owned(), key.to_owned())
    //                 .await
    //                 .unwrap()
    //         });
    //     }
    // }

    // println!("Testing basic get..");

    // let key = make_key(100, key_size);
    // println!("{:?}", key);
    // println!("{:?}", tiered.get("test".to_string(), key.clone()));
    // println!("{:?}", tiered.activecache.borrow_mut().get(&key));

    // println!("Testing basic put..");
    // let key = make_key(100, key_size);
    // let value = make_value(value_size, &rng);
    // println!("Key is {:?}", key);
    // println!("Value is {:?}", value);
    // tiered.put("test".to_string(), key.clone(), value);
    // println!("{:?}", tiered.activecache.borrow_mut().get(&key));

    println!("Testing massive read/write..");
    let mut c = 0;
    for i in 0..entry_num {
        let key = make_key(i, key_size);
        let value = make_value(value_size, &rng);
        tiered.put("test".to_string(), key.clone(), value.clone());
        c += 1;
        println!("{}", c);

        if let Ok(ret) = tiered.get("test".to_string(), key.clone()) {
            let mut ac = tiered.activecache.borrow_mut();
            let cret = ac.get(&key);
            if *(cret.unwrap()) != ret.clone().unwrap() {
                println!("cret is {:?}, ret is {:?}", cret, ret);
            }
        }
    }

    // tikv, rocksdb equal
    for i in 0..entry_num {
        let key = make_key(i, key_size);

        if let Some(ret) = tiered
            .rt
            .block_on(async { tiered.tikv.get(key.clone().to_owned()).await.unwrap() })
        {
            let mut ac = tiered.db_mut();
            if let Ok(cret) = ac.get_pinned(&key) {
                if *(cret.unwrap().to_vec()) != ret.clone() {
                    println!("ret is {:?}", ret);
                }
            }
        }
    }
}
