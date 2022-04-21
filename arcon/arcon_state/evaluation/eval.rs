extern crate arcon_state;

use arcon_state::*;
use std::io::Write;
use std::ops::Deref;
use std::{
    env,
    error::Error,
    iter,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};
use tempfile::tempdir_in;

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

fn measure<B: Backend>(
    mut out: Box<dyn Write>,
    mut f: impl FnMut() -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    println!("Measurement started... ");
    let num_ops = 1_000_000;

    let start = std::time::Instant::now();

    let mut ops_done = 0usize;
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
    let tikv_path = Path::new("127.0.0.1:2379");
    let tikv = Arc::new(Tikv::create(&tikv_path, "testDB".to_string()).unwrap());
    let eval_map = Handle::map("map").with_item_key(0).with_namespace(0);
    tikv.register_map_handle(&mut eval_map);
    let map = eval_map.activate(tikv.clone());

    let entry_num = 1_000_000usize;
    let key_size = 8;
    let value_size = 32;

    let rng = fastrand::Rng::new();
    rng.seed(6);

    let mut out = Box::new(std::io::stdout());

    // generate data in db
    {
        for i in 0..entry_num {
            let key = make_key(i, key_size);
            let value = make_value(value_size, &rng);
            map.fast_insert(key, value)?;
        }
    }

    measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        map.get(&key)?;
        Ok(())
    })?
}
