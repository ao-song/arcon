extern crate arcon_state;

use arcon_state::*;
use std::{
    env,
    error::Error,
    iter,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
    path::PathBuf,
};
use tempfile::tempdir_in;
use std::io::Write;
use std::ops::Deref;


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

    // generate data in db
    {
        for i in 0..entry_num {
            let key = make_key(i: usize, key_size: usize);
            let value = make_value(value_size: usize, &rng: &fastrand::Rng);
            map.fast_insert(key, value)?;
        }
    }
}