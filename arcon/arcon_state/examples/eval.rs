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
use charts::{Chart, ScaleLinear, MarkerType, PointLabelPosition, LineSeriesView, Color};

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

fn zipf_plot() {
    let mut rng = rand::thread_rng();
    let mut zipf = zipf::ZipfDistribution::new(99, 1.0).unwrap();

    let mut line = vec![0; 100];
    for _i in 0..1000 {
        let n = zipf.sample(&mut rng);
        line[n] = line[n] + 1;
    }

    // Define chart related sizes.
    let width = 800;
    let height = 600;
    let (top, right, bottom, left) = (90, 40, 50, 60);

    // Create a band scale that will interpolate values in [0, 200] to values in the
    // [0, availableWidth] range (the width of the chart without the margins).
    let x = ScaleLinear::new()
        .set_domain(vec![0_f32, 100_f32])
        .set_range(vec![0, width - left - right]);

    // Create a linear scale that will interpolate values in [0, 100] range to corresponding
    // values in [availableHeight, 0] range (the height of the chart without the margins).
    // The [availableHeight, 0] range is inverted because SVGs coordinate system's origin is
    // in top left corner, while chart's origin is in bottom left corner, hence we need to invert
    // the range on Y axis for the chart to display as though its origin is at bottom left.
    let y = ScaleLinear::new()
        .set_domain(vec![0_f32, 300_f32])
        .set_range(vec![height - top - bottom, 0]);

    // You can use your own iterable as data as long as its items implement the `PointDatum` trait.
    let mut line_data = vec![(0_f32, 0_f32); 100];
    for i in 0..100 {
        line_data[i] = (i as f32, line[i] as f32);
    }

    let mut zipf = zipf::ZipfDistribution::new(99, 0.5).unwrap();

    let mut line1 = vec![0; 100];
    for _i in 0..1000 {
        let n = zipf.sample(&mut rng);
        line1[n] = line1[n] + 1;
    }

    let mut line_data1 = vec![(0_f32, 0_f32); 100];
    for i in 0..100 {
        line_data1[i] = (i as f32, line1[i] as f32);
    }

    let mut zipf = zipf::ZipfDistribution::new(99, 0.1).unwrap();

    let mut line2 = vec![0; 100];
    for _i in 0..1000 {
        let n = zipf.sample(&mut rng);
        line2[n] = line2[n] + 1;
    }

    let mut line_data2 = vec![(0_f32, 0_f32); 100];
    for i in 0..100 {
        line_data2[i] = (i as f32, line2[i] as f32);
    }

    // Create Line series view that is going to represent the data.
    let line_view = LineSeriesView::new()
        .set_x_scale(&x)
        .set_y_scale(&y)
        .set_marker_type(MarkerType::X)
        .set_label_visibility(false)
        .set_label_position(PointLabelPosition::N)
        .set_colors(Color::from_vec_of_hex_strings(vec!["#aa0000"]))
        .load_data(&line_data).unwrap();

    let line_view1 = LineSeriesView::new()
        .set_x_scale(&x)
        .set_y_scale(&y)
        .set_marker_type(MarkerType::X)
        .set_label_visibility(false)
        .set_colors(Color::from_vec_of_hex_strings(vec!["#00aa00"]))
        .set_label_position(PointLabelPosition::N)
        .load_data(&line_data1).unwrap();

    let line_view2 = LineSeriesView::new()
        .set_x_scale(&x)
        .set_y_scale(&y)
        .set_marker_type(MarkerType::X)
        .set_label_visibility(false)
        .set_colors(Color::from_vec_of_hex_strings(vec!["#0000aa"]))
        .set_label_position(PointLabelPosition::N)
        .load_data(&line_data2).unwrap();

    // Generate and save the chart.
    Chart::new()
        .set_width(width)
        .set_height(height)
        .set_margins(top, right, bottom, left)
        .add_title(String::from("Zipf Distribution"))
        .add_view(&line_view)
        .add_view(&line_view1)
        .add_view(&line_view2)
        .add_axis_bottom(&x)
        .add_axis_left(&y)
        .add_left_axis_label("Appearance Number")
        .add_bottom_axis_label("Elements")
        .save("zipf.svg").unwrap();
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
    let cache_size = 20_000usize;

    let rng = fastrand::Rng::new();
    rng.seed(6);

    let out = Box::new(std::io::stdout());

    // println!("Draw the Zipf distribution");
    // zipf_plot();

    // generate data in db
    {
        for i in 0..entry_num {
            let key = make_key(i, key_size);
            let value = make_value(value_size, &rng);
            let _ret = map.fast_insert(key, value);
        }
    }

    println!("Now measure random read on tikv...");
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        map.get(&key)?;
        let new_key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let new_value = make_value(value_size, &rng);
        map.insert(new_key, new_value);
        Ok(true)
    });

    println!("Now measure random read on hashmap...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    // for _i in 0..cache_size {
    //     // Random fill the cache first
    //     let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
    //     map.backend.hashmap_get(&map.inner, &key);
    // }
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let ret = map.backend.hashmap_get(&map.inner, &key)?;
        let new_key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let new_value = make_value(value_size, &rng);
        map.backend.hashmap_fast_insert(&map.inner, new_key, new_value);
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
    // for _i in 0..cache_size {
    //     // Random fill the cache first
    //     let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
    //     map.backend.lru_get(&map.inner, &key);
    // }
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let ret = map.backend.lru_get(&map.inner, &key)?;
        let new_key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let new_value = make_value(value_size, &rng);
        map.backend.lru_fast_insert(&map.inner, new_key, new_value);
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
    // for _i in 0..cache_size {
    //     // Random fill the cache first
    //     let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
    //     map.backend.tiny_lfu_get(&map.inner, &key);
    // }
    let _ret = measure(out, || {
        let key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let ret = map.backend.tiny_lfu_get(&map.inner, &key)?;
        let new_key: Vec<_> = make_key(rng.usize(0..entry_num), key_size);
        let new_value = make_value(value_size, &rng);
        map.backend.tiny_lfu_fast_insert(&map.inner, new_key, new_value);
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    let mut rng = rand::thread_rng();
    let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();

    let fast_rng = fastrand::Rng::new();
    fast_rng.seed(6);

    println!("Now measure zipf read on hashmap...");
    let out = Box::new(std::io::stdout());
    // let newhandle: Handle<MapState<Vec<u8>, Vec<u8>>, i32, i32> =
    //     Handle::map("hashmap").with_item_key(1).with_namespace(1);
    // for _i in 0..cache_size {
    //     // Random fill the cache first
    //     let key: Vec<_> = make_key(fast_rng.usize(0..entry_num), key_size);
    //     map.backend.hashmap_get(&map.inner, &key);
    // }
    let _ret = measure(out, || {
        // let mut rng = rand::thread_rng();
        // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();
        let key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        let ret = map.backend.hashmap_get(&map.inner, &key)?;
        // let new_key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        // let new_value = make_value(value_size, &fast_rng);
        // map.backend.hashmap_fast_insert(&map.inner, new_key, new_value);
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
    // for _i in 0..cache_size {
    //     // Random fill the cache first
    //     let key: Vec<_> = make_key(fast_rng.usize(0..entry_num), key_size);
    //     map.backend.lru_get(&map.inner, &key);
    // }
    let _ret = measure(out, || {
        // let mut rng = rand::thread_rng();
        // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();
        let key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        let ret = map.backend.lru_get(&map.inner, &key)?;
        // let new_key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        // let new_value = make_value(value_size, &fast_rng);
        // map.backend.lru_fast_insert(&map.inner, new_key, new_value);
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
    // for _i in 0..cache_size {
    //     // Random fill the cache first
    //     let key: Vec<_> = make_key(fast_rng.usize(0..entry_num), key_size);
    //     map.backend.tiny_lfu_get(&map.inner, &key);
    // }
    let _ret = measure(out, || {
        // let mut rng = rand::thread_rng();
        // let mut zipf = zipf::ZipfDistribution::new(entry_num, 1.0).unwrap();
        let key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        let ret = map.backend.tiny_lfu_get(&map.inner, &key)?;
        // let new_key: Vec<_> = make_key(zipf.sample(&mut rng), key_size);
        // let new_value = make_value(value_size, &fast_rng);
        // map.backend.tiny_lfu_fast_insert(&map.inner, new_key, new_value);
        if let Some((_, hit)) = ret {
            Ok(hit)
        } else {
            Err("Oops".into())
        }
    });

    // cargo run --example eval
}
