// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Benchmarks for different Window types

use arcon::{prelude::*, stream::operator::window::WindowContext};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

const WINDOW_MSGS: usize = 100000;

fn arcon_window_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_window_latency");
    group.bench_function("AppenderWindow sum", window_appender_sum_latency);
    group.bench_function("IncrementalWindow sum", window_incremental_sum_latency);

    group.finish()
}

pub fn window_appender_sum_latency(b: &mut Bencher) {
    b.iter(|| window_appender_sum(black_box(WINDOW_MSGS)));
}

pub fn window_appender_sum(messages: usize) {
    let mut state_backend = InMemory::new("bench").unwrap();

    #[inline]
    fn materializer(buffer: &[u64]) -> u64 {
        buffer.iter().sum()
    }
    let mut window: AppenderWindow<u64, u64> =
        AppenderWindow::new(&materializer, &mut state_backend);
    for i in 0..messages {
        let _ = window.on_element(i as u64, WindowContext::new(&mut state_backend, 0, 0));
    }
    let s: u64 = window
        .result(WindowContext::new(&mut state_backend, 0, 0))
        .unwrap();
    assert!(s > 0);
}

pub fn window_incremental_sum_latency(b: &mut Bencher) {
    b.iter(|| window_incremental_sum(black_box(WINDOW_MSGS)));
}

pub fn window_incremental_sum(messages: usize) {
    let mut state_backend = InMemory::new("bench").unwrap();

    #[inline]
    fn init(i: u64) -> u64 {
        i
    }

    #[inline]
    fn aggregation(i: u64, agg: &u64) -> u64 {
        agg + i
    }

    let mut window: IncrementalWindow<u64, u64> =
        IncrementalWindow::new(&init, &aggregation, &mut state_backend);

    for i in 0..messages {
        let _ = window.on_element(i as u64, WindowContext::new(&mut state_backend, 0, 0));
    }

    let s: u64 = window
        .result(WindowContext::new(&mut state_backend, 0, 0))
        .unwrap();
    assert!(s > 0);
}

criterion_group!(benches, arcon_window_latency);
criterion_main!(benches);
