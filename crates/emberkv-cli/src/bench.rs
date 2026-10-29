//! Closed-loop load generator.
//!
//! Each connection sends one request, waits for the reply, records the
//! latency, and repeats. Latencies go into an HDR histogram so tail
//! percentiles are exact rather than averaged away.

use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;

#[derive(Debug, clap::Args)]
pub(crate) struct BenchArgs {
    /// Concurrent connections.
    #[arg(long, short, default_value_t = 32)]
    connections: u64,
    /// Total measured requests across all connections.
    #[arg(long, short = 'n', default_value_t = 200_000)]
    requests: u64,
    /// Number of distinct keys.
    #[arg(long, default_value_t = 10_000)]
    keyspace: u64,
    /// Value size in bytes.
    #[arg(long, default_value_t = 128)]
    value_size: usize,
    /// Fraction of requests that are reads, in 0.0..=1.0.
    #[arg(long, default_value_t = 0.8)]
    read_ratio: f64,
}

struct WorkerResult {
    reads: Histogram<u64>,
    writes: Histogram<u64>,
}

pub(crate) async fn run(addr: &str, token: Option<&str>, args: BenchArgs) -> anyhow::Result<()> {
    anyhow::ensure!(args.connections > 0 && args.keyspace > 0, "connections and keyspace must be positive");
    let value = Bytes::from(vec![b'x'; args.value_size]);

    println!("prefilling {} keys ...", args.keyspace);
    let mut client = crate::connect(addr, token).await?;
    for i in 0..args.keyspace {
        client.put(key(i), value.clone()).await?;
    }

    println!(
        "running {} requests over {} connections ({:.0}% reads, {}-byte values) ...",
        args.requests,
        args.connections,
        args.read_ratio * 100.0,
        args.value_size
    );
    let per_worker = args.requests / args.connections;
    let read_threshold = (args.read_ratio.clamp(0.0, 1.0) * u64::MAX as f64) as u64;
    let started = Instant::now();
    let workers: Vec<_> = (0..args.connections)
        .map(|id| {
            let (addr, token, value) = (addr.to_owned(), token.map(str::to_owned), value.clone());
            let keyspace = args.keyspace;
            tokio::spawn(async move {
                worker(id, &addr, token.as_deref(), per_worker, keyspace, read_threshold, value).await
            })
        })
        .collect();

    let mut reads = new_histogram();
    let mut writes = new_histogram();
    for worker in workers {
        let result = worker.await??;
        reads.add(&result.reads)?;
        writes.add(&result.writes)?;
    }
    let elapsed = started.elapsed();
    let total = reads.len() + writes.len();

    println!();
    println!(
        "completed {total} requests in {elapsed:.2?}  =>  {:.0} req/s",
        total as f64 / elapsed.as_secs_f64()
    );
    println!();
    println!("{:<6} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}", "op", "count", "p50", "p90", "p99", "p99.9", "max");
    report("get", &reads);
    report("put", &writes);
    Ok(())
}

async fn worker(
    id: u64,
    addr: &str,
    token: Option<&str>,
    requests: u64,
    keyspace: u64,
    read_threshold: u64,
    value: Bytes,
) -> anyhow::Result<WorkerResult> {
    let mut client = crate::connect(addr, token).await?;
    let mut rng = SplitMix64(id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xDEAD_BEEF);
    let mut result = WorkerResult { reads: new_histogram(), writes: new_histogram() };
    for _ in 0..requests {
        let k = key(rng.next() % keyspace);
        let is_read = rng.next() < read_threshold;
        let started = Instant::now();
        if is_read {
            client.get(k).await?;
        } else {
            client.put(k, value.clone()).await?;
        }
        let micros = started.elapsed().as_micros() as u64;
        let histogram = if is_read { &mut result.reads } else { &mut result.writes };
        histogram.saturating_record(micros);
    }
    Ok(result)
}

fn key(i: u64) -> String {
    format!("bench:{i:010}")
}

fn new_histogram() -> Histogram<u64> {
    // 1µs .. 60s at 3 significant digits.
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

fn report(op: &str, h: &Histogram<u64>) {
    if h.is_empty() {
        return;
    }
    let fmt = |micros: u64| format!("{:.2?}", Duration::from_micros(micros));
    println!(
        "{:<6} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        op,
        h.len(),
        fmt(h.value_at_quantile(0.50)),
        fmt(h.value_at_quantile(0.90)),
        fmt(h.value_at_quantile(0.99)),
        fmt(h.value_at_quantile(0.999)),
        fmt(h.max()),
    );
}

/// Tiny deterministic PRNG; avoids a dependency for something this simple.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
