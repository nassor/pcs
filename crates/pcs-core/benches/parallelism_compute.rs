// Parallelism compute benchmark
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench --bench parallelism_compute -- --sample-size 10
//
// Uses SHA3-256 (128 bytes/row → 32-byte hash) to provide a genuinely
// CPU-bound workload (a cheaper math kernel like sqrt would be
// DRAM-bandwidth-bound and mask real parallelism wins).
//
// Three configurations:
//   (a) sequential: single-threaded run() in a System
//   (b) slice_parallel: ParallelSystem with run_slice (rayon)
//   (c) high_threshold: same slice_parallel but threshold forced to 10M rows,
//       so the 1M-row batch falls back to single-threaded.
//
// Expected for (b): ≥ 0.7 × num_cpus speedup over (a).
// If (b) ≈ (c) ≈ (a), the slice-parallel infrastructure is broken.
//
// Data: 1M rows, each with a 128-byte random blob stored as a fixed-size
// binary column. SHA3-256 is computed per row. The result is stored as a
// 32-byte hash column.

use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use pcs_core::PcsError;
use pcs_core::component::Component;
use pcs_core::pipeline::{Dataset, Pipeline};
use pcs_core::system::{ParallelSystem, SliceWriteSet, System, SystemMeta, WriteSet};
use sha3::{Digest, Sha3_256};

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

/// Input: 128-byte random blob per row.
/// We implement Component manually (schema only) because FixedSizeBinary is not
/// supported by serde_arrow — batches are built directly from arrays.
struct Blob;

impl Component for Blob {
    fn name() -> &'static str {
        "Blob"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::FixedSizeBinary(128),
            false,
        )]))
    }
}

/// Output: 32-byte SHA3-256 hash per row.
struct Hash;

impl Component for Hash {
    fn name() -> &'static str {
        "Hash"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "digest",
            DataType::FixedSizeBinary(32),
            false,
        )]))
    }
}

// ---------------------------------------------------------------------------
// Data generator
// ---------------------------------------------------------------------------

fn generate_blob_batch(n: usize, seed: u64) -> RecordBatch {
    use std::num::Wrapping;
    let mut state = Wrapping(seed);
    let lcg = |s: &mut Wrapping<u64>| -> u64 {
        *s = *s * Wrapping(6364136223846793005) + Wrapping(1442695040888963407);
        s.0
    };

    // FixedSizeBinaryArray stores values as a flat byte buffer
    let mut flat: Vec<u8> = Vec::with_capacity(n * 128);
    for _ in 0..n {
        for _ in 0..16 {
            // 16 × 8 bytes = 128 bytes
            let v = lcg(&mut state);
            flat.extend_from_slice(&v.to_le_bytes());
        }
    }

    let arr = FixedSizeBinaryArray::try_from_iter(flat.chunks_exact(128))
        .expect("FixedSizeBinaryArray construction failed");

    let schema = Blob::schema();
    RecordBatch::try_new(schema, vec![Arc::new(arr)])
        .expect("generate_blob_batch: RecordBatch construction failed")
}

/// Build a zeroed-out hash batch to serve as the write target.
fn zero_hash_batch(n: usize) -> RecordBatch {
    let zero: Vec<u8> = vec![0u8; 32];
    // Repeat the zero slice n times
    let flat: Vec<u8> = zero.iter().cloned().cycle().take(n * 32).collect();
    let arr = FixedSizeBinaryArray::try_from_iter(flat.chunks_exact(32))
        .expect("zero_hash_batch construction failed");
    RecordBatch::try_new(Hash::schema(), vec![Arc::new(arr)])
        .expect("zero_hash_batch: RecordBatch construction failed")
}

/// Compute SHA3-256 hashes for a slice of a FixedSizeBinaryArray.
fn compute_hashes(arr: &FixedSizeBinaryArray) -> Vec<u8> {
    let n = arr.len();
    let mut out = Vec::with_capacity(n * 32);
    for i in 0..n {
        let blob = arr.value(i);
        let hash = Sha3_256::digest(blob);
        out.extend_from_slice(&hash);
    }
    out
}

fn hashes_to_array(flat: Vec<u8>, n: usize) -> Arc<dyn arrow_array::Array> {
    assert_eq!(flat.len(), n * 32);
    if n == 0 {
        // FixedSizeBinaryArray::try_from_iter requires at least one element to
        // infer the item size. For empty output, construct a zero-row array directly.
        return Arc::new(FixedSizeBinaryArray::new_null(32, 0));
    }
    Arc::new(
        FixedSizeBinaryArray::try_from_iter(flat.chunks_exact(32)).expect("hashes_to_array failed"),
    )
}

// ---------------------------------------------------------------------------
// Systems
// ---------------------------------------------------------------------------

/// Sequential SHA3-256 system (System / &mut pipeline).
struct SequentialHashSystem;

#[async_trait]
impl System for SequentialHashSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("sequential_hash")
            .read("Blob", "data")
            .write("Hash", "digest")
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let blob_batch = pipeline
            .batch_for("Blob")
            .ok_or_else(|| PcsError::generic("Blob not found"))?;
        let arr = blob_batch
            .column_by_name("data")
            .ok_or_else(|| PcsError::generic("data not found"))?
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| PcsError::generic("data wrong type"))?;

        let n = arr.len();
        let flat = compute_hashes(arr);
        let hash_arr = hashes_to_array(flat, n);

        let batch = RecordBatch::try_new(Hash::schema(), vec![hash_arr])
            .map_err(|e| PcsError::generic(format!("hash batch rebuild: {e}")))?;
        pipeline
            .replace_batch::<Hash>(batch)
            .map_err(|e| PcsError::generic(format!("replace_batch Hash: {e}")))?;
        Ok(())
    }
}

/// Slice-parallel SHA3-256 system (ParallelSystem with run_slice).
struct ParallelHashSystem;

#[async_trait]
impl ParallelSystem for ParallelHashSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("parallel_hash")
            .read("Blob", "data")
            .write("Hash", "digest")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let blob_batch = pipeline
            .batch_for("Blob")
            .ok_or_else(|| PcsError::generic("Blob not found"))?;
        let arr = blob_batch
            .column_by_name("data")
            .ok_or_else(|| PcsError::generic("data not found"))?
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| PcsError::generic("data wrong type"))?;

        let n = arr.len();
        let flat = compute_hashes(arr);
        let hash_arr = hashes_to_array(flat, n);
        Ok(WriteSet::new().put("Hash", "digest", hash_arr))
    }

    fn run_slice(
        &self,
        pipeline: &Dataset,
        rows: std::ops::Range<u32>,
    ) -> Option<Result<SliceWriteSet, PcsError>> {
        let blob_batch = pipeline.batch_for("Blob")?;
        let arr = blob_batch
            .column_by_name("data")?
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()?;

        let start = rows.start as usize;
        let len = (rows.end - rows.start) as usize;
        let slice = arr.slice(start, len);
        let slice_arr = slice
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let flat = compute_hashes(slice_arr);
        let hash_arr: Arc<dyn arrow_array::Array> = hashes_to_array(flat, len);
        Some(Ok(SliceWriteSet::new(rows).put("Hash", "digest", hash_arr)))
    }
}

// ---------------------------------------------------------------------------
// Pipeline builder
// ---------------------------------------------------------------------------

fn build_pipeline(blob_batch: &RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<Blob>().unwrap();
    pipeline.register_component::<Hash>().unwrap();
    pipeline
        .append_record_batch("Blob", blob_batch.clone())
        .unwrap();
    let n = blob_batch.num_rows();
    pipeline
        .append_record_batch("Hash", zero_hash_batch(n))
        .unwrap();
    pipeline
}

// ---------------------------------------------------------------------------
// Correctness check helper
// ---------------------------------------------------------------------------

fn extract_hashes(pipeline: &Dataset) -> Vec<Vec<u8>> {
    let batch = pipeline.batch_for("Hash").unwrap();
    let arr = batch
        .column_by_name("digest")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..arr.len()).map(|i| arr.value(i).to_vec()).collect()
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn bench_parallelism_compute(c: &mut Criterion) {
    const N: usize = 1_000_000;
    const SEED: u64 = 42;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let num_cpus = num_cpus::get();
    let blob_batch = generate_blob_batch(N, SEED);

    println!(
        "\n[parallelism_compute] {} rows × 128 bytes/row = {:.1} MB input, {} CPUs",
        N,
        (N * 128) as f64 / 1e6,
        num_cpus
    );

    // Correctness: sequential == parallel hashes
    {
        let mut wl_seq = Pipeline::new("hash_seq_check");
        wl_seq.data = build_pipeline(&blob_batch);
        wl_seq.add_system(SequentialHashSystem);
        rt.block_on(wl_seq.run()).unwrap();
        let seq_hashes = extract_hashes(&wl_seq.data);

        let mut wl_par = Pipeline::new("hash_par_check");
        wl_par.data = build_pipeline(&blob_batch);
        wl_par.add_parallel_system(ParallelHashSystem);
        rt.block_on(wl_par.run()).unwrap();
        let par_hashes = extract_hashes(&wl_par.data);

        assert_eq!(seq_hashes.len(), par_hashes.len(), "hash count mismatch");
        for (i, (s, p)) in seq_hashes.iter().zip(par_hashes.iter()).enumerate() {
            assert_eq!(s, p, "hash mismatch at row {i}");
        }
        println!("[parallelism_compute] correctness check passed ({N} hashes verified)");
    }

    let mut group = c.benchmark_group("parallelism_compute");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));

    // (a) Sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("hash_seq");
            wl.data = build_pipeline(std::hint::black_box(&blob_batch));
            wl.add_system(SequentialHashSystem);
            rt.block_on(wl.run()).unwrap();
            let first = wl.data.batch_for("Hash").map(|b| b.num_rows()).unwrap_or(0);
            std::hint::black_box(first)
        })
    });

    // (b) Slice parallel (default threshold = 100_000)
    group.bench_function("slice_parallel", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("hash_par");
            wl.data = build_pipeline(std::hint::black_box(&blob_batch));
            wl.add_parallel_system(ParallelHashSystem);
            rt.block_on(wl.run()).unwrap();
            let first = wl.data.batch_for("Hash").map(|b| b.num_rows()).unwrap_or(0);
            std::hint::black_box(first)
        })
    });

    // (c) Force single-threaded by wrapping in a non-slice parallel system.
    //     We use the system's run() path (no run_slice override), which is
    //     equivalent to forcing the threshold above N.
    struct ForcedSingleThreadSystem;
    #[async_trait]
    impl ParallelSystem for ForcedSingleThreadSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("forced_single_thread")
                .read("Blob", "data")
                .write("Hash", "digest")
        }
        async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
            // Same as ParallelHashSystem::run — single-threaded path.
            let blob_batch = pipeline
                .batch_for("Blob")
                .ok_or_else(|| PcsError::generic("Blob not found"))?;
            let arr = blob_batch
                .column_by_name("data")
                .ok_or_else(|| PcsError::generic("data not found"))?
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| PcsError::generic("data wrong type"))?;
            let n = arr.len();
            let flat = compute_hashes(arr);
            let hash_arr = hashes_to_array(flat, n);
            Ok(WriteSet::new().put("Hash", "digest", hash_arr))
        }
        // run_slice returns None → slice parallelism disabled
    }

    group.bench_function("high_threshold_single_thread", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("hash_forced_st");
            wl.data = build_pipeline(std::hint::black_box(&blob_batch));
            wl.add_parallel_system(ForcedSingleThreadSystem);
            rt.block_on(wl.run()).unwrap();
            let first = wl.data.batch_for("Hash").map(|b| b.num_rows()).unwrap_or(0);
            std::hint::black_box(first)
        })
    });

    group.finish();

    // Post-benchmark: compute and print speedup ratio
    // (we can't easily read criterion timings here, but we print CPUs for context)
    println!(
        "[parallelism_compute] target: slice_parallel speedup >= {:.2}x over sequential (0.7 × {num_cpus} CPUs)",
        0.7 * num_cpus as f64
    );
}

criterion_group!(benches, bench_parallelism_compute);
criterion_main!(benches);
