//! Drive one PCS workload through six WebAssembly guests written in six
//! different languages. Every stage is a separate `.wasm` component
//! implementing the same `pcs:pipeline@0.2.0` WIT world, loaded through the
//! same host (`WasmPipelineRuntime`).
//!
//! | order | component        | language   | writes                  |
//! |-------|------------------|------------|-------------------------|
//! | 1     | `validate-go`    | Go         | `valid`                 |
//! | 2     | `enrich-py`      | Python     | `usd_amount`            |
//! | 3     | `score-ts`       | TypeScript | `risk_score`, `flagged` |
//! | 4     | `fee-kt`         | Kotlin     | `fee`                   |
//! | 5     | `tier-cs`        | C#         | `review_tier`           |
//! | 6     | `settle-rs`      | Rust       | `settlement` + ledger   |
//!
//! ```bash
//! # regenerate examples/polyglot/generated/ (schema bytes, fingerprint, fixtures)
//! cargo run -p pcs-service --features wasm --example polyglot_orders -- emit
//!
//! # run the six-language chain (needs `bash scripts/build-polyglot.sh` first)
//! cargo run -p pcs-service --features wasm,tracing --example polyglot_orders
//! ```
//!
//! Without the `tracing` feature the host prints guest logs through `eprintln!`
//! and drops guest metrics, so use `wasm,tracing` for the run.
//!
//! `emit` derives the canonical `Order` Arrow IPC schema bytes and fingerprint
//! from `pcs_polyglot_order::Order` and writes one generated source file per
//! language, plus the shared six-row fixture as `.pcs` (host wire format) and
//! `.json` (expected values). Only the Rust guest links arrow-rs; the other five
//! need those constants baked in. `run` checks every stage's reported
//! `schema_fingerprint` against the live one and exits non-zero on mismatch.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use arrow_ipc::writer::StreamWriter;
use pcs_core::runtime::PipelineRuntime;
use pcs_polyglot_order::{Order, fixture_rows};
use pcs_service::component::Component;
use pcs_service::dataset::Dataset;
use pcs_service::wasm::{WasmEngine, WasmPipelineRuntime};

/// Epoch ticks granted to each guest call: 100 × 100 ms, the same 10 s budget
/// `pcs-service`'s own WASM loader uses.
const EPOCH_TICKS: u64 = 100;

/// Absolute tolerance for every float comparison and for the formatted table.
/// `100.0 * 1.10` is not exactly `110.0` in f64.
const EPS: f64 = 1e-6;

/// Install a subscriber so the guests' `host-io::log` and `host-io::metric`
/// calls reach the terminal. Metrics arrive at TRACE, which no default filter
/// passes, hence the explicit directive. `RUST_LOG` overrides it.
#[cfg(feature = "tracing")]
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,pcs_service::wasm=trace"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .try_init();
}

/// Built without the `tracing` feature: the host uses `eprintln!` for `log` and
/// drops `metric`. Nothing to install.
#[cfg(not(feature = "tracing"))]
fn init_tracing() {}

/// Workspace root, derived from this crate's manifest dir so the example
/// behaves identically no matter which directory `cargo run` was invoked from.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/pcs-service")
        .to_path_buf()
}

fn generated_dir() -> PathBuf {
    workspace_root().join("examples/polyglot/generated")
}

/// Directory holding the four built components. `PCS_POLYGLOT_BUILD_DIR`
/// overrides it; the integration test resolves the same way.
fn build_dir() -> PathBuf {
    match std::env::var_os("PCS_POLYGLOT_BUILD_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => workspace_root().join("examples/polyglot/build"),
    }
}

/// A dataset with only `Order` registered: the source of both the fingerprint
/// and the fixture.
fn order_dataset() -> Dataset {
    let mut dataset = Dataset::new();
    dataset
        .register_component::<Order>()
        .expect("register Order");
    dataset
}

/// The 8-char lowercase hex fingerprint that WIT
/// `pipeline-descriptor.schema-fingerprint` carries for a pipeline registering
/// exactly `Order`.
fn order_fingerprint() -> String {
    format!("{:08x}", order_dataset().schemas().fingerprint())
}

/// Schema-only Arrow IPC stream for `Order`: a `StreamWriter` opened on the
/// schema and finished with no batches. Byte-identical to what
/// `pcs_guest::__rt::schema_to_ipc_bytes` produces, which is what the host
/// parses out of `component-descriptor.arrow-schema-ipc`.
fn order_schema_ipc() -> Vec<u8> {
    let schema = Order::schema();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, &schema).expect("StreamWriter::try_new(Order::schema)");
        writer.finish().expect("StreamWriter::finish");
    }
    buf
}

/// Standard base64 with `=` padding (RFC 4648 §4). Hand-rolled so the example
/// stays dependency-free; it runs once per `emit`, on ~700 bytes.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(triple >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[triple as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Header stamped into every generated source file, in the target language's
/// comment syntax.
const GENERATED_BY: &str = "@generated by `cargo run -p pcs-service --features wasm --example polyglot_orders -- emit` — do not edit.";

fn emit() -> Result<(), Box<dyn std::error::Error>> {
    let dir = generated_dir();
    fs::create_dir_all(&dir)?;

    let schema_ipc = order_schema_ipc();
    let fingerprint = order_fingerprint();
    let b64 = base64(&schema_ipc);

    let rows = fixture_rows();
    let mut dataset = order_dataset();
    dataset.append::<Order>(&rows)?;
    let mut fixture: Vec<u8> = Vec::new();
    dataset.write_ipc(&mut fixture)?;

    let json = serde_json::to_string_pretty(&rows)?;

    let go = format!(
        "// {GENERATED_BY}\n\npackage export_pcs_pipeline_pipeline\n\n\
         // OrderSchemaIPCBase64 is the canonical `Order` Arrow IPC schema-message\n\
         // stream, base64-encoded. Decoded once at package init.\n\
         const OrderSchemaIPCBase64 = \"{b64}\"\n\n\
         // OrderFingerprint is the schema fingerprint the host expects in\n\
         // pipeline-descriptor.schema-fingerprint.\n\
         const OrderFingerprint = \"{fingerprint}\"\n"
    );
    let py = format!(
        "# {GENERATED_BY}\n\n\
         #: Canonical `Order` Arrow IPC schema-message stream, base64-encoded.\n\
         ORDER_SCHEMA_IPC_BASE64 = \"{b64}\"\n\n\
         #: Schema fingerprint reported in pipeline-descriptor.schema-fingerprint.\n\
         ORDER_FINGERPRINT = \"{fingerprint}\"\n"
    );
    let ts = format!(
        "// {GENERATED_BY}\n\n\
         /** Canonical `Order` Arrow IPC schema-message stream, base64-encoded. */\n\
         export const ORDER_SCHEMA_IPC_BASE64: string = \"{b64}\";\n\n\
         /** Schema fingerprint reported in pipeline-descriptor.schema-fingerprint. */\n\
         export const ORDER_FINGERPRINT: string = \"{fingerprint}\";\n"
    );
    let kt = format!(
        "// {GENERATED_BY}\n\n\
         package impl\n\n\
         /** Canonical `Order` Arrow IPC schema-message stream, base64-encoded. */\n\
         const val ORDER_SCHEMA_IPC_BASE64: String =\n    \"{b64}\"\n\n\
         /** Schema fingerprint reported in pipeline-descriptor.schema-fingerprint. */\n\
         const val ORDER_FINGERPRINT: String = \"{fingerprint}\"\n"
    );
    let cs = format!(
        "// {GENERATED_BY}\n\n\
         namespace PolyglotTier;\n\n\
         internal static class SchemaGen\n{{\n\
         \x20   /// <summary>Canonical `Order` Arrow IPC schema-message stream, base64-encoded.</summary>\n\
         \x20   internal const string OrderSchemaIpcBase64 =\n\
         \x20       \"{b64}\";\n\n\
         \x20   /// <summary>Schema fingerprint reported in pipeline-descriptor.schema-fingerprint.</summary>\n\
         \x20   internal const string OrderFingerprint = \"{fingerprint}\";\n\
         }}\n"
    );

    let files: [(&str, &[u8]); 9] = [
        ("order_schema.ipc", &schema_ipc),
        ("order_fingerprint.txt", fingerprint.as_bytes()),
        ("fixture_input.pcs", &fixture),
        ("fixture_input.json", json.as_bytes()),
        ("schema_gen.go", go.as_bytes()),
        ("schema_gen.py", py.as_bytes()),
        ("schema_gen.ts", ts.as_bytes()),
        ("SchemaGen.kt", kt.as_bytes()),
        ("SchemaGen.cs", cs.as_bytes()),
    ];

    for (name, bytes) in files {
        let path = dir.join(name);
        fs::write(&path, bytes)?;
        println!("wrote {} ({} bytes)", path.display(), bytes.len());
    }

    println!("\nOrder fingerprint: {fingerprint}");
    println!("Order schema IPC:  {} bytes", schema_ipc.len());
    Ok(())
}

/// One stage of the chain: which file to load, what to call it, and the config
/// table the host injects through `host-io::get-config`.
struct Stage {
    file: &'static str,
    name: &'static str,
    language: &'static str,
    config: &'static [(&'static str, &'static str)],
}

/// The chain, in execution order: each stage reads columns the previous one
/// wrote.
const STAGES: [Stage; 6] = [
    Stage {
        file: "validate-go.wasm",
        name: "polyglot-validate-go",
        language: "Go",
        config: &[("min_amount", "0")],
    },
    Stage {
        file: "enrich-py.wasm",
        name: "polyglot-enrich-py",
        language: "Python",
        config: &[("fx_eur", "1.10"), ("fx_gbp", "1.30"), ("fx_jpy", "0.0068")],
    },
    Stage {
        file: "score-ts.wasm",
        name: "polyglot-score-ts",
        language: "TypeScript",
        config: &[("risk_threshold", "50000")],
    },
    Stage {
        file: "fee-kt.wasm",
        name: "polyglot-fee-kt",
        language: "Kotlin",
        config: &[
            ("fee_emea", "0.012"),
            ("fee_apac", "0.008"),
            ("fee_amer", "0.010"),
        ],
    },
    Stage {
        file: "tier-cs.wasm",
        name: "polyglot-tier-cs",
        language: "C#",
        config: &[("review_score", "0.2")],
    },
    Stage {
        file: "settle-rs.wasm",
        name: "polyglot-settle-rs",
        language: "Rust",
        config: &[],
    },
];

fn load_stage(engine: &WasmEngine, stage: &Stage) -> Result<WasmPipelineRuntime, String> {
    let path = build_dir().join(stage.file);
    let bytes = fs::read(&path).map_err(|e| {
        format!(
            "{}: {e}\n  run `bash scripts/build-polyglot.sh` first",
            path.display()
        )
    })?;
    let config: HashMap<String, String> = stage
        .config
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    WasmPipelineRuntime::from_bytes(engine.clone(), stage.name, &bytes, config, EPOCH_TICKS)
        .map_err(|e| format!("{}: {e}", path.display()))
}

fn print_table(dataset: &Dataset) -> Result<(), Box<dyn std::error::Error>> {
    let orders = dataset.view::<Order>()?;
    let id = orders.i64("id")?;
    let region = orders.str("region")?;
    let currency = orders.str("currency")?;
    let amount = orders.f64("amount")?;
    let valid = orders.bool("valid")?;
    let usd = orders.f64("usd_amount")?;
    let risk = orders.f64("risk_score")?;
    let flagged = orders.bool("flagged")?;
    let fee = orders.f64("fee")?;
    let tier = orders.i64("review_tier")?;
    let settlement = orders.str("settlement")?;

    println!(
        "  {:>2}  {:<6} {:<4} {:>11}  {:<5} {:>11} {:>8}  {:<7} {:>9} {:>4}  {:<9}",
        "id",
        "region",
        "cur",
        "amount",
        "valid",
        "usd_amount",
        "risk",
        "flagged",
        "fee",
        "tier",
        "settlement"
    );
    for i in 0..orders.len() {
        println!(
            "  {:>2}  {:<6} {:<4} {:>11.2}  {:<5} {:>11.2} {:>8.4}  {:<7} {:>9.2} {:>4}  {:<9}",
            id.value(i),
            region.value(i),
            currency.value(i),
            amount.value(i),
            valid.value(i),
            usd.value(i),
            risk.value(i),
            flagged.value(i),
            fee.value(i),
            tier.value(i),
            settlement.value(i),
        );
    }
    Ok(())
}

/// Read the ledger totals out of a `run-result.checkpoint` blob. The blob is a
/// standalone single-component Arrow IPC stream written by `pcs-guest`'s
/// `Stateful::capture`, so the host decodes it with the same `Dataset::read_ipc`
/// it uses for the data plane.
fn ledger_totals(blob: &[u8]) -> Result<(i64, f64), Box<dyn std::error::Error>> {
    let state = Dataset::read_ipc(&mut &blob[..])?;
    let batch = state
        .batch_for("Ledger")
        .ok_or("checkpoint blob has no Ledger component")?;
    let count = batch
        .column_by_name("settled_count")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .map(|a| a.value(0))
        })
        .ok_or("Ledger.settled_count missing or not Int64")?;
    let usd = batch
        .column_by_name("settled_usd")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .map(|a| a.value(0))
        })
        .ok_or("Ledger.settled_usd missing or not Float64")?;
    Ok((count, usd))
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let expected_fingerprint = order_fingerprint();
    println!("build dir:        {}", build_dir().display());
    println!("Order fingerprint: {expected_fingerprint} (from pcs_polyglot_order::Order)\n");

    let engine = WasmEngine::new()?;
    let mut runtimes: Vec<(&Stage, WasmPipelineRuntime)> = Vec::with_capacity(STAGES.len());
    for stage in &STAGES {
        match load_stage(&engine, stage) {
            Ok(runtime) => runtimes.push((stage, runtime)),
            Err(msg) => {
                eprintln!("error: cannot load {} stage: {msg}", stage.language);
                std::process::exit(1);
            }
        }
    }

    println!("── describe() ──");
    let mut drift = false;
    for (stage, runtime) in &runtimes {
        let d = runtime.describe()?;
        let components = runtime.declared_components();
        println!(
            "  {:<10} name={:<22} version={:<7} stateful={:<5} fingerprint={} components={:?}",
            stage.language, d.name, d.version, d.stateful, d.schema_fingerprint, components
        );
        if d.schema_fingerprint != expected_fingerprint {
            eprintln!(
                "  FINGERPRINT DRIFT: {} reports {} but pcs_polyglot_order::Order is {}",
                stage.name, d.schema_fingerprint, expected_fingerprint
            );
            drift = true;
        }
        if components != ["Order"] {
            eprintln!(
                "  COMPONENT DRIFT: {} declares {:?}, expected [\"Order\"]",
                stage.name, components
            );
            drift = true;
        }
    }
    if drift {
        eprintln!(
            "\nA stage disagrees with the canonical schema. Regenerate the constants:\n  \
             cargo run -p pcs-service --features wasm --example polyglot_orders -- emit\n  \
             bash scripts/build-polyglot.sh"
        );
        std::process::exit(1);
    }

    let mut dataset = order_dataset();
    dataset.append::<Order>(&fixture_rows())?;

    println!("\n── batch 1 ──");
    let mut checkpoint: Option<Vec<u8>> = None;
    for (stage, runtime) in &runtimes {
        let produced = runtime.run_on_with_state(&mut dataset, None).await?;
        match (&produced, stage.config.is_empty()) {
            (Some(blob), _) => {
                println!(
                    "  {:<10} {} → checkpoint {} bytes",
                    stage.language,
                    stage.name,
                    blob.len()
                );
                checkpoint = produced;
            }
            (None, _) => println!("  {:<10} {} → stateless", stage.language, stage.name),
        }
    }
    print_table(&dataset)?;

    let checkpoint = checkpoint.ok_or("no stage returned a checkpoint")?;
    let (count1, usd1) = ledger_totals(&checkpoint)?;
    println!("  ledger after batch 1: settled_count = {count1}, settled_usd = {usd1:.2}");

    println!("\n── batch 2 (checkpoint from batch 1 fed back as `prior`) ──");
    let mut dataset2 = order_dataset();
    dataset2.append::<Order>(&fixture_rows())?;

    let mut checkpoint2: Option<Vec<u8>> = None;
    for (stage, runtime) in &runtimes {
        // Only the stateful stage gets a `prior`; the others return `None`
        // regardless of what they are handed.
        let prior = if stage.name == "polyglot-settle-rs" {
            Some(checkpoint.as_slice())
        } else {
            None
        };
        let produced = runtime.run_on_with_state(&mut dataset2, prior).await?;
        if produced.is_some() {
            checkpoint2 = produced;
        }
    }
    print_table(&dataset2)?;

    let checkpoint2 = checkpoint2.ok_or("no stage returned a checkpoint on batch 2")?;
    let (count2, usd2) = ledger_totals(&checkpoint2)?;
    println!(
        "  ledger after batch 2: settled_count = {count2}, settled_usd = {usd2:.2} \
         (checkpoint {} bytes)",
        checkpoint2.len()
    );

    if count2 != count1 * 2 || (usd2 - usd1 * 2.0).abs() > EPS {
        eprintln!(
            "error: the stateful stage did not accumulate across the batch boundary \
             (batch 1: {count1}/{usd1:.2}, batch 2: {count2}/{usd2:.2})"
        );
        std::process::exit(1);
    }

    println!(
        "\nOK: six languages, one WIT world. Only `settlement` came from Rust. \
         `valid` came from Go, `usd_amount` from Python, `risk_score`/`flagged` from \
         TypeScript, `fee` from Kotlin, `review_tier` from C#."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    match std::env::args().nth(1).as_deref() {
        Some("emit") => emit(),
        _ => run().await,
    }
}
