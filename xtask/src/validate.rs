//! `cargo xtask validate`: gate every runnable example config through
//! `pcs-service validate`.
//!
//! One command that used to need a page of per-config `export VAR=...` lines
//! in a shell before `pcs-service validate` could even parse the file. The
//! config variables feature removes that: each config's registry entry
//! (`examples.rs`) carries the `variables` block that resolves its
//! `${...}` placeholders, and this task injects it into a temp copy, so every
//! example validates against an environment that declares nothing.
//!
//! `pcs-service validate` opens no connector connections (every factory
//! builds synchronously), so a config that references NATS, PostgreSQL or
//! Kafka still validates without those services running.
//!
//! Steps, per selected config:
//!
//! 1. Build the processor components it loads (wasm components, native
//!    plugins; the quickstart and polyglot builds run their own tasks). A
//!    config whose toolchains are missing is recorded as failed and does not
//!    stop the rest.
//! 2. Build `pcs-service` once with the union of features the configs whose
//!    artifacts built need (only `plugin` is ever non-default).
//! 3. Write a temp copy with the entry's `variables` block and run
//!    `pcs-service validate --config <copy>`.
//!
//! The templates under `examples/configs/` (`kafka.kdl`, `nats.kdl`,
//! `s3.kdl`, `postgresql.kdl`, `http.kdl`, `tcp.kdl`, `cluster.kdl`,
//! `redb.kdl`) are not in the registry: each either names a
//! `pipelines/*.wasm` placeholder that does not exist in the repository, so
//! `validate`'s build gate cannot load it, or expects an endpoint or store
//! path that only exists on the operator's machine. The registry is where a
//! real module path would add one.
//!
//! Exit codes:
//!
//! | code | meaning                                        |
//! |------|------------------------------------------------|
//! | 1    | an artifact build or a `pcs-service validate` run failed |
//! | 2    | `--only` named an unknown config               |

use std::path::PathBuf;

use crate::examples::{EXAMPLES, Example, build_service, inject, service_binary};
use crate::sh::{Ctx, Result};

const USAGE: &str = "usage: cargo xtask validate [--only=name1,name2,...]";

/// What `-h` prints.
const HELP: &str = "\
usage: cargo xtask validate [--only=LIST]

  --only=LIST   Validate only the named example configs, a comma-separated
                subset of:

                standalone_wasm   load the Rust order-processing component
                standalone_plugin load the native plugin smoketest
                branching         load both routers, wasm and plugin
                windowing         load both windowed processors
                quickstart        load the Go + C# Quick Start components
                standalone_polyglot  load the Python stage

                The default is every config above. Processor artifacts are
                built first, then `pcs-service validate` runs once per config
                against a temp copy carrying its `variables` block, so no
                OS env vars need exporting.";

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("validate");

    let mut only: Option<Vec<String>> = None;
    for arg in args {
        if let Some(list) = arg.strip_prefix("--only=") {
            only = Some(list.split(',').map(str::to_owned).collect());
            continue;
        }
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{HELP}");
                return Ok(());
            }
            other => return ctx.fail(1, &[&format!("unknown argument '{other}'"), USAGE]),
        }
    }

    let selected: Vec<&Example> = match &only {
        None => EXAMPLES.iter().collect(),
        Some(names) => {
            let mut found = Vec::new();
            for name in names {
                match crate::examples::by_name(name) {
                    Some(ex) => found.push(ex),
                    None => {
                        return ctx.fail(2, &[&format!("unknown example '{name}'"), HELP]);
                    }
                }
            }
            found
        }
    };

    // Build every selected config's artifacts, recording (not aborting on) a
    // failure — a config whose toolchains are missing still lets the rest
    // validate. Configs that built then get validated below.
    let mut build_failures: Vec<&'static str> = Vec::new();
    let mut buildable: Vec<&Example> = Vec::new();
    for ex in &selected {
        ctx.log(format!("building artifacts for '{0}'", ex.name));
        match (ex.build)(&ctx) {
            Ok(()) => buildable.push(ex),
            Err(e) => {
                ctx.log(format!(
                    "'{}' artifact build failed: {}",
                    ex.name, e.message
                ));
                build_failures.push(ex.name);
            }
        }
    }

    if buildable.is_empty() {
        return ctx.fail(
            1,
            &[&format!(
                "no example configs had their artifacts built; failing: {}",
                build_failures.join(", ")
            )],
        );
    }

    ctx.log("building pcs-service");
    build_service(&ctx, &buildable)?;
    let binary = service_binary(&ctx);

    let tmp = std::env::temp_dir();
    let mut validate_failures: Vec<&'static str> = Vec::new();
    for ex in &buildable {
        let copy = tmp.join(format!("pcs-xtask-{0}.kdl", ex.name));
        inject(&ctx, ex, &copy)?;
        ctx.log(format!(
            "validating '{0}' ({1})",
            ex.name,
            PathBuf::from(ex.config).display()
        ));
        match ctx.run_exe(&binary, &["validate", "--config", &copy.to_string_lossy()]) {
            Ok(()) => {}
            Err(e) => {
                ctx.log(format!("'{}' failed to validate: {}", ex.name, e.message));
                validate_failures.push(ex.name);
            }
        }
    }

    let passed = buildable.len() - validate_failures.len();
    if validate_failures.is_empty() {
        ctx.log(format!("PASS: all {passed} example configs validated"));
        Ok(())
    } else {
        ctx.fail(
            1,
            &[&format!(
                "{passed} of {} example configs validated; {} failed: {}",
                buildable.len(),
                validate_failures.len(),
                validate_failures.join(", ")
            )],
        )
    }
}
