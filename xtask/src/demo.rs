//! `cargo xtask demo <name>`: run an example pipeline end to end.
//!
//! For a `one_shot` config this builds its artifacts and the service, writes a
//! `variables`-bearing copy of the config into `target/xtask/<name>.kdl`, and
//! runs `pcs-service serve` against it to completion — the FileSinks truncate
//! on build, so the run is repeatable and self-contained.
//!
//! A streaming config (branching, windowing, quickstart) has no natural end:
//! `serve` runs until Ctrl-C against live NATS/PostgreSQL services. For those
//! the command builds the artifacts and writes the same ready-to-run config,
//! then prints the exact `serve` + publisher invocations instead of blocking.
//! The injected config needs no OS env vars; only the external services do.
//!
//! Exit codes:
//!
//! | code | meaning                                    |
//! |------|--------------------------------------------|
//! | 1    | a build or the `serve` run failed          |
//! | 2    | no `<name>` given or it is unknown         |

use std::path::Path;

use crate::examples::{Example, build_service, inject, service_binary};
use crate::sh::{Ctx, Result};

const USAGE: &str = "usage: cargo xtask demo <name>";

const HELP: &str = "\
usage: cargo xtask demo <name>

  <name>  one of:
            standalone_wasm    one_shot: runs serve and exits
            standalone_plugin  one_shot: runs serve and exits
            branching          streaming: prints the run commands
            windowing          streaming: prints the run commands
            quickstart         streaming: prints the run commands
            standalone_polyglot  one_shot: runs serve and exits

  A one_shot demo builds the config's artifacts, writes
  target/xtask/<name>.kdl (a variables-bearing copy), and runs
  `pcs-service serve` against it. A streaming demo builds the artifacts and
  prints the serve + publisher commands, which need live NATS/PostgreSQL.";

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("demo");

    let name = match args {
        [name] if name == "-h" || name == "--help" => {
            println!("{HELP}");
            return Ok(());
        }
        [name] => name,
        [] => return ctx.fail(2, &["no <name> given", USAGE]),
        _ => return ctx.fail(2, &["too many arguments", USAGE]),
    };

    let Some(ex) = crate::examples::by_name(name) else {
        return ctx.fail(2, &[&format!("unknown example '{name}'"), HELP]);
    };

    ctx.log(format!("building artifacts for '{name}'"));
    (ex.build)(&ctx)?;

    ctx.log("building pcs-service");
    build_service(&ctx, &[ex])?;
    let binary = service_binary(&ctx);

    let out = ctx.root().join("target/xtask").join(format!("{name}.kdl"));
    inject(&ctx, ex, &out)?;
    ctx.log(format!("config written to {}", out.display()));

    if ex.one_shot {
        run_one_shot(&ctx, &binary, ex, &out)?;
    } else {
        print_commands(ex);
    }
    Ok(())
}

fn run_one_shot(ctx: &Ctx, binary: &Path, ex: &Example, config: &Path) -> Result<()> {
    ctx.log(format!("running `pcs-service serve` for '{0}'", ex.name));
    ctx.run_exe(binary, &["serve", "--config", &config.to_string_lossy()])?;
    ctx.log(format!("PASS: '{0}' ran to completion", ex.name));
    Ok(())
}

/// Print the `serve` + publisher invocations a streaming config needs, with
/// the already-written variables-bearing config.
fn print_commands(ex: &Example) {
    let publish = match ex.name {
        "branching" => "cargo run -p pcs-service --example branching_publish -- --rate 50",
        "windowing" => {
            "cargo run -p pcs-service --example windowed_publish -- --rate 20 --ts-step-ms 2000"
        }
        "quickstart" => {
            "cargo run -p pcs-service --example quickstart_publish --features connector-nats -- \
             --count 5000 --rate 500"
        }
        other => unreachable!("streaming config {other} has no publisher"),
    };
    println!("[demo] Start the services, then run:");
    match ex.name {
        "branching" => {
            println!("[demo]   docker run -d --name pcs-nats -p 4222:4222 nats:2.11-alpine");
        }
        _ => {
            println!(
                "[demo]   docker compose -f examples/{0}/docker-compose.yml up -d",
                ex.name
            );
        }
    }
    println!(
        "[demo]   cargo run -p pcs-service -- serve -c target/xtask/{0}.kdl",
        ex.name
    );
    println!("[demo]   {publish}");
}
