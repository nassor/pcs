+++
title = "Native plugins"
description = "A shared library the host loads at runtime: two C symbols, the same Arrow IPC wire format a processor speaks, and none of the sandbox."
template = "page.html"
weight = 2
+++

# Native plugins

A native plugin is a shared library that `pcs-service` loads at runtime, with
`dlopen` on Unix and `LoadLibrary` on Windows. It exports two C symbols.
`pcs_abi_version` reports the ABI the library was built against, and
`pcs_plugin_v1` fills a host allocated vtable with four function pointers.

The contract behind those pointers is the one a
[WebAssembly processor](@/processors/_index.md) implements, written in C
instead of WIT.
`describe` runs once at load and reports the plugin name, its version, and the
Arrow schema of every component it declares. `run_batch` runs once per batch over
the same [Arrow IPC wire format](@/reference/wire-format.md), and the opaque
checkpoint it returns is the only channel for state that crosses a batch
boundary.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 176" role="img" aria-labelledby="plg-title plg-desc">
        <title id="plg-title">The host loads a shared library and drives two calls across a C ABI</title>
        <desc id="plg-desc">
            pcs-service opens the shared library, checks the ABI version symbol, then calls
            pcs_plugin_v1 to collect a vtable of four function pointers. It calls describe
            once at load to learn the component schemas and the schema fingerprint, then
            run_batch once per batch, passing Arrow IPC input bytes and the prior checkpoint
            and receiving output rows, a new checkpoint and metrics. The plugin calls back
            into the host for log, metric and get_config. The host stores the returned
            checkpoint and replays it as the next batch's prior, which is the only state that
            crosses a batch boundary. Both sides share one address space, and no epoch
            deadline bounds the call.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="176" height="72" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="176" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="176" height="8"/>
            <text class="t-lbl" x="12" y="55">pcs-service</text>
            <text class="t-sm" x="12" y="76">dlopen, LoadLibrary</text>
            <text class="t-sm" x="12" y="89">owns the vtable</text>
            <text class="t-sm" x="12" y="102">no epoch deadline</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="456" y="40" width="204" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="456" y="40" width="204" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="456" y="52" width="204" height="8"/>
            <text class="t-lbl" x="468" y="55">your plugin &middot; .so</text>
            <text class="t-sm t-bnd" x="468" y="76">pcs_abi_version()</text>
            <text class="t-sm t-bnd" x="468" y="89">pcs_plugin_v1()</text>
            <text class="t-sm" x="468" y="102">one address space</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="316" y="44">describe() &rarr; manifest, once at load</text>
            <path class="arw arw-ctl" d="M176 50 H456" marker-end="url(#plg-c)"/>
            <text class="t-sm t-mid" x="316" y="62">run_batch(input, prior)</text>
            <path class="arw arw-data" d="M176 68 H456" marker-end="url(#plg-d)"/>
            <path class="arw arw-data" d="M456 86 H176" marker-end="url(#plg-d)"/>
            <text class="t-sm t-mid" x="316" y="98">rows, checkpoint, metrics</text>
        </g>
        <g class="anim anim-4">
            <text class="t-sm t-ctl t-mid" x="316" y="126">log, metric, get_config</text>
            <path class="arw arw-ctl" d="M456 132 H176" marker-end="url(#plg-c)"/>
            <path class="arw arw-bnd" d="M25 112 V148 H111 V112" marker-end="url(#plg-b)"/>
            <text class="t-sm t-bnd t-mid" x="68" y="164">checkpoint &rarr; prior</text>
        </g>
        <defs>
            <marker id="plg-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control)"/>
            </marker>
            <marker id="plg-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
            <marker id="plg-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> control plane: describe, and the three host callbacks</span>
        <span class="k-data"><i></i> data plane: Arrow IPC bytes both ways</span>
        <span class="k-boundary"><i></i> the C ABI boundary, and the checkpoint that crosses it</span>
    </div>
    <figcaption class="dgm-cap">
        Every <code>PcsBuffer</code> the plugin writes stays plugin owned. The host copies out
        of it and hands it back to <code>free_buffer</code>, so allocator ownership never
        crosses the boundary in either direction.
    </figcaption>
</div>

## What a plugin costs you

A processor is sandboxed, portable and preemptible. A wasmtime epoch deadline
bounds every call, and a trap ends the batch instead of the service.

A plugin has none of that. It runs in-process with full host privileges, it
cannot be interrupted, and a memory error in it is a memory error in the host.
What it buys is the absence of the sandbox: native threads, native extensions,
and no componentizer in the build.

<div class="note note-warn">
<span class="note-label">A plugin is an operator trusted path</span>

A wedged plugin wedges the thread driving it, and a segfault in one takes the
service down with it. The optional `sha3_256` digest below is the only integrity
gate there is. Point the `plugin` node at a library you built or a library you
trust the way you trust the service binary itself.

</div>

## Authoring one in Rust

`pcs-plugin` is the Rust SDK. A plugin crate sets `crate-type = ["cdylib"]`,
depends on it, and hands a function that builds a `Pipeline` to
`export_plugin!`. Every block below is from
`crates/pcs-plugin-smoketest/src/lib.rs`, which CI builds.

```toml,name=The plugin crate manifest
[lib]
crate-type = ["cdylib"]

[dependencies]
pcs-plugin = { path = "../pcs-plugin" }
serde      = { workspace = true }
```

```rust,name=The build function and the export macro
use pcs_plugin::prelude::*;

pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("smoketest-plugin");
    pipeline
        .data
        .register_component::<Counter>()
        .expect("register Counter");
    pipeline.add_system(AdvanceSystem);
    pipeline
}

pcs_plugin::export_plugin!(build, state = Total);
```

The macro writes both exported symbols, the four vtable thunks, and the
`pcs_config_get` and `pcs_config_parse` functions into the crate.
`cargo build --release` on it produces the library.

The optional `state = T` names the one component whose rows survive a batch,
and `export_plugin!(build)` without it declares a stateless plugin. `T` must
not be registered in `build()`: the macro decodes the prior checkpoint into a
`ProcessorState<T>` resource before the pipeline runs and captures it
afterwards, so those rows never appear in the output. A plugin's process
memory does survive between calls, and keeping state there is still wrong.
Consecutive batches of one partition may land on different processes, and
only the checkpoint travels with the claim.

Config values arrive as strings through those generated functions. Logging and
metrics go out through `pcs_plugin::host`, the native counterpart of the
`host-io` interface a processor imports.

```rust,name=Reading a config value in a system
let multiplier = match pcs_config_parse::<i64>(MULTIPLIER_KEY) {
    Some(Ok(value)) => value,
    Some(Err(e)) => {
        return Err(PcsError::system_execution(format!(
            "smoketest: {MULTIPLIER_KEY} is not an integer: {e}"
        )));
    }
    None => 1,
};
```

```rust,name=Logs and metrics through the host
pcs_plugin::host::metric("smoketest.rows", rows as f64);
pcs_plugin::host::info("smoketest", &format!("numbered {rows} rows through {advanced}"));
```

Return a `PcsError` from a system rather than panicking. `SystemExecution` and
`RetryExhausted` become a retryable status, every other variant becomes a
permanent one, and a caught panic is reported as permanent. The batch is lost
either way, and only the retryable status lets the runner release the claim and
try again.

## How the host loads one

```kdl,name=The plugin node in a service config
pipeline {
    plugin library="/var/lib/pcs/pipelines/libpcs_plugin_smoketest.so" {
        // Optional. Hex digest of the library file's bytes, with an optional
        // `sha3-256:` prefix.
        sha3_256 "abc123..."
        config "smoketest.multiplier"="10"
    }
}
```

A relative `library` resolves against the loader base directory. An unknown key
in the node is a parse error, and setting both a `wasm` and a `plugin` node
fails the build rather than picking one.

Load ordering is fixed: check `pcs_abi_version` against the host's own, verify
the digest when one is set, call `describe`, decode every component schema, then
recompute the schema fingerprint from what was decoded. A fingerprint that
disagrees with the manifest fails the load, because it means the plugin's
embedded schema constants have drifted from what it declares. All of it happens
in the constructor, so a running service never holds a plugin whose schemas it
has not verified.

To load one from your own binary, call
`pcs_service::plugin::NativePluginRuntime::open` with the library path and the
config map. There is no name argument: the manifest name is authoritative.

```bash,name=Build the plugin then load it
cargo build -p pcs-plugin-smoketest
cargo run -p pcs-service --features plugin --example native_plugin
```

## Plugins in other languages

Any toolchain that emits a shared library exporting those two C symbols can
author a plugin.

| Language | How the symbols get exported | Build |
|---|---|---|
| **Rust** | `pcs_plugin::export_plugin!` in a `cdylib` crate | `cargo build --release -p my-plugin` |
| **Go** | cgo, `//export pcs_abi_version` and `//export pcs_plugin_v1` | `go build -buildmode=c-shared -o my_plugin.so .` |
| **C#** | NativeAOT, `[UnmanagedCallersOnly(EntryPoint = "pcs_abi_version")]` | `dotnet publish -r linux-x64 -c Release` |
| **Kotlin** | GraalVM, `@CEntryPoint(name = "pcs_abi_version")` | `native-image --shared` |

Python and TypeScript cannot export a C ABI, so both stay on the
[WebAssembly processor](@/processors/_index.md) path.

`crates/pcs-plugin-abi/include/pcs_plugin.h` is the authority outside Rust. It
declares every struct and both entry points, and its contract comment carries the
buffer ownership rules and the threading rule: the host never calls one instance
concurrently, but successive calls may arrive on different OS threads.

The same comment names the hazard each language brings. The Go runtime installs
its own signal handlers and starts its collector at load, and cgo needs a static
shim to call a host function pointer. A GraalVM `@CEntryPoint` takes an
`IsolateThread`, and an `[UnmanagedCallersOnly]` method cannot throw across the
boundary. In every language, no panic and no exception may cross it.
