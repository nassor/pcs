+++
title = "Native pipelines"
description = "Link the engine into your own Rust binary: your main, your loop, no wasmtime and no service."
template = "section.html"
sort_by = "weight"

[extra]
kicker = "Native"
+++

Native mode is the short path. Your crate depends on `pcs-core` or `pcs-service`
as a library, you own `main`, and you call `pipeline.run().await`. There is no
`.wasm` file, no wasmtime, and no `pcs-service` binary in the picture.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 120" role="img" aria-labelledby="nat-title nat-desc">
        <title id="nat-title">A native pipeline runs entirely inside your own binary</title>
        <desc id="nat-desc">
            Your binary links the engine, builds a Pipeline that owns its stage plan and its
            per-system retry, and writes rows out to stdout, Parquet or CSV. Nothing crosses
            a component boundary and no separate host process is involved.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="176" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="176" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="176" height="8"/>
            <text class="t-lbl" x="12" y="49">your binary</text>
            <text class="t-sm" x="12" y="70">cargo run</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M176 60 H222" marker-end="url(#nat-d)"/>
            <rect class="blk blk-data" x="228" y="34" width="204" height="52" rx="8"/>
            <text class="t-lbl" x="240" y="56">Pipeline</text>
            <text class="t-sm" x="240" y="74">stages + retry</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M432 60 H478" marker-end="url(#nat-d)"/>
            <rect class="blk" x="484" y="34" width="176" height="52" rx="8"/>
            <text class="t-lbl" x="496" y="56">rows out</text>
            <text class="t-sm" x="496" y="74">stdout, Parquet, CSV</text>
        </g>
        <defs>
            <marker id="nat-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane, all of it in one process</span>
    </div>
    <figcaption class="dgm-cap">
        One process, one address space. The <code>Pipeline</code> derives its own stage plan
        from the field declarations and retries what fails, exactly as it does inside a
        WebAssembly guest.
    </figcaption>
</div>

## Which mode do you want

Reach for native when:

- The transform ships with the binary and they version together.
- You want a debugger and a profiler on the same process as the pipeline.
- You need Rust types and `Resource` singletons that never cross an IPC
  boundary.

Reach for a [WebAssembly guest](@/guests/_index.md) when:

- You want to change the pipeline without rebuilding the host.
- You need the sandbox: no filesystem, no network, no clock unless the host
  grants it.
- The pipeline is not Rust.

Reach for a [native plugin](@/native/plugins.md) when the transform must load at
runtime like a guest, and the sandbox is what stands in the way. It is a shared
library the service opens with `dlopen`: native threads and native extensions,
and none of the isolation.

<div class="note">
<span class="note-label">Depending on PCS</span>

The crates are **not published to crates.io**. Inside a clone of the repository,
use a path dependency. Outside one, point cargo at the repository:

```toml
pcs-service = { git = "https://github.com/nassor/pcs" }
```

`pcs-service` re-exports `pcs-core`, so `pcs_service::pipeline::Pipeline` and
`pcs_core::pipeline::Pipeline` are the same type. Depend on `pcs-core` alone
when you want no IO formats, no wasmtime and no HTTP.

</div>

The [core concepts](@/dataset.md) apply to both modes: a guest runs the same
`Pipeline` DAG, just inside a component.
