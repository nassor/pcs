# Toolchain pins — polyglot example

Four guest components, four languages, four componentization toolchains. These
are the versions CI installs in the `Polyglot Guests` job and the versions the
committed build script is written against. `crates/pcs-guest/PINS.md` covers the
Rust-side and host-side pins; this file covers everything the non-Rust stages
need.

## Required tools

| Tool | Version | Language runtime | Install |
| ---- | ------- | ---------------- | ------- |
| `cargo-component`  | 0.21.1 | Rust 1.95.0                | `cargo install cargo-component --locked --version 0.21.1` |
| `wasm-tools`       | 1.246.2 | —                         | `cargo install wasm-tools --locked --version 1.246.2` |
| Rust target        | `wasm32-wasip2` | —                 | `rustup target add wasm32-wasip2` |
| `componentize-go`  | 0.4.1  | Go 1.25.5+ (verified on 1.26.3) | `go install github.com/bytecodealliance/componentize-go@v0.4.1` |
| `componentize-py`  | 0.25.0 | Python 3.10+ (verified on 3.14) | `pip install componentize-py==0.25.0` |
| `@bytecodealliance/jco` | 1.30.0 | Node 22+ (verified on 24.19) | `npm install -g @bytecodealliance/jco@1.30.0` |

Build everything with `bash scripts/build-polyglot.sh`; it fails with a distinct
exit code per missing tool (2 cargo-component, 3 wasm-tools, 4 Go,
5 componentize-go, 6 componentize-py, 7 Node/npm).

## Why these three and not others

They are the languages with maintained, single-command WASI 0.2 component
toolchains. `componentize-go` is the Bytecode Alliance's current Go
recommendation — the TinyGo component page now carries a "not currently being
maintained" banner pointing at it. C via `wasi-sdk` + `wit-bindgen c` is the next
cheapest addition; the wire format is documented language-neutrally in
`docs/content/polyglot/wire-format.md`, so a fifth stage needs no host changes.

## Known caveats

- **`componentize-go` on Windows.** `go install` puts a thin wrapper on your
  PATH that downloads the real binary on first use — and it asks for
  `componentize-go-windows-amd64.tar.gz`, while the release only publishes
  `componentize-go-windows-amd64.zip`. The wrapper 404s. Workaround: download
  that `.zip` from the v0.4.1 release page and put `componentize-go.exe` on your
  PATH (overwriting the wrapper in `%GOPATH%\bin` is fine). Linux and macOS are
  unaffected, which is why CI does not need this.
- **`componentize-go bindings` owns `go.mod`.** It rewrites the module line to
  `module wit_component` every time it runs. That is why
  `examples/polyglot/stages/go-validate/go.mod` is committed with that name and
  why intra-module imports read `wit_component/arrowipc`. Do not "fix" it.
- **Go native tests must be scoped.** `go test ./...` fails: the generated
  binding packages use `//go:wasmimport`, which does not compile for the host
  target. Use `go test ./arrowipc/...`.
- **componentize-py's `bindings` output is IDE stubs only.** `componentize`
  regenerates the real bindings itself and never reads the files on disk; the
  build succeeds with them deleted. Generating them is worth it for type
  checking, but the step is not load-bearing.
- **componentize-py resolves imports at build time only.** Every `import` in the
  Python stage must be at module top level. A function-local import works when
  you run the file with CPython and then fails inside the component.
- **`python -m unittest discover` breaks after the bindings step.** The generated
  `componentize_py_async_support/` package imports `componentize_py_runtime`,
  which only exists inside the component, and discovery imports every package it
  finds. Name the test module: `python -m unittest test_arrow_ipc`.
- **jco needs ES modules, a versioned import specifier, `--bundle`, and two
  disable flags.** `"type": "module"` in `package.json`; the host-io import must
  be `'pcs:pipeline/host-io@0.2.0'` (the unversioned form fails at wizer time);
  `--bundle` is mandatory as soon as the guest imports another file of its own,
  because StarlingMonkey's loader cannot resolve relative modules at wizer time;
  and `--disable http` must be paired with `--disable fetch-event`, or the
  component still imports `wasi:http/types` and refuses to instantiate against
  the PCS host. Do not disable `clocks`: `Date.now()` silently returns garbage
  and the stage reports timing in `run-metrics`.
- **cargo-component output path.** A `--target wasm32-wasip2` build lands in
  `target/wasm32-wasip1/release/`. Expected — cargo-component compiles the core
  module for wasip1 and adapts it into a component, keeping the pre-adapter
  directory name.

## Load-bearing crate pin

`arrow-ipc = "=59.2.0"` in the workspace `Cargo.toml` is the host's Arrow IPC
implementation, and the byte layout the three hand-rolled codecs target. A patch
bump there can change the buffer layout the Go/Python/JavaScript stages walk. The
`polyglot_chain` integration test is the regression gate: it asserts exact
per-column values produced by all four guests. See
`crates/pcs-guest/PINS.md` for the full upgrade policy.
