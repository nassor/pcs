# pcs-sdk

One package per language. Each SDK is the zero-ceremony processor authoring
package for its language, and each carries the Arrow IPC codec that reads and
mutates the PCS host to processor wire format using only the language's
standard library. A WebAssembly processor can decode a batch without an Arrow
dependency that survives its componentizer: the codec is internal to the SDK,
which keeps the package's original module and namespace name.

The format itself is specified in
[the wire format reference](https://nassor.github.io/pcs/reference/wire-format/).
A sixth language can reimplement it from there.

## Coordinates

All five ship in lockstep at the version in `VERSION`: one wire format, one
version. The Kotlin KSP symbol processor (`pcs-sdk-kt-ksp`) and the C# source
generator (`Pcs.Sdk.Generators`) are build-time companions packed with their
runtime, not additional runtime packages.

| Language | Directory | Coordinate | Codec import |
|---|---|---|---|
| Go | `pcs-sdk-go` | `github.com/nassor/pcs/packages/pcs-sdk-go` | subpackage `arrowipc` |
| Python | `pcs-sdk-py` | `pcs-sdk` | `pcs_sdk.arrow_ipc` |
| TypeScript | `pcs-sdk-ts` | `@nassor/pcs-sdk` | `./arrow_ipc.ts` (internal) |
| Kotlin | `pcs-sdk-kt` (+ KSP `pcs-sdk-kt-ksp`) | `io.github.nassor:pcs-sdk-kt` | `io.github.nassor.pcs.arrowipc` |
| C# | `pcs-sdk-cs` (+ generator `Pcs.Sdk.Generators`) | `Pcs.Sdk` | `Pcs.ArrowIpc` |

## Install

Everything but Go is a GitHub Release asset of the `sdk-v0.1.0` tag. Go
resolves through the module proxy from the `packages/pcs-sdk-go/v0.1.0` tag.

```bash
# Go
go get github.com/nassor/pcs/packages/pcs-sdk-go@v0.1.0

# Python
pip install pcs_sdk-0.1.0-py3-none-any.whl

# TypeScript
npm install ./nassor-pcs-sdk-0.1.0.tgz

# C#
dotnet nuget add source <download-dir> -n pcs-local
dotnet add package Pcs.Sdk --version 0.1.0
```

Kotlin resolves from a static Maven repository served by the docs site, with
the KSP processor alongside the runtime:

```kotlin
repositories {
    maven("https://nassor.github.io/pcs/maven")
    mavenCentral()
}

dependencies {
    implementation("io.github.nassor:pcs-sdk-kt:0.1.0")
    add("kspWasmWasi", "io.github.nassor:pcs-sdk-kt-ksp:0.1.0")
}
```

## Tests

Every suite reads `examples/polyglot/generated/`, so run the emitter first. It
runs the same on Linux, macOS and Windows (PowerShell), from the repository
root:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
cargo run -p pcs-service --features wasm --example polyglot_schema_emit -- emit
```

The five suites differ per shell. Linux/macOS:

```bash
cd packages/pcs-sdk-go && go test ./...
cd packages/pcs-sdk-py && PYTHONPATH=src python -m unittest discover -s tests
cd packages/pcs-sdk-ts && npm ci && npm run typecheck && npm run build && npm test
cd packages/pcs-sdk-kt && gradle jvmTest
cd packages/pcs-sdk-cs && dotnet test tests
```

Windows (PowerShell):

```powershell
cd packages\pcs-sdk-go; go test ./...
$env:PYTHONPATH = "src"; cd packages\pcs-sdk-py; python -m unittest discover -s tests
cd packages\pcs-sdk-ts; npm ci; npm run typecheck; npm run build; npm test
cd packages\pcs-sdk-kt; gradle jvmTest
cd packages\pcs-sdk-cs; dotnet test tests
```

Each suite covers the SDK and its absorbed codec, including the shared
conformance corpus at `packages/arrow-ipc-conformance/`.

## Release

1. Bump `VERSION`, the five manifests that carry a version
   (`pcs-sdk-py/pyproject.toml`, `pcs-sdk-ts/package.json`,
   `pcs-sdk-kt/build.gradle.kts`, `pcs-sdk-kt-ksp/build.gradle.kts`,
   `pcs-sdk-cs/Pcs.Sdk.csproj`) and the Kotlin stage's
   `implementation("io.github.nassor:pcs-sdk-kt:...")` and
   `add("kspWasmWasi", "io.github.nassor:pcs-sdk-kt-ksp:...")` lines in
   `examples/polyglot/stages/kotlin-fee/build.gradle.kts`. Go carries no manifest
   version: its version is the git tag.
2. `cargo xtask pack-sdk`. It asserts the version declarations agree with
   `VERSION`, builds every artifact into `target/arrow-ipc-dist/`, and writes the
   Kotlin publications into `docs/static/maven/`.
3. Commit, including `docs/static/maven/**`, and push to `main`. That is what
   makes the Maven repository live: `docs.yml` redeploys the Pages site and Zola
   copies `docs/static/` verbatim.
4. Tag `sdk-v<version>` and push the tag. `release-sdk.yml` packs again, creates
   the release with the assets, and pushes the `packages/pcs-sdk-go/v<version>`
   tag the Go module proxy serves.

## License

This subtree is Apache-2.0, per `LICENSE-APACHE`. The rest of the repository, the
engine crates included, is AGPL-3.0-only.
