# pcs-arrow-ipc

One Arrow IPC codec, five languages. Each reads and mutates the PCS host to guest
wire format using nothing but its language's standard library, so a WebAssembly
guest can decode a batch without an Arrow dependency that survives its
componentizer.

The format itself is specified in
[the wire format reference](https://nassor.github.io/pcs/reference/wire-format/).
A sixth language reimplements it from there.

## Coordinates

All five ship in lockstep at the version in `VERSION`: one wire format, one
version.

| Language | Directory | Coordinate | Import |
|---|---|---|---|
| Go | `arrow-ipc-go` | `github.com/nassor/pcs/packages/arrow-ipc-go` | `arrowipc` |
| Python | `arrow-ipc-py` | `pcs-arrow-ipc` | `pcs_arrow_ipc` |
| TypeScript | `arrow-ipc-ts` | `@nassor/pcs-arrow-ipc` | `@nassor/pcs-arrow-ipc` |
| Kotlin | `arrow-ipc-kt` | `io.github.nassor:pcs-arrow-ipc` | `io.github.nassor.pcs.arrowipc` |
| C# | `arrow-ipc-cs` | `Pcs.ArrowIpc` | `Pcs.ArrowIpc` |

## Install

Everything but Go is a GitHub Release asset of the `arrow-ipc-v0.1.0` tag. Go
resolves through the module proxy from the `packages/arrow-ipc-go/v0.1.0` tag.

```bash
# Go
go get github.com/nassor/pcs/packages/arrow-ipc-go@v0.1.0

# Python
pip install pcs_arrow_ipc-0.1.0-py3-none-any.whl

# TypeScript
npm install ./nassor-pcs-arrow-ipc-0.1.0.tgz

# C#
dotnet nuget add source <download-dir> -n pcs-local
dotnet add package Pcs.ArrowIpc --version 0.1.0
```

Kotlin resolves from a static Maven repository served by the docs site:

```kotlin
repositories {
    maven("https://nassor.github.io/pcs/maven")
    mavenCentral()
}

dependencies {
    implementation("io.github.nassor:pcs-arrow-ipc:0.1.0")
}
```

## Tests

Every suite reads `examples/polyglot/generated/`, so run the emitter first:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit

cd packages/arrow-ipc-go && go test ./...
cd packages/arrow-ipc-py && PYTHONPATH=src python -m unittest discover -s tests
cd packages/arrow-ipc-ts && npm ci && npm run typecheck && npm run build && npm test
cd packages/arrow-ipc-kt && gradle jvmTest
cd packages/arrow-ipc-cs && dotnet test tests
```

## Release

1. Bump `VERSION`, the four manifests that carry a version
   (`arrow-ipc-py/pyproject.toml`, `arrow-ipc-ts/package.json`,
   `arrow-ipc-kt/build.gradle.kts`, `arrow-ipc-cs/Pcs.ArrowIpc.csproj`) and the
   Kotlin stage's `implementation("io.github.nassor:pcs-arrow-ipc:...")` line in
   `examples/polyglot/stages/kotlin-fee/build.gradle.kts`. Go carries no manifest
   version: its version is the git tag.
2. `bash scripts/pack-arrow-ipc.sh`. It asserts the four manifests agree with
   `VERSION`, builds every artifact into `target/arrow-ipc-dist/`, and writes the
   Kotlin publication into `docs/static/maven/`.
3. Commit, including `docs/static/maven/**`, and push to `main`. That is what
   makes the Maven repository live: `docs.yml` redeploys the Pages site and Zola
   copies `docs/static/` verbatim.
4. Tag `arrow-ipc-v<version>` and push the tag. `release-arrow-ipc.yml` packs
   again, creates the release with the assets, and pushes the
   `packages/arrow-ipc-go/v<version>` tag the Go module proxy serves.

## License

This subtree is Apache-2.0, per `LICENSE-APACHE`. The rest of the repository, the
engine crates included, is AGPL-3.0-only.
