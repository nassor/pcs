// Stage 4 of the polyglot example, the Kotlin guest.
//
// `wasmWasiMain` holds the WIT bindings and the guest export. The Arrow IPC
// codec is a published dependency, `io.github.nassor:pcs-arrow-ipc`, resolved
// from `mavenLocal()` in an in-repo build: `scripts/build-polyglot.sh` runs
// `gradle publishToMavenLocal` in `packages/arrow-ipc-kt` first. Resolving the
// real artifact, Gradle module metadata plus the `wasmWasi` klib, is what proves
// the publication is consumable.
//
// Gradle produces a core wasm module, not a component. `scripts/build-polyglot.sh`
// runs `wit-bindgen kotlin` before this build and `wasm-tools component embed`
// plus `wasm-tools component new --adapt` after it. See examples/polyglot/PINS.md.

@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl

plugins {
    kotlin("multiplatform") version "2.4.0"
}

repositories {
    mavenLocal()
    mavenCentral()
}

kotlin {
    wasmWasi {
        binaries.executable()
        nodejs()
    }

    sourceSets {
        val wasmWasiMain by getting {
            dependencies {
                implementation("io.github.nassor:pcs-arrow-ipc:0.1.0")
            }
        }
    }
}
