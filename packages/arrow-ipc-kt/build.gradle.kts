// The Arrow IPC codec for PCS WebAssembly guests, published for two targets.
//
// `commonMain` holds the whole codec, so the source that decodes bytes inside a
// `wasmWasi` component is byte for byte the source `jvmTest` exercises against a
// real fixture on the host.

@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl

plugins {
    kotlin("multiplatform") version "2.4.0"
    `maven-publish`
}

group = "io.github.nassor"
version = "0.1.0"

repositories {
    mavenCentral()
}

kotlin {
    jvm()

    // A library, so no `binaries.executable()`: the consumer's build produces
    // the component. `nodejs()` only names the environment the Kotlin plugin
    // requires for a `wasmWasi` target.
    wasmWasi {
        nodejs()
    }

    sourceSets {
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test"))
            }
        }
    }
}

publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name.set("pcs-arrow-ipc")
            description.set("Arrow IPC codec for PCS WebAssembly guests, for wasmWasi and the JVM.")
            url.set("https://github.com/nassor/pcs")
            licenses {
                license {
                    name.set("Apache-2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0")
                }
            }
        }
    }
    repositories {
        maven {
            // The Pages-served Maven repository. `docs/static/` is copied verbatim
            // into the built site, so publishing here makes the artifacts resolvable
            // at https://nassor.github.io/pcs/maven once the commit is on main.
            name = "pages"
            url = uri(layout.projectDirectory.dir("../../docs/static/maven"))
        }
    }
}
