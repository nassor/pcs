//! # Pipeline Runtime Loader
//!
//! Resolves a [`WasmSpec`] to a [`WasmPipelineRuntime`] at service startup.
//!
//! ## Responsibilities
//!
//! 1. Read the `.wasm` bytes via a [`ModuleResolver`].
//! 2. Verify the optional SHA-256 digest *before* JIT compilation.
//! 3. Compile + instantiate the component via [`WasmPipelineRuntime::from_bytes`].
//! 4. Call `describe()` eagerly to pre-populate the component-name cache and
//!    surface guest errors at startup rather than at first batch.

use std::collections::HashMap;
use std::path::Path;

use pcs_core::PcsResult;
use pcs_core::error::PcsError;

use crate::service::config::WasmSpec;
use crate::wasm::{WasmEngine, WasmPipelineRuntime};

// ── ModuleResolver ────────────────────────────────────────────────────────────

/// Reads raw WASM bytes given a module path string.
///
/// The default implementation ([`LocalModuleResolver`]) reads from the local
/// filesystem. Tests can substitute a resolver that serves fixtures from memory.
pub trait ModuleResolver: Send + Sync {
    /// Return the raw WASM component bytes for `module_path`.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if the bytes cannot be read.
    fn resolve(&self, module_path: &str) -> PcsResult<Vec<u8>>;
}

// ── LocalModuleResolver ───────────────────────────────────────────────────────

/// Reads WASM bytes from the local filesystem.
pub struct LocalModuleResolver {
    /// Optional base directory. When set, relative `module_path` values are
    /// resolved against it. Absolute paths ignore `base_dir`.
    pub base_dir: Option<std::path::PathBuf>,
}

impl LocalModuleResolver {
    /// Create a resolver with no base directory (paths are used as-is).
    pub fn new() -> Self {
        Self { base_dir: None }
    }

    /// Create a resolver that resolves relative paths under `base_dir`.
    pub fn with_base_dir(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: Some(base_dir.into()),
        }
    }

    fn full_path(&self, module_path: &str) -> std::path::PathBuf {
        let p = Path::new(module_path);
        if p.is_absolute() {
            p.to_path_buf()
        } else if let Some(ref base) = self.base_dir {
            base.join(p)
        } else {
            p.to_path_buf()
        }
    }
}

impl Default for LocalModuleResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ModuleResolver for LocalModuleResolver {
    fn resolve(&self, module_path: &str) -> PcsResult<Vec<u8>> {
        let path = self.full_path(module_path);
        std::fs::read(&path).map_err(|e| {
            PcsError::configuration(format!("reading wasm module '{}': {e}", path.display()))
        })
    }
}

// ── SHA3-256 helpers ──────────────────────────────────────────────────────────

fn sha3_256_hex(bytes: &[u8]) -> String {
    use sha3::{Digest, Sha3_256};
    let hash = Sha3_256::digest(bytes);
    use std::fmt::Write;
    let mut out = String::with_capacity(64);
    for b in hash.iter() {
        write!(out, "{b:02x}").unwrap();
    }
    out
}

fn verify_sha3_256(bytes: &[u8], expected: &str) -> PcsResult<()> {
    // Strip optional "sha3-256:" prefix that some tooling adds.
    let expected = expected.strip_prefix("sha3-256:").unwrap_or(expected);
    let actual = sha3_256_hex(bytes);
    if actual != expected {
        return Err(PcsError::configuration(format!(
            "wasm module SHA3-256 mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

// ── PipelineRuntimeLoader ────────────────────────────────────────────────────

/// Loads a [`WasmPipelineRuntime`] from a [`WasmSpec`] using the supplied
/// engine and module resolver.
///
/// ```no_run
/// # #[cfg(all(feature = "service", feature = "wasm"))]
/// # {
/// use pcs_service::service::config::WasmSpec;
/// use pcs_service::service::loader::{LocalModuleResolver, PipelineRuntimeLoader};
/// use pcs_service::wasm::WasmEngine;
///
/// let engine = WasmEngine::new().unwrap();
/// let resolver = LocalModuleResolver::with_base_dir("/opt/pcs/pipelines");
/// let loader = PipelineRuntimeLoader::new(engine, resolver);
///
/// // WasmSpec comes from ServiceConfig::pipeline.wasm (deserialized from TOML).
/// # let spec = WasmSpec { module: "transform.wasm".into(), sha3_256: None, watch: false,
/// #     config: Default::default() };
/// let runtime = loader.load("my-pipeline", &spec).unwrap();
/// # }
/// ```
pub struct PipelineRuntimeLoader<R = LocalModuleResolver> {
    engine: WasmEngine,
    resolver: R,
    /// Epoch ticks before a single WASM call is interrupted.
    epoch_deadline: u64,
}

impl<R: ModuleResolver> PipelineRuntimeLoader<R> {
    /// Default epoch deadline (100 ticks × 100 ms/tick = 10 s).
    const DEFAULT_EPOCH_DEADLINE: u64 = 100;

    /// Create a loader with the given engine and resolver.
    pub fn new(engine: WasmEngine, resolver: R) -> Self {
        Self {
            engine,
            resolver,
            epoch_deadline: Self::DEFAULT_EPOCH_DEADLINE,
        }
    }

    /// Override the epoch deadline (in ticks) for timeout enforcement.
    pub fn with_epoch_deadline(mut self, ticks: u64) -> Self {
        self.epoch_deadline = ticks;
        self
    }

    /// Resolve, verify, compile, and describe the WASM module.
    ///
    /// The pipeline name is used as the runtime's identifier in logs and
    /// metrics — typically derived from the `[pipeline]` table in the config.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] for IO / digest / compile failures.
    /// Returns [`PcsError::SystemExecution`] if the guest's `describe()` call
    /// fails on the first instantiation.
    pub fn load(&self, pipeline_name: &str, spec: &WasmSpec) -> PcsResult<WasmPipelineRuntime> {
        // 1. Read bytes — fail fast before paying JIT cost.
        let bytes = self.resolver.resolve(&spec.module)?;

        // 2. SHA3-256 check before compilation.
        if let Some(ref expected) = spec.sha3_256 {
            verify_sha3_256(&bytes, expected)?;
        }

        // 3. Compile + instantiate.
        let config: HashMap<String, String> = spec.config.clone();
        let runtime = WasmPipelineRuntime::from_bytes(
            self.engine.clone(),
            pipeline_name.to_string(),
            &bytes,
            config,
            self.epoch_deadline,
        )?;

        // 4. Eagerly call describe() to populate the component-name cache and
        //    surface guest errors at startup.
        runtime.describe().map_err(|e| {
            PcsError::configuration(format!(
                "wasm module '{}' describe() failed: {e}",
                spec.module
            ))
        })?;

        Ok(runtime)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "service", feature = "wasm"))]
mod tests {
    use super::*;

    // ── InMemoryResolver ──────────────────────────────────────────────────────

    struct InMemoryResolver {
        bytes: Vec<u8>,
    }
    impl InMemoryResolver {
        fn new(bytes: Vec<u8>) -> Self {
            Self { bytes }
        }
    }
    impl ModuleResolver for InMemoryResolver {
        fn resolve(&self, _module_path: &str) -> PcsResult<Vec<u8>> {
            Ok(self.bytes.clone())
        }
    }

    struct FailingResolver;
    impl ModuleResolver for FailingResolver {
        fn resolve(&self, module_path: &str) -> PcsResult<Vec<u8>> {
            Err(PcsError::configuration(format!(
                "simulated IO failure for {module_path}"
            )))
        }
    }

    // ── Test: resolver IO failure propagates ─────────────────────────────────

    #[tokio::test]
    async fn test_io_failure_propagates() {
        let engine = WasmEngine::new().expect("engine");
        let loader = PipelineRuntimeLoader::new(engine, FailingResolver);
        let spec = WasmSpec {
            module: "missing.wasm".to_string(),
            sha3_256: None,
            watch: false,
            config: HashMap::new(),
        };
        let err = loader.load("test", &spec).err().expect("expected error");
        assert!(
            err.to_string().contains("simulated IO failure"),
            "unexpected error: {err}"
        );
    }

    // ── Test: invalid wasm bytes fail at compile ──────────────────────────────

    #[tokio::test]
    async fn test_invalid_wasm_bytes_rejected() {
        let engine = WasmEngine::new().expect("engine");
        let loader =
            PipelineRuntimeLoader::new(engine, InMemoryResolver::new(b"not-wasm".to_vec()));
        let spec = WasmSpec {
            module: "bad.wasm".to_string(),
            sha3_256: None,
            watch: false,
            config: HashMap::new(),
        };
        let err = loader.load("test", &spec).err().expect("expected error");
        assert!(
            err.to_string().contains("wasm compile error") || err.category() == "configuration",
            "unexpected error: {err}"
        );
    }

    // ── Test: LocalModuleResolver returns error for missing file ──────────────

    #[test]
    fn test_local_resolver_missing_file() {
        let resolver = LocalModuleResolver::new();
        let err = resolver
            .resolve("/nonexistent/path/pipeline.wasm")
            .unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.to_string().contains("reading wasm module"),
            "unexpected error: {err}"
        );
    }

    // ── Test: LocalModuleResolver resolves relative path under base_dir ───────

    #[test]
    fn test_local_resolver_base_dir() {
        use std::io::Write;
        use tempfile::NamedTempFile;
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(b"dummy").expect("write");
        let dir = f.path().parent().unwrap().to_path_buf();
        let filename = f.path().file_name().unwrap().to_str().unwrap().to_string();

        let resolver = LocalModuleResolver::with_base_dir(&dir);
        let bytes = resolver.resolve(&filename).expect("resolve");
        assert_eq!(bytes, b"dummy");
    }

    // ── Test: LocalModuleResolver with absolute path ignores base_dir ─────────

    #[test]
    fn test_local_resolver_absolute_path_ignores_base_dir() {
        use std::io::Write;
        use tempfile::NamedTempFile;
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(b"abs").expect("write");
        let abs_path = f.path().to_str().unwrap().to_string();

        let resolver = LocalModuleResolver::with_base_dir("/some/other/dir");
        let bytes = resolver.resolve(&abs_path).expect("resolve");
        assert_eq!(bytes, b"abs");
    }
}
