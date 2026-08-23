//! Error types for the PCS engine.
//!
//! [`PcsError`] is the single error enum and [`PcsResult`] the matching
//! `Result` alias. Every variant implements `std::error::Error`, so `?`
//! propagates them into other error types.
//!
//! | Error type | When it occurs | Remedy |
//! |------------|----------------|--------|
//! | `SystemExecution` | System processing fails | Check the system logic |
//! | `ComponentNotFound` | Entity missing a component | Add the component first |
//! | `EntityNotFound` | Entity does not exist or is dead | Verify entity IDs |
//! | `ResourceNotFound` | A global resource is missing | Register the resource |
//! | `Store` | Store operations fail | Check store state and keys |
//! | `Scheduler` | Scheduler orchestration fails | Verify system registration |
//! | `Configuration` | Invalid system or pipeline config | Check the parameters |
//! | `RetryExhausted` | All retries failed | Raise the budget or fix the cause |
//! | `Generic` | Everything else | Read the error message |

/// Error type for PCS workflows.
///
/// Each variant carries enough context to say what happened; the table in the
/// [module docs](self) maps every variant to its cause and remedy.
///
/// `From` impls exist for `&str`, `String`, `std::io::Error`, and
/// `Box<dyn std::error::Error>`, so `?` works against those sources directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PcsError {
    /// Failure inside a system's own processing logic.
    SystemExecution(String),

    /// A system accessed a component the entity does not have.
    ComponentNotFound { entity_id: u32, type_name: String },

    /// The referenced entity does not exist or has been despawned.
    EntityNotFound(u32),

    /// A required global resource was never registered.
    ResourceNotFound(String),

    /// Store operation failed: missing key, type mismatch, backend error.
    Store(String),

    /// Scheduler orchestration failed: unregistered system, invalid routing.
    Scheduler(String),

    /// Invalid system or pipeline configuration.
    Configuration(String),

    /// Every configured retry attempt has been used up.
    RetryExhausted {
        /// The source error from the final failed attempt.
        source: Box<PcsError>,
        /// The total number of attempts that were made.
        attempts: usize,
    },

    /// General-purpose error for cases outside the other categories.
    Generic(String),

    /// Distributed coordination failure: partitioning, consensus, networking.
    #[cfg(feature = "distributed")]
    Distributed(String),

    /// A batch lease expired before processing completed. Another instance may
    /// reclaim the batch.
    #[cfg(feature = "distributed")]
    LeaseExpired {
        /// The batch whose lease expired.
        batch_id: u64,
    },
}

impl PcsError {
    /// Create a new system execution error
    pub fn system_execution<S: Into<String>>(msg: S) -> Self {
        PcsError::SystemExecution(msg.into())
    }

    /// Create a new component not found error
    pub fn component_not_found(entity_id: u32, type_name: &str) -> Self {
        PcsError::ComponentNotFound {
            entity_id,
            type_name: type_name.to_string(),
        }
    }

    /// Create a new entity not found error
    pub fn entity_not_found(entity_id: u32) -> Self {
        PcsError::EntityNotFound(entity_id)
    }

    /// Create a new resource not found error
    pub fn resource_not_found<S: Into<String>>(name: S) -> Self {
        PcsError::ResourceNotFound(name.into())
    }

    /// Create a new store error
    pub fn store<S: Into<String>>(msg: S) -> Self {
        PcsError::Store(msg.into())
    }

    /// Create a new scheduler error
    pub fn scheduler<S: Into<String>>(msg: S) -> Self {
        PcsError::Scheduler(msg.into())
    }

    /// Create a new configuration error
    pub fn configuration<S: Into<String>>(msg: S) -> Self {
        PcsError::Configuration(msg.into())
    }

    /// Create a new retry exhausted error
    pub fn retry_exhausted(source: PcsError, attempts: usize) -> Self {
        PcsError::RetryExhausted {
            source: Box::new(source),
            attempts,
        }
    }

    /// Create a new generic error
    pub fn generic<S: Into<String>>(msg: S) -> Self {
        PcsError::Generic(msg.into())
    }

    /// Create a new distributed error
    #[cfg(feature = "distributed")]
    pub fn distributed<S: Into<String>>(msg: S) -> Self {
        PcsError::Distributed(msg.into())
    }

    /// Create a new lease expired error
    #[cfg(feature = "distributed")]
    pub fn lease_expired(batch_id: u64) -> Self {
        PcsError::LeaseExpired { batch_id }
    }

    /// Get the error message as a string
    pub fn message(&self) -> String {
        match self {
            PcsError::SystemExecution(msg) => msg.clone(),
            PcsError::ComponentNotFound { type_name, .. } => type_name.clone(),
            PcsError::EntityNotFound(id) => id.to_string(),
            PcsError::ResourceNotFound(name) => name.clone(),
            PcsError::Store(msg) => msg.clone(),
            PcsError::Scheduler(msg) => msg.clone(),
            PcsError::Configuration(msg) => msg.clone(),
            PcsError::RetryExhausted { source, attempts } => {
                format!("after {attempts} attempt(s): {source}")
            }
            PcsError::Generic(msg) => msg.clone(),
            #[cfg(feature = "distributed")]
            PcsError::Distributed(msg) => msg.clone(),
            #[cfg(feature = "distributed")]
            PcsError::LeaseExpired { batch_id } => {
                format!("lease expired for batch {batch_id}")
            }
        }
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            PcsError::SystemExecution(_) => "system_execution",
            PcsError::ComponentNotFound { .. } => "component_not_found",
            PcsError::EntityNotFound(_) => "entity_not_found",
            PcsError::ResourceNotFound(_) => "resource_not_found",
            PcsError::Store(_) => "store",
            PcsError::Scheduler(_) => "scheduler",
            PcsError::Configuration(_) => "configuration",
            PcsError::RetryExhausted { .. } => "retry_exhausted",
            PcsError::Generic(_) => "generic",
            #[cfg(feature = "distributed")]
            PcsError::Distributed(_) => "distributed",
            #[cfg(feature = "distributed")]
            PcsError::LeaseExpired { .. } => "lease_expired",
        }
    }
}

impl std::fmt::Display for PcsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PcsError::SystemExecution(msg) => write!(f, "System execution error: {msg}"),
            PcsError::ComponentNotFound {
                entity_id,
                type_name,
            } => write!(
                f,
                "Component not found: entity {entity_id} missing {type_name}"
            ),
            PcsError::EntityNotFound(id) => write!(f, "Entity not found: {id}"),
            PcsError::ResourceNotFound(name) => write!(f, "Resource not found: {name}"),
            PcsError::Store(msg) => write!(f, "Store error: {msg}"),
            PcsError::Scheduler(msg) => write!(f, "Scheduler error: {msg}"),
            PcsError::Configuration(msg) => write!(f, "Configuration error: {msg}"),
            PcsError::RetryExhausted { source, attempts } => {
                write!(f, "Retry exhausted after {attempts} attempt(s): {source}")
            }
            PcsError::Generic(msg) => write!(f, "Error: {msg}"),
            #[cfg(feature = "distributed")]
            PcsError::Distributed(msg) => write!(f, "Distributed error: {msg}"),
            #[cfg(feature = "distributed")]
            PcsError::LeaseExpired { batch_id } => {
                write!(f, "Lease expired for batch {batch_id}")
            }
        }
    }
}

impl std::error::Error for PcsError {}

impl From<Box<dyn std::error::Error + Send + Sync>> for PcsError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        PcsError::Generic(err.to_string())
    }
}

impl From<&str> for PcsError {
    fn from(err: &str) -> Self {
        PcsError::Generic(err.to_string())
    }
}

impl From<String> for PcsError {
    fn from(err: String) -> Self {
        PcsError::Generic(err)
    }
}

impl From<std::io::Error> for PcsError {
    fn from(err: std::io::Error) -> Self {
        PcsError::Generic(format!("IO error: {err}"))
    }
}

/// `Result` alias with [`PcsError`] as the error type.
pub type PcsResult<TState> = Result<TState, PcsError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_execution_error_creation() {
        let error = PcsError::system_execution("system failed");
        assert_eq!(error.message(), "system failed");
        assert_eq!(error.category(), "system_execution");
    }

    #[test]
    fn test_system_execution_display() {
        let error = PcsError::SystemExecution("bad state".to_string());
        assert_eq!(format!("{error}"), "System execution error: bad state");
    }

    #[test]
    fn test_component_not_found_error_creation() {
        let error = PcsError::component_not_found(42, "Health");
        assert_eq!(error.category(), "component_not_found");
        assert_eq!(
            format!("{error}"),
            "Component not found: entity 42 missing Health"
        );
    }

    #[test]
    fn test_component_not_found_message_contains_type_name() {
        let error = PcsError::component_not_found(1, "Transform");
        assert!(error.message().contains("Transform"));
    }

    #[test]
    fn test_entity_not_found_error_creation() {
        let error = PcsError::entity_not_found(99);
        assert_eq!(error.category(), "entity_not_found");
        assert_eq!(format!("{error}"), "Entity not found: 99");
    }

    #[test]
    fn test_entity_not_found_message_contains_id() {
        let error = PcsError::entity_not_found(7);
        assert!(error.message().contains("7"));
    }

    #[test]
    fn test_resource_not_found_error_creation() {
        let error = PcsError::resource_not_found("GameConfig");
        assert_eq!(error.category(), "resource_not_found");
        assert_eq!(format!("{error}"), "Resource not found: GameConfig");
    }

    #[test]
    fn test_scheduler_error_creation() {
        let error = PcsError::scheduler("missing system");
        assert_eq!(error.message(), "missing system");
        assert_eq!(error.category(), "scheduler");
    }

    #[test]
    fn test_scheduler_display() {
        let error = PcsError::Scheduler("cycle detected".to_string());
        assert_eq!(format!("{error}"), "Scheduler error: cycle detected");
    }

    #[test]
    fn test_error_conversions() {
        let error1: PcsError = "Test error".into();
        let error2: PcsError = "Test error".to_string().into();

        match (&error1, &error2) {
            (PcsError::Generic(msg1), PcsError::Generic(msg2)) => {
                assert_eq!(msg1, msg2);
            }
            _ => panic!("Expected Generic errors"),
        }
    }

    #[test]
    fn test_error_categories() {
        assert_eq!(
            PcsError::SystemExecution("".to_string()).category(),
            "system_execution"
        );
        assert_eq!(
            PcsError::ComponentNotFound {
                entity_id: 0,
                type_name: "".to_string()
            }
            .category(),
            "component_not_found"
        );
        assert_eq!(PcsError::EntityNotFound(0).category(), "entity_not_found");
        assert_eq!(
            PcsError::ResourceNotFound("".to_string()).category(),
            "resource_not_found"
        );
        assert_eq!(PcsError::store("").category(), "store");
        assert_eq!(PcsError::Scheduler("".to_string()).category(), "scheduler");
        assert_eq!(PcsError::configuration("").category(), "configuration");
        assert_eq!(
            PcsError::retry_exhausted(PcsError::generic(""), 0).category(),
            "retry_exhausted"
        );
        assert_eq!(PcsError::generic("").category(), "generic");
    }

    #[test]
    fn test_io_error_maps_to_generic() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let pcs_err: PcsError = io_err.into();
        assert_eq!(pcs_err.category(), "generic");
        assert!(pcs_err.message().contains("IO error"));
        assert!(pcs_err.message().contains("file missing"));
    }

    #[test]
    fn test_partial_eq_system_execution_same_message() {
        let a = PcsError::SystemExecution("oops".to_string());
        let b = PcsError::SystemExecution("oops".to_string());
        assert_eq!(a, b);
    }

    #[test]
    fn test_partial_eq_system_execution_different_message() {
        let a = PcsError::SystemExecution("a".to_string());
        let b = PcsError::SystemExecution("b".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_component_not_found() {
        let a = PcsError::ComponentNotFound {
            entity_id: 1,
            type_name: "Health".to_string(),
        };
        let b = PcsError::ComponentNotFound {
            entity_id: 1,
            type_name: "Health".to_string(),
        };
        let c = PcsError::ComponentNotFound {
            entity_id: 2,
            type_name: "Health".to_string(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_partial_eq_entity_not_found() {
        let a = PcsError::EntityNotFound(5);
        let b = PcsError::EntityNotFound(5);
        let c = PcsError::EntityNotFound(6);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_partial_eq_different_variants() {
        let a = PcsError::SystemExecution("msg".to_string());
        let b = PcsError::Scheduler("msg".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_resource_not_found() {
        let a = PcsError::ResourceNotFound("Config".to_string());
        let b = PcsError::ResourceNotFound("Config".to_string());
        let c = PcsError::ResourceNotFound("Other".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
