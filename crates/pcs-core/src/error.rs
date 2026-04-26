//! # Error Handling - Clear, Actionable Error Messages
//!
//! This module provides comprehensive error handling for the PCS workflow engine.
//! It includes categorized error types that make it easy to understand what went wrong
//! and how to fix it.
//!
//! ## Design Philosophy
//!
//! PCS's error handling is designed to be:
//! - **Clear**: Error messages explain what happened
//! - **Actionable**: Errors suggest how to fix the problem
//! - **Categorized**: Different error types for different scenarios
//! - **Context-Rich**: Errors include relevant context information
//!
//! ## Quick Start
//!
//! Use `PcsResult<T>` for functions that might fail and return different
//! `PcsError` variants to indicate specific failure types. Handle errors
//! with pattern matching to provide appropriate responses.
//!
//! ## Error Categories
//!
//! | Error Type | When It Occurs | How to Fix |
//! |------------|----------------|------------|
//! | `SystemExecution` | System processing fails | Check your system logic |
//! | `ComponentNotFound` | Entity missing a required component | Ensure component is added before use |
//! | `EntityNotFound` | Entity does not exist or is dead | Verify entity IDs are valid |
//! | `ResourceNotFound` | A required global resource is missing | Register the resource before use |
//! | `Store` | Store operations fail | Check store state and keys |
//! | `Scheduler` | Scheduler orchestration fails | Verify system registration and routing |
//! | `Configuration` | Invalid system/pipeline config | Check parameters and settings |
//! | `RetryExhausted` | All retries failed | Increase retries or fix root cause |
//! | `Generic` | General errors | Check the specific error message |
//!
//! ## Using Errors in Your Systems
//!
//! In your custom systems, return specific error types for different failures.
//! Use `PcsError::SystemExecution` for system phase issues and handle errors
//! appropriately in the pipeline.
//!
//! ## Error Propagation
//!
//! Use the `?` operator to propagate errors. All variants implement `std::error::Error`
//! so they integrate naturally with the broader Rust ecosystem.

/// Comprehensive error type for PCS workflows
///
/// This enum covers all the different ways things can go wrong in PCS pipelines.
/// Each variant is designed to give you clear information about what happened
/// and how to fix it.
///
/// ## Error Categories
///
/// ### SystemExecution
/// Something went wrong in your system's business logic during execution.
///
/// **Common causes:**
/// - Invalid input data
/// - External API failures
/// - Business rule violations
/// - Resource unavailability
///
/// **How to fix:** Check your system's logic and input validation.
///
/// ### ComponentNotFound
/// An entity is missing a required component.
///
/// **Common causes:**
/// - Component was never added to the entity
/// - Component was removed before it was needed
/// - Wrong entity ID used
///
/// **How to fix:** Ensure the component is added to the entity before the system runs.
///
/// ### EntityNotFound
/// The referenced entity does not exist or has been removed.
///
/// **Common causes:**
/// - Entity was despawned
/// - Entity ID was never valid
/// - Stale entity reference
///
/// **How to fix:** Verify entity IDs and check entity lifecycle.
///
/// ### ResourceNotFound
/// A required global resource is not registered.
///
/// **Common causes:**
/// - Resource was never inserted into the pipeline
/// - Resource was removed before use
///
/// **How to fix:** Register the resource before running systems that depend on it.
///
/// ### Store
/// Store operations failed (get/put/remove).
///
/// **Common causes:**
/// - Type mismatches when retrieving data
/// - Store backend issues
/// - Concurrent access problems
///
/// **How to fix:** Check store keys and ensure type consistency.
///
/// ### Scheduler
/// Scheduler orchestration problems.
///
/// **Common causes:**
/// - Unregistered system references
/// - Invalid action routing
/// - Circular dependencies
///
/// **How to fix:** Verify system registration and action string routing.
///
/// ### Configuration
/// Invalid system or pipeline configuration.
///
/// **Common causes:**
/// - Invalid concurrency settings
/// - Negative retry counts
/// - Conflicting settings
///
/// **How to fix:** Review system builder parameters and pipeline setup.
///
/// ### RetryExhausted
/// All retry attempts have been exhausted.
///
/// **Common causes:**
/// - Persistent external failures
/// - Insufficient retry configuration
/// - Systemic issues
///
/// **How to fix:** Increase retry count, fix root cause, or add exponential backoff.
///
/// ## Converting from Other Errors
///
/// PCS errors can be created from various sources including standard library
/// errors, string slices, and owned strings. Use the appropriate constructor
/// method or the `Into` trait for convenient conversion.
#[derive(Debug, Clone)]
pub enum PcsError {
    /// Error during system execution (your business logic)
    ///
    /// Use this when your system encounters an error in its core processing logic.
    /// Include specific details about what went wrong.
    SystemExecution(String),

    /// Entity missing a required component
    ///
    /// Use this when a system tries to access a component that is not present
    /// on the given entity.
    ComponentNotFound { entity_id: u32, type_name: String },

    /// Entity does not exist or is dead
    ///
    /// Use this when an operation references an entity ID that is not valid
    /// or has been despawned.
    EntityNotFound(u32),

    /// A required global resource is missing
    ///
    /// Use this when a system requires a resource that has not been registered
    /// in the pipeline.
    ResourceNotFound(String),

    /// Error in store operations (get/put/remove)
    ///
    /// Use this for store-related failures like missing keys, type mismatches,
    /// or store backend issues.
    Store(String),

    /// Error in scheduler orchestration (routing/registration)
    ///
    /// Use this for scheduler-level problems like unregistered systems,
    /// invalid action routing, or scheduler configuration issues.
    Scheduler(String),

    /// Error in system or pipeline configuration (invalid settings)
    ///
    /// Use this for configuration problems like invalid parameters,
    /// conflicting settings, or constraint violations.
    Configuration(String),

    /// All retry attempts have been exhausted
    ///
    /// This error is automatically generated when a system fails and
    /// all configured retry attempts have been used up.
    RetryExhausted {
        /// The source error from the final failed attempt.
        source: Box<PcsError>,
        /// The total number of attempts that were made.
        attempts: usize,
    },

    /// General-purpose error for other scenarios
    ///
    /// Use this for errors that don't fit the other categories.
    /// Try to be specific in the error message.
    Generic(String),

    /// Error in distributed coordination (partitioning, consensus, networking)
    ///
    /// Use this for failures in distributed batch processing, consensus
    /// operations, or inter-instance communication.
    #[cfg(feature = "distributed")]
    Distributed(String),

    /// Batch lease expired during processing
    ///
    /// This error indicates that the lease on a batch expired before processing
    /// completed. The batch may be reclaimed by another instance.
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

impl PartialEq for PcsError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PcsError::SystemExecution(a), PcsError::SystemExecution(b)) => a == b,
            (
                PcsError::ComponentNotFound {
                    entity_id: eid_a,
                    type_name: tn_a,
                },
                PcsError::ComponentNotFound {
                    entity_id: eid_b,
                    type_name: tn_b,
                },
            ) => eid_a == eid_b && tn_a == tn_b,
            (PcsError::EntityNotFound(a), PcsError::EntityNotFound(b)) => a == b,
            (PcsError::ResourceNotFound(a), PcsError::ResourceNotFound(b)) => a == b,
            (PcsError::Store(a), PcsError::Store(b)) => a == b,
            (PcsError::Scheduler(a), PcsError::Scheduler(b)) => a == b,
            (PcsError::Configuration(a), PcsError::Configuration(b)) => a == b,
            (
                PcsError::RetryExhausted {
                    source: sa,
                    attempts: aa,
                },
                PcsError::RetryExhausted {
                    source: sb,
                    attempts: ab,
                },
            ) => aa == ab && sa == sb,
            (PcsError::Generic(a), PcsError::Generic(b)) => a == b,
            #[cfg(feature = "distributed")]
            (PcsError::Distributed(a), PcsError::Distributed(b)) => a == b,
            #[cfg(feature = "distributed")]
            (PcsError::LeaseExpired { batch_id: a }, PcsError::LeaseExpired { batch_id: b }) => {
                a == b
            }
            _ => false,
        }
    }
}

impl Eq for PcsError {}

// Conversion traits for ergonomic error handling

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

/// Convenient Result type alias for PCS operations
///
/// This type alias wraps the standard `Result<TState, E>` with [`PcsError`] as the error type.
/// It's the recommended return type for all PCS-related functions that can fail.
///
/// ## Why Use PcsResult?
///
/// Instead of writing `Result<String, PcsError>` everywhere, you can use `PcsResult<String>`.
/// This makes your function signatures cleaner and more consistent.
///
/// ## Examples
///
/// Use `PcsResult<TState>` for functions that process data and might fail.
/// Return appropriate `PcsError` variants to indicate specific failure types.
/// Use pattern matching to handle results, or use the `?` operator to
/// propagate errors automatically in functions that return `PcsResult`.
///
/// ## Common Patterns
///
/// ### Converting Other Errors
///
/// Convert from standard library errors using `map_err` and provide
/// descriptive error messages that help with debugging.
///
/// ### Chaining Operations
///
/// Use the `?` operator to chain operations and short-circuit on the first error.
/// This allows you to write clean, readable error-handling code that stops
/// execution as soon as any step fails.
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
