//! Blank-node skolemization (RDF 1.1 Concepts §3.5).
//!
//! A blank node label such as `_:b0` is only meaningful inside the document
//! that contains it. Hashing the label alone would make `_:b0` in two
//! unrelated imports the same node, so every import runs inside an
//! [`ImportScope`] that mints a globally unique skolem IRI per label:
//!
//! ```text
//! {base}/.well-known/genid/{import_id}/{label}
//! ```
//!
//! The skolem IRI is mapped to a [`NodeId`] with [`NodeId::from_iri`], so the
//! same `(import_id, label)` always yields the same node — re-running an
//! import with the same `import_id` is idempotent.

use uuid::Uuid;

use crate::id::NodeId;

/// Base used when the operator hasn't configured one. `.invalid` is reserved
/// (RFC 2606), so these IRIs can never collide with a real host.
pub const DEFAULT_SKOLEM_BASE: &str = "https://polargraph.invalid";

/// Path segment marking a skolem IRI, per RDF 1.1.
pub const GENID_SEGMENT: &str = "/.well-known/genid/";

/// Scope in which blank-node labels are unique: one import call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportScope {
    base: String,
    import_id: String,
}

impl ImportScope {
    /// A scope with a caller-supplied `import_id` (for idempotent re-imports).
    ///
    /// `base` is an absolute IRI prefix such as `https://kb.example.com`; a
    /// trailing `/` is ignored.
    pub fn new(base: &str, import_id: impl Into<String>) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            import_id: import_id.into(),
        }
    }

    /// A scope with a freshly generated UUIDv7 `import_id`.
    pub fn fresh(base: &str) -> Self {
        Self::new(base, Uuid::now_v7().to_string())
    }

    pub fn import_id(&self) -> &str {
        &self.import_id
    }

    /// The skolem IRI for a blank-node label (without the `_:` prefix).
    pub fn skolem_iri(&self, label: &str) -> String {
        format!("{}{}{}/{}", self.base, GENID_SEGMENT, self.import_id, label)
    }

    /// The `NodeId` for a blank-node label in this scope.
    pub fn bnode_node_id(&self, label: &str) -> NodeId {
        NodeId::from_iri(&self.skolem_iri(label))
    }
}

/// Returns true if `iri` is a skolem IRI minted by an [`ImportScope`]
/// (or any other RDF 1.1-conformant skolemizer).
pub fn is_skolem_iri(iri: &str) -> bool {
    iri.contains(GENID_SEGMENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_label_in_different_imports_is_distinct() {
        let a = ImportScope::fresh(DEFAULT_SKOLEM_BASE);
        let b = ImportScope::fresh(DEFAULT_SKOLEM_BASE);
        assert_ne!(a.bnode_node_id("b0"), b.bnode_node_id("b0"));
    }

    #[test]
    fn same_import_id_is_idempotent() {
        let a = ImportScope::new("https://kb.example.com/", "run-1");
        let b = ImportScope::new("https://kb.example.com", "run-1");
        assert_eq!(a.bnode_node_id("b0"), b.bnode_node_id("b0"));
        assert_ne!(a.bnode_node_id("b0"), a.bnode_node_id("b1"));
    }

    #[test]
    fn skolem_iri_shape() {
        let s = ImportScope::new("https://kb.example.com", "run-1");
        let iri = s.skolem_iri("b0");
        assert_eq!(iri, "https://kb.example.com/.well-known/genid/run-1/b0");
        assert!(is_skolem_iri(&iri));
        assert!(!is_skolem_iri("https://kb.example.com/Alice"));
    }
}
