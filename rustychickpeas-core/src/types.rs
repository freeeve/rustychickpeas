//! Core types for the graph API

/// Interned string ID
/// u32 allows up to 4.3 billion unique strings
pub type InternedStringId = u32;

/// Node identifier
/// Using u32 for better performance and memory efficiency with RoaringBitmaps
/// Maximum: 4,294,967,295 nodes
pub type NodeId = u32;

/// Property key (interned string ID)
/// Used to identify property columns in GraphSnapshot
pub type PropertyKey = u32;

// RelationshipId removed - GraphSnapshot doesn't track relationship IDs, only node-to-node connections
// PropertyId removed - not used in GraphSnapshot


/// Relationship direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// Label for nodes
/// Uses interned string ID for memory efficiency
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Label(InternedStringId);

impl Label {
    pub fn new(id: InternedStringId) -> Self {
        Label(id)
    }

    pub fn id(&self) -> InternedStringId {
        self.0
    }

    /// Resolve the label to a string (requires access to the interner)
    pub fn as_str<'a>(&self, interner: &'a crate::interner::StringInterner) -> String {
        interner.resolve(self.0)
    }
}

/// Relationship type
/// Uses interned string ID for memory efficiency
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationshipType(InternedStringId);

impl RelationshipType {
    pub fn new(id: InternedStringId) -> Self {
        RelationshipType(id)
    }

    pub fn id(&self) -> InternedStringId {
        self.0
    }

    /// Resolve the relationship type to a string (requires access to the interner)
    pub fn as_str<'a>(&self, interner: &'a crate::interner::StringInterner) -> String {
        interner.resolve(self.0)
    }
}

/// Property value types
/// Note: Cannot implement Eq/Hash because f64 doesn't implement Eq
/// Use PropertyIndexValue for hashable property values in indexes
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    String(String),                    // Not interned (default)
    InternedString(InternedStringId), // Interned (when property value interning is enabled)
    Integer(i64),
    Float(f64),
    Boolean(bool),
    // TODO: Add more types (List, Map, etc.)
}

impl PropertyValue {
    /// Resolve a property value to a string representation
    /// For interned strings, requires access to the interner
    pub fn as_string(&self, interner: Option<&crate::interner::StringInterner>) -> String {
        match self {
            PropertyValue::String(s) => s.clone(),
            PropertyValue::InternedString(id) => {
                interner
                    .expect("Interner required to resolve interned string")
                    .resolve(*id)
            }
            PropertyValue::Integer(i) => i.to_string(),
            PropertyValue::Float(f) => f.to_string(),
            PropertyValue::Boolean(b) => b.to_string(),
        }
    }
}

// Provenance removed - GraphSnapshot uses version strings instead

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interner::StringInterner;

    #[test]
    fn test_label_new() {
        let label = Label::new(5);
        assert_eq!(label.id(), 5);
    }

    #[test]
    fn test_label_as_str() {
        let interner = StringInterner::new();
        let id = interner.get_or_intern("Person");
        let label = Label::new(id);
        assert_eq!(label.as_str(&interner), "Person");
    }

    #[test]
    fn test_relationship_type_new() {
        let rel_type = RelationshipType::new(10);
        assert_eq!(rel_type.id(), 10);
    }

    #[test]
    fn test_relationship_type_as_str() {
        let interner = StringInterner::new();
        let id = interner.get_or_intern("KNOWS");
        let rel_type = RelationshipType::new(id);
        assert_eq!(rel_type.as_str(&interner), "KNOWS");
    }

    #[test]
    fn test_property_value_string() {
        let pv = PropertyValue::String("hello".to_string());
        assert_eq!(pv.as_string(None), "hello");
    }

    #[test]
    fn test_property_value_interned_string() {
        let interner = StringInterner::new();
        let id = interner.get_or_intern("world");
        let pv = PropertyValue::InternedString(id);
        assert_eq!(pv.as_string(Some(&interner)), "world");
    }

    #[test]
    fn test_property_value_integer() {
        let pv = PropertyValue::Integer(42);
        assert_eq!(pv.as_string(None), "42");
    }

    #[test]
    fn test_property_value_float() {
        let pv = PropertyValue::Float(3.14);
        assert_eq!(pv.as_string(None), "3.14");
    }

    #[test]
    fn test_property_value_boolean() {
        let pv = PropertyValue::Boolean(true);
        assert_eq!(pv.as_string(None), "true");
        let pv = PropertyValue::Boolean(false);
        assert_eq!(pv.as_string(None), "false");
    }

    #[test]
    fn test_direction() {
        assert_eq!(Direction::Outgoing, Direction::Outgoing);
        assert_ne!(Direction::Outgoing, Direction::Incoming);
        assert_ne!(Direction::Outgoing, Direction::Both);
    }
}
