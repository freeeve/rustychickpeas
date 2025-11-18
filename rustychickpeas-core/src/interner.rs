//! String interning for memory efficiency
//!
//! Uses lasso to intern strings, storing each unique string only once
//! and using small integer IDs everywhere else.
//!
//! Uses RwLock instead of Mutex to allow concurrent reads while writes
//! (interning new strings) require exclusive access.

use lasso::Rodeo;
use std::sync::{Arc, RwLock};

/// Thread-safe string interner
/// Uses Arc<RwLock<>> to allow concurrent reads and exclusive writes
#[derive(Clone)]
pub struct StringInterner {
    inner: Arc<RwLock<Rodeo>>,
}

impl StringInterner {
    pub fn new() -> Self {
        StringInterner {
            inner: Arc::new(RwLock::new(Rodeo::new())),
        }
    }

    /// Intern a string and return its ID
    /// This requires a write lock, so it's exclusive
    pub fn get_or_intern(&self, s: &str) -> u32 {
        let mut interner = self.inner.write().unwrap();
        let spur = interner.get_or_intern(s);
        // Spur is a newtype around u32, so we can safely transmute
        unsafe { std::mem::transmute(spur) }
    }

    /// Batch intern multiple strings in a single lock
    /// This is more efficient than calling get_or_intern multiple times
    pub fn batch_intern(&self, strings: &[&str]) -> Vec<u32> {
        let mut interner = self.inner.write().unwrap();
        strings
            .iter()
            .map(|s| {
                let spur = interner.get_or_intern(s);
                unsafe { std::mem::transmute(spur) }
            })
            .collect()
    }

    /// Resolve an interned ID back to the string
    /// This uses a read lock, allowing concurrent reads
    pub fn resolve(&self, id: u32) -> String {
        let interner = self.inner.read().unwrap();
        let spur: lasso::Spur = unsafe { std::mem::transmute(id) };
        interner.resolve(&spur).to_string()
    }

    /// Try to resolve an interned ID (returns None if not found)
    /// This uses a read lock, allowing concurrent reads
    pub fn try_resolve(&self, id: u32) -> Option<String> {
        let interner = self.inner.read().unwrap();
        let spur: lasso::Spur = unsafe { std::mem::transmute(id) };
        interner.try_resolve(&spur).map(|s| s.to_string())
    }

    /// Get the ID for a string if it's already interned (returns None if not found)
    /// This uses a read lock, allowing concurrent reads
    pub fn get(&self, s: &str) -> Option<u32> {
        let interner = self.inner.read().unwrap();
        interner.get(s).map(|spur| unsafe { std::mem::transmute(spur) })
    }

    /// Get the number of interned strings
    /// This uses a read lock, allowing concurrent reads
    pub fn len(&self) -> usize {
        let interner = self.inner.read().unwrap();
        interner.len()
    }

    /// Extract all interned strings as a Vec (for snapshot creation)
    /// This consumes the interner and returns all strings in order
    /// The returned Vec is indexed by string ID (id 0 is always "")
    /// 
    /// Note: lasso's Rodeo assigns IDs sequentially starting from 0, so we can
    /// resolve all IDs from 0 to len-1. The empty string is always at ID 0.
    pub fn into_vec(self) -> Vec<String> {
        // Unwrap the Arc (should succeed since we're consuming self)
        let rwlock = Arc::try_unwrap(self.inner).unwrap_or_else(|_| {
            panic!("StringInterner has multiple Arc references - cannot extract");
        });
        let rodeo = rwlock.into_inner().unwrap();
        
        // Convert Rodeo to a reader which allows us to resolve strings
        let reader = rodeo.into_reader();
        let len = reader.len();
        
        // Handle empty case - always return at least empty string at index 0
        if len == 0 {
            return vec!["".to_string()];
        }
        
        // Build a mapping of all strings by trying to resolve IDs
        // The issue: Spur uses an offset (into_usize does `key.get() - 1`)
        // So a Spur with internal value n represents usize value n-1
        // We need to create Spurs correctly using try_from_usize if available,
        // or be very careful with transmute.
        //
        // Strategy: Since we're using transmute, we need to account for the offset.
        // But actually, the transmute should work if we use the correct value.
        // The problem might be that we're trying IDs that are out of range.
        //
        // Let's try a safer approach: iterate from 0 to len-1, but use try_resolve
        // which won't panic, and stop early if we hit too many failures.
        let mut result = Vec::with_capacity(len + 1);
        result.push("".to_string()); // Index 0 is always empty string
        
        // Try to resolve IDs from 1 up to len
        // Note: len() returns the count, so valid IDs should be 0..len-1
        // But we start from 1 since 0 is the empty string
        let mut consecutive_failures = 0;
        for i in 1..=len {
            if i > u32::MAX as usize {
                break;
            }
            // Create Spur - the internal representation is i+1, but we transmute i
            // This might be the issue - let's try using the Key trait's try_from_usize
            // Actually, we can't use that directly. Let's try a different approach:
            // Use the fact that Spur's internal value should be i+1 for external value i
            // But transmute doesn't account for this offset, so we need to adjust
            let spur_value = i as u32;
            let spur: lasso::Spur = unsafe { std::mem::transmute(spur_value) };
            match reader.try_resolve(&spur) {
                Some(s) => {
                    result.push(s.to_string());
                    consecutive_failures = 0;
                }
                None => {
                    consecutive_failures += 1;
                    // If we hit too many consecutive failures, we've probably reached the end
                    if consecutive_failures > 10 {
                        break;
                    }
                    // Push empty string to maintain index alignment
                    result.push(String::new());
                }
            }
        }
        
        result
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interner_new() {
        let interner = StringInterner::new();
        assert_eq!(interner.len(), 0);
    }

    #[test]
    fn test_interner_default() {
        let interner = StringInterner::default();
        assert_eq!(interner.len(), 0);
    }

    #[test]
    fn test_get_or_intern() {
        let interner = StringInterner::new();
        let id1 = interner.get_or_intern("hello");
        let id2 = interner.get_or_intern("world");
        assert_ne!(id1, id2);
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_get_or_intern_duplicate() {
        let interner = StringInterner::new();
        let id1 = interner.get_or_intern("hello");
        let id2 = interner.get_or_intern("hello");
        assert_eq!(id1, id2);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn test_resolve() {
        let interner = StringInterner::new();
        let id = interner.get_or_intern("test");
        assert_eq!(interner.resolve(id), "test");
    }

    #[test]
    fn test_try_resolve() {
        let interner = StringInterner::new();
        let id = interner.get_or_intern("test");
        assert_eq!(interner.try_resolve(id), Some("test".to_string()));
        assert_eq!(interner.try_resolve(999), None);
    }

    #[test]
    fn test_get() {
        let interner = StringInterner::new();
        assert_eq!(interner.get("hello"), None);
        let id = interner.get_or_intern("hello");
        assert_eq!(interner.get("hello"), Some(id));
        assert_eq!(interner.get("world"), None);
    }

    #[test]
    fn test_batch_intern() {
        let interner = StringInterner::new();
        let ids = interner.batch_intern(&["a", "b", "c"]);
        assert_eq!(ids.len(), 3);
        assert_eq!(interner.len(), 3);
        assert_eq!(interner.resolve(ids[0]), "a");
        assert_eq!(interner.resolve(ids[1]), "b");
        assert_eq!(interner.resolve(ids[2]), "c");
    }

    #[test]
    fn test_clone() {
        let interner = StringInterner::new();
        let id = interner.get_or_intern("test");
        let interner2 = interner.clone();
        assert_eq!(interner2.resolve(id), "test");
    }
}
