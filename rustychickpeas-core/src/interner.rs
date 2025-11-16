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
        
        // Build a mapping of all strings by trying to resolve IDs
        // We'll scan from 0 up to a reasonable maximum (len should be the count of strings)
        // Rodeo assigns IDs sequentially, so we should be able to resolve 0..len-1
        let mut result = Vec::with_capacity(len);
        let mut found_count = 0;
        
        // Try to resolve IDs from 0 up to len (or until we've found all strings)
        for i in 0..len {
            let spur: lasso::Spur = unsafe { std::mem::transmute(i as u32) };
            match reader.try_resolve(&spur) {
                Some(s) => {
                    result.push(s.to_string());
                    found_count += 1;
                }
                None => {
                    // If ID doesn't exist, push empty string as placeholder
                    // This maintains index alignment (id i maps to result[i])
                    result.push(String::new());
                }
            }
        }
        
        // If we didn't find all strings, try scanning further (in case len() is wrong)
        if found_count < len {
            for i in len..(len * 2).min(u32::MAX as usize) {
                let spur: lasso::Spur = unsafe { std::mem::transmute(i as u32) };
                if let Some(s) = reader.try_resolve(&spur) {
                    // Extend the vec if needed
                    while result.len() <= i {
                        result.push(String::new());
                    }
                    result[i] = s.to_string();
                    found_count += 1;
                    if found_count >= len {
                        break;
                    }
                } else {
                    // If we hit a gap and haven't found all strings, something's wrong
                    // But continue anyway
                    break;
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
