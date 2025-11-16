//! RoaringBitmap utilities and wrappers

use roaring::RoaringBitmap;
use std::ops::{BitAnd, BitOr, Sub};

/// Node set: RoaringBitmap for large sets, BitVec for small hot sets
#[derive(Debug, Clone)]
pub enum NodeSet {
    /// RoaringBitmap for efficient large sets
    Roaring(RoaringBitmap),
    /// BitVec for ultra-hot small buckets (≤256 nodes)
    Bitset(bitvec::vec::BitVec),
}

impl NodeSet {
    pub fn new(bitmap: RoaringBitmap) -> Self {
        NodeSet::Roaring(bitmap)
    }

    pub fn new_bitset(bitset: bitvec::vec::BitVec) -> Self {
        NodeSet::Bitset(bitset)
    }

    pub fn empty() -> Self {
        NodeSet::Roaring(RoaringBitmap::new())
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        match self {
            NodeSet::Roaring(bitmap) => Box::new(bitmap.iter()),
            NodeSet::Bitset(bitset) => Box::new(
                bitset
                    .iter_ones()
                    .map(|idx| idx as u32),
            ),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodeSet::Roaring(bitmap) => bitmap.len() as usize,
            NodeSet::Bitset(bitset) => bitset.count_ones(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            NodeSet::Roaring(bitmap) => bitmap.is_empty(),
            NodeSet::Bitset(bitset) => bitset.not_any(),
        }
    }

    #[inline]
    pub fn contains(&self, node_id: u32) -> bool {
        match self {
            NodeSet::Roaring(bitmap) => bitmap.contains(node_id),
            NodeSet::Bitset(bitset) => bitset
                .get(node_id as usize)
                .map(|b| *b)
                .unwrap_or(false),
        }
    }

    pub fn insert(&mut self, node_id: u32) -> bool {
        match self {
            NodeSet::Roaring(bitmap) => bitmap.insert(node_id),
            NodeSet::Bitset(bitset) => {
                if node_id as usize >= bitset.len() {
                    bitset.resize(node_id as usize + 1, false);
                }
                let old = bitset[node_id as usize];
                bitset.set(node_id as usize, true);
                !old
            }
        }
    }

    pub fn remove(&mut self, node_id: u32) -> bool {
        match self {
            NodeSet::Roaring(bitmap) => bitmap.remove(node_id),
            NodeSet::Bitset(bitset) => {
                if node_id as usize >= bitset.len() {
                    return false;
                }
                let old = bitset[node_id as usize];
                bitset.set(node_id as usize, false);
                old
            }
        }
    }
}

impl Default for NodeSet {
    fn default() -> Self {
        Self::empty()
    }
}

impl BitAnd for &NodeSet {
    type Output = NodeSet;

    fn bitand(self, rhs: Self) -> Self::Output {
        // Convert to RoaringBitmap for operations, then optimize
        let left: RoaringBitmap = match self {
            NodeSet::Roaring(b) => b.clone(),
            NodeSet::Bitset(bv) => {
                let mut rb = RoaringBitmap::new();
                for idx in bv.iter_ones() {
                    rb.insert(idx as u32);
                }
                rb
            }
        };
        let right: RoaringBitmap = match rhs {
            NodeSet::Roaring(b) => b.clone(),
            NodeSet::Bitset(bv) => {
                let mut rb = RoaringBitmap::new();
                for idx in bv.iter_ones() {
                    rb.insert(idx as u32);
                }
                rb
            }
        };
        let result = &left & &right;
        // If result is small, use Bitset
        if result.len() <= 256 {
            let mut bv = bitvec::vec::BitVec::new();
            for id in result.iter() {
                if id as usize >= bv.len() {
                    bv.resize(id as usize + 1, false);
                }
                bv.set(id as usize, true);
            }
            NodeSet::Bitset(bv)
        } else {
            NodeSet::Roaring(result)
        }
    }
}

impl BitOr for &NodeSet {
    type Output = NodeSet;

    fn bitor(self, rhs: Self) -> Self::Output {
        let left: RoaringBitmap = match self {
            NodeSet::Roaring(b) => b.clone(),
            NodeSet::Bitset(bv) => {
                let mut rb = RoaringBitmap::new();
                for idx in bv.iter_ones() {
                    rb.insert(idx as u32);
                }
                rb
            }
        };
        let right: RoaringBitmap = match rhs {
            NodeSet::Roaring(b) => b.clone(),
            NodeSet::Bitset(bv) => {
                let mut rb = RoaringBitmap::new();
                for idx in bv.iter_ones() {
                    rb.insert(idx as u32);
                }
                rb
            }
        };
        let result = &left | &right;
        if result.len() <= 256 {
            let mut bv = bitvec::vec::BitVec::new();
            for id in result.iter() {
                if id as usize >= bv.len() {
                    bv.resize(id as usize + 1, false);
                }
                bv.set(id as usize, true);
            }
            NodeSet::Bitset(bv)
        } else {
            NodeSet::Roaring(result)
        }
    }
}

impl Sub for &NodeSet {
    type Output = NodeSet;

    fn sub(self, rhs: Self) -> Self::Output {
        let left: RoaringBitmap = match self {
            NodeSet::Roaring(b) => b.clone(),
            NodeSet::Bitset(bv) => {
                let mut rb = RoaringBitmap::new();
                for idx in bv.iter_ones() {
                    rb.insert(idx as u32);
                }
                rb
            }
        };
        let right: RoaringBitmap = match rhs {
            NodeSet::Roaring(b) => b.clone(),
            NodeSet::Bitset(bv) => {
                let mut rb = RoaringBitmap::new();
                for idx in bv.iter_ones() {
                    rb.insert(idx as u32);
                }
                rb
            }
        };
        let result = &left - &right;
        if result.len() <= 256 {
            let mut bv = bitvec::vec::BitVec::new();
            for id in result.iter() {
                if id as usize >= bv.len() {
                    bv.resize(id as usize + 1, false);
                }
                bv.set(id as usize, true);
            }
            NodeSet::Bitset(bv)
        } else {
            NodeSet::Roaring(result)
        }
    }
}

// RelationshipSet removed - GraphSnapshot doesn't track relationship IDs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nodeset_new() {
        let mut rb = RoaringBitmap::new();
        rb.insert(1);
        rb.insert(2);
        let ns = NodeSet::new(rb);
        assert_eq!(ns.len(), 2);
        assert!(!ns.is_empty());
    }

    #[test]
    fn test_nodeset_empty() {
        let ns = NodeSet::empty();
        assert_eq!(ns.len(), 0);
        assert!(ns.is_empty());
    }

    #[test]
    fn test_nodeset_default() {
        let ns = NodeSet::default();
        assert_eq!(ns.len(), 0);
        assert!(ns.is_empty());
    }

    #[test]
    fn test_nodeset_roaring_contains() {
        let mut rb = RoaringBitmap::new();
        rb.insert(1);
        rb.insert(5);
        let ns = NodeSet::new(rb);
        assert!(ns.contains(1));
        assert!(ns.contains(5));
        assert!(!ns.contains(3));
    }

    #[test]
    fn test_nodeset_roaring_insert() {
        let rb = RoaringBitmap::new();
        let mut ns = NodeSet::new(rb);
        assert!(!ns.contains(1));
        assert!(ns.insert(1));
        assert!(ns.contains(1));
        assert!(!ns.insert(1)); // Already present
    }

    #[test]
    fn test_nodeset_roaring_remove() {
        let mut rb = RoaringBitmap::new();
        rb.insert(1);
        let mut ns = NodeSet::new(rb);
        assert!(ns.contains(1));
        assert!(ns.remove(1));
        assert!(!ns.contains(1));
        assert!(!ns.remove(1)); // Already removed
    }

    #[test]
    fn test_nodeset_roaring_iter() {
        let mut rb = RoaringBitmap::new();
        rb.insert(1);
        rb.insert(3);
        rb.insert(5);
        let ns = NodeSet::new(rb);
        let mut items: Vec<u32> = ns.iter().collect();
        items.sort();
        assert_eq!(items, vec![1, 3, 5]);
    }

    #[test]
    fn test_nodeset_bitset() {
        let mut bv = bitvec::vec::BitVec::new();
        bv.resize(10, false);
        bv.set(1, true);
        bv.set(3, true);
        let ns = NodeSet::new_bitset(bv);
        assert_eq!(ns.len(), 2);
        assert!(!ns.is_empty());
        assert!(ns.contains(1));
        assert!(ns.contains(3));
        assert!(!ns.contains(2));
    }

    #[test]
    fn test_nodeset_bitset_insert() {
        let bv = bitvec::vec::BitVec::new();
        let mut ns = NodeSet::new_bitset(bv);
        assert!(!ns.contains(5));
        assert!(ns.insert(5));
        assert!(ns.contains(5));
        assert!(!ns.insert(5)); // Already present
    }

    #[test]
    fn test_nodeset_bitset_remove() {
        let mut bv = bitvec::vec::BitVec::new();
        bv.resize(10, false);
        bv.set(3, true);
        let mut ns = NodeSet::new_bitset(bv);
        assert!(ns.contains(3));
        assert!(ns.remove(3));
        assert!(!ns.contains(3));
        assert!(!ns.remove(3)); // Already removed
    }

    #[test]
    fn test_nodeset_bitset_iter() {
        let mut bv = bitvec::vec::BitVec::new();
        bv.resize(10, false);
        bv.set(1, true);
        bv.set(3, true);
        bv.set(7, true);
        let ns = NodeSet::new_bitset(bv);
        let mut items: Vec<u32> = ns.iter().collect();
        items.sort();
        assert_eq!(items, vec![1, 3, 7]);
    }

    #[test]
    fn test_nodeset_bitand_roaring() {
        let mut rb1 = RoaringBitmap::new();
        rb1.insert(1);
        rb1.insert(2);
        rb1.insert(3);
        let ns1 = NodeSet::new(rb1);
        
        let mut rb2 = RoaringBitmap::new();
        rb2.insert(2);
        rb2.insert(3);
        rb2.insert(4);
        let ns2 = NodeSet::new(rb2);
        
        let result = &ns1 & &ns2;
        assert_eq!(result.len(), 2);
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(1));
        assert!(!result.contains(4));
    }

    #[test]
    fn test_nodeset_bitand_bitset() {
        let mut bv1 = bitvec::vec::BitVec::new();
        bv1.resize(10, false);
        bv1.set(1, true);
        bv1.set(2, true);
        let ns1 = NodeSet::new_bitset(bv1);
        
        let mut bv2 = bitvec::vec::BitVec::new();
        bv2.resize(10, false);
        bv2.set(2, true);
        bv2.set(3, true);
        let ns2 = NodeSet::new_bitset(bv2);
        
        let result = &ns1 & &ns2;
        assert_eq!(result.len(), 1);
        assert!(result.contains(2));
    }

    #[test]
    fn test_nodeset_bitor_roaring() {
        let mut rb1 = RoaringBitmap::new();
        rb1.insert(1);
        rb1.insert(2);
        let ns1 = NodeSet::new(rb1);
        
        let mut rb2 = RoaringBitmap::new();
        rb2.insert(2);
        rb2.insert(3);
        let ns2 = NodeSet::new(rb2);
        
        let result = &ns1 | &ns2;
        assert_eq!(result.len(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_nodeset_bitor_bitset() {
        let mut bv1 = bitvec::vec::BitVec::new();
        bv1.resize(10, false);
        bv1.set(1, true);
        let ns1 = NodeSet::new_bitset(bv1);
        
        let mut bv2 = bitvec::vec::BitVec::new();
        bv2.resize(10, false);
        bv2.set(2, true);
        let ns2 = NodeSet::new_bitset(bv2);
        
        let result = &ns1 | &ns2;
        assert_eq!(result.len(), 2);
        assert!(result.contains(1));
        assert!(result.contains(2));
    }

    #[test]
    fn test_nodeset_sub_roaring() {
        let mut rb1 = RoaringBitmap::new();
        rb1.insert(1);
        rb1.insert(2);
        rb1.insert(3);
        let ns1 = NodeSet::new(rb1);
        
        let mut rb2 = RoaringBitmap::new();
        rb2.insert(2);
        let ns2 = NodeSet::new(rb2);
        
        let result = &ns1 - &ns2;
        assert_eq!(result.len(), 2);
        assert!(result.contains(1));
        assert!(result.contains(3));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_nodeset_sub_bitset() {
        let mut bv1 = bitvec::vec::BitVec::new();
        bv1.resize(10, false);
        bv1.set(1, true);
        bv1.set(2, true);
        let ns1 = NodeSet::new_bitset(bv1);
        
        let mut bv2 = bitvec::vec::BitVec::new();
        bv2.resize(10, false);
        bv2.set(2, true);
        let ns2 = NodeSet::new_bitset(bv2);
        
        let result = &ns1 - &ns2;
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));
    }

    #[test]
    fn test_nodeset_remove_out_of_bounds() {
        // Test remove() when node_id >= bitset.len() (line 83)
        let mut bv = bitvec::vec::BitVec::new();
        bv.resize(10, false);
        bv.set(5, true);
        let mut ns = NodeSet::new_bitset(bv);
        
        // Try to remove a node_id that's out of bounds
        assert_eq!(ns.remove(20), false);
        assert_eq!(ns.len(), 1);
    }

    #[test]
    fn test_nodeset_bitand_large_result() {
        // Test BitAnd that produces a result > 256 nodes (returns Roaring, line 136)
        // Create two large overlapping sets - intersection will be > 256
        let mut rb1 = RoaringBitmap::new();
        for i in 0..500 {
            rb1.insert(i);
        }
        let ns1 = NodeSet::new(rb1);
        
        let mut rb2 = RoaringBitmap::new();
        for i in 200..700 {
            rb2.insert(i);
        }
        let ns2 = NodeSet::new(rb2);
        
        let result = &ns1 & &ns2;
        // Result should be > 256 nodes (intersection of 0..500 and 200..700 = 200..500 = 300 nodes)
        assert!(result.len() > 256, "Result length: {}", result.len());
        match result {
            NodeSet::Roaring(_) => {},
            NodeSet::Bitset(_) => panic!("Expected Roaring for large result"),
        }
    }

    #[test]
    fn test_nodeset_bitor_large_result() {
        // Test BitOr that produces a result > 256 nodes (returns Roaring, line 176)
        let mut rb1 = RoaringBitmap::new();
        for i in 0..300 {
            rb1.insert(i);
        }
        let ns1 = NodeSet::new(rb1);
        
        let mut rb2 = RoaringBitmap::new();
        for i in 300..600 {
            rb2.insert(i);
        }
        let ns2 = NodeSet::new(rb2);
        
        let result = &ns1 | &ns2;
        // Result should be > 256 nodes, so it should be Roaring
        assert!(result.len() > 256);
        match result {
            NodeSet::Roaring(_) => {},
            NodeSet::Bitset(_) => panic!("Expected Roaring for large result"),
        }
    }

    #[test]
    fn test_nodeset_sub_large_result() {
        // Test Sub that produces a result > 256 nodes (returns Roaring, line 216)
        let mut rb1 = RoaringBitmap::new();
        for i in 0..500 {
            rb1.insert(i);
        }
        let ns1 = NodeSet::new(rb1);
        
        let mut rb2 = RoaringBitmap::new();
        for i in 100..200 {
            rb2.insert(i);
        }
        let ns2 = NodeSet::new(rb2);
        
        let result = &ns1 - &ns2;
        // Result should be > 256 nodes, so it should be Roaring
        assert!(result.len() > 256);
        match result {
            NodeSet::Roaring(_) => {},
            NodeSet::Bitset(_) => panic!("Expected Roaring for large result"),
        }
    }
}
