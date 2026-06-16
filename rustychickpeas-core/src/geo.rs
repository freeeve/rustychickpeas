//! Geo-spatial index over `(latitude, longitude)` node properties.
//!
//! A lazy, label-scoped index mirroring the equality `prop_index` and the
//! [`fulltext`](crate::fulltext) index: the first geo query for a
//! `(label, lat_key, lon_key)` triple reads that label's coordinates into a k-d
//! tree and caches it; later queries reuse it.
//!
//! Points are embedded on the **unit sphere** (lat/lon -> 3-D Cartesian), so
//! Euclidean chord distance is monotonic with great-circle distance. A 3-D k-d
//! tree therefore answers radius and k-nearest-neighbour queries *exactly* —
//! correct at the poles and across the antimeridian, unlike a planar lat/lon
//! tree. Distances are reported in kilometres. Results are [`NodeSet`]s (radius,
//! bbox) or `(node, km)` pairs (k-NN), composing with `nodes_with_label` and the
//! full-text index via `&`/`|`/`-`. No third-party geo dependency.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use roaring::RoaringBitmap;

use crate::bitmap::NodeSet;

/// Mean Earth radius in kilometres (IUGG).
const EARTH_RADIUS_KM: f64 = 6371.0088;

/// A balanced 3-D k-d tree node, indexing into the parallel coordinate arrays.
#[derive(Debug)]
struct KdNode {
    point: usize,
    left: Option<Box<KdNode>>,
    right: Option<Box<KdNode>>,
}

/// Immutable geo index for one `(label, lat_key, lon_key)` field.
#[derive(Debug, Default)]
pub struct GeoIndex {
    nodes: Vec<u32>,
    lats: Vec<f64>,
    lons: Vec<f64>,
    xyz: Vec<[f64; 3]>,
    root: Option<Box<KdNode>>,
}

impl GeoIndex {
    /// Build from `(node, lat_deg, lon_deg)` points. Non-finite or
    /// out-of-range coordinates are skipped.
    pub fn build(points: impl Iterator<Item = (u32, f64, f64)>) -> Self {
        let mut idx = GeoIndex::default();
        for (node, lat, lon) in points {
            if !lat.is_finite()
                || !lon.is_finite()
                || !(-90.0..=90.0).contains(&lat)
                || !(-180.0..=180.0).contains(&lon)
            {
                continue;
            }
            idx.nodes.push(node);
            idx.lats.push(lat);
            idx.lons.push(lon);
            idx.xyz.push(unit_vec(lat, lon));
        }
        let mut indices: Vec<usize> = (0..idx.nodes.len()).collect();
        idx.root = build_kd(&idx.xyz, &mut indices, 0);
        idx
    }

    /// Nodes within `km` great-circle distance of `(lat, lon)`.
    pub fn within_radius(&self, lat: f64, lon: f64, km: f64) -> NodeSet {
        if km < 0.0 || self.root.is_none() {
            return NodeSet::empty();
        }
        let q = unit_vec(lat, lon);
        let chord = chord_threshold(km);
        let chord_sq = chord * chord;
        let mut found: Vec<usize> = Vec::new();
        self.range(self.root.as_deref(), &q, chord_sq, 0, &mut found);
        let mut out = RoaringBitmap::new();
        for p in found {
            out.insert(self.nodes[p]);
        }
        NodeSet::new(out)
    }

    /// Up to `k` nearest nodes to `(lat, lon)` as `(node, distance_km)`, sorted
    /// by increasing distance, ties broken by ascending node id.
    pub fn knn(&self, lat: f64, lon: f64, k: usize) -> Vec<(u32, f64)> {
        if k == 0 || self.root.is_none() {
            return Vec::new();
        }
        let q = unit_vec(lat, lon);
        let mut heap: BinaryHeap<Neighbor> = BinaryHeap::with_capacity(k + 1);
        self.nn(self.root.as_deref(), &q, k, 0, &mut heap);
        let mut out: Vec<(u32, f64)> = heap
            .into_iter()
            .map(|n| (n.node, chord_to_km(n.dist_sq.sqrt())))
            .collect();
        out.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        out
    }

    /// Nodes whose coordinates fall in the lat/lon rectangle. When
    /// `min_lon > max_lon` the box is treated as crossing the antimeridian.
    pub fn within_bbox(&self, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> NodeSet {
        let lon_in = |lon: f64| {
            if min_lon <= max_lon {
                lon >= min_lon && lon <= max_lon
            } else {
                lon >= min_lon || lon <= max_lon
            }
        };
        let mut out = RoaringBitmap::new();
        for i in 0..self.nodes.len() {
            if self.lats[i] >= min_lat && self.lats[i] <= max_lat && lon_in(self.lons[i]) {
                out.insert(self.nodes[i]);
            }
        }
        NodeSet::new(out)
    }

    /// Number of indexed points.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the index holds no points.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Recursive range search: collect point indices within `r_sq` chord².
    fn range(&self, node: Option<&KdNode>, q: &[f64; 3], r_sq: f64, axis: usize, out: &mut Vec<usize>) {
        let Some(n) = node else {
            return;
        };
        let p = n.point;
        if dist_sq(&self.xyz[p], q) <= r_sq {
            out.push(p);
        }
        let diff = q[axis] - self.xyz[p][axis];
        let next = (axis + 1) % 3;
        let (near, far) = if diff <= 0.0 {
            (n.left.as_deref(), n.right.as_deref())
        } else {
            (n.right.as_deref(), n.left.as_deref())
        };
        self.range(near, q, r_sq, next, out);
        if diff * diff <= r_sq {
            self.range(far, q, r_sq, next, out);
        }
    }

    /// Recursive k-NN search maintaining a bounded max-heap of the k closest.
    fn nn(&self, node: Option<&KdNode>, q: &[f64; 3], k: usize, axis: usize, heap: &mut BinaryHeap<Neighbor>) {
        let Some(n) = node else {
            return;
        };
        let p = n.point;
        push_bounded(
            heap,
            k,
            Neighbor {
                dist_sq: dist_sq(&self.xyz[p], q),
                node: self.nodes[p],
            },
        );
        let diff = q[axis] - self.xyz[p][axis];
        let next = (axis + 1) % 3;
        let (near, far) = if diff <= 0.0 {
            (n.left.as_deref(), n.right.as_deref())
        } else {
            (n.right.as_deref(), n.left.as_deref())
        };
        self.nn(near, q, k, next, heap);
        let worst = if heap.len() < k {
            f64::INFINITY
        } else {
            heap.peek().map_or(f64::INFINITY, |w| w.dist_sq)
        };
        if diff * diff <= worst {
            self.nn(far, q, k, next, heap);
        }
    }
}

/// A k-NN candidate, ordered by distance then node id so the heap's max is the
/// current worst neighbour.
#[derive(Debug)]
struct Neighbor {
    dist_sq: f64,
    node: u32,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Neighbor {}
impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist_sq
            .partial_cmp(&other.dist_sq)
            .unwrap_or(Ordering::Equal)
            .then(self.node.cmp(&other.node))
    }
}
impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Push into a max-heap kept to size `k` (the largest/worst is evicted).
fn push_bounded(heap: &mut BinaryHeap<Neighbor>, k: usize, n: Neighbor) {
    heap.push(n);
    if heap.len() > k {
        heap.pop();
    }
}

/// Build a balanced k-d tree over `indices` (median split, axis = depth % 3).
fn build_kd(xyz: &[[f64; 3]], indices: &mut [usize], depth: usize) -> Option<Box<KdNode>> {
    if indices.is_empty() {
        return None;
    }
    let axis = depth % 3;
    indices.sort_by(|&a, &b| xyz[a][axis].partial_cmp(&xyz[b][axis]).unwrap_or(Ordering::Equal));
    let mid = indices.len() / 2;
    let point = indices[mid];
    let (left, rest) = indices.split_at_mut(mid);
    let right = &mut rest[1..];
    Some(Box::new(KdNode {
        point,
        left: build_kd(xyz, left, depth + 1),
        right: build_kd(xyz, right, depth + 1),
    }))
}

/// Unit-sphere Cartesian coordinates for a lat/lon in degrees.
fn unit_vec(lat: f64, lon: f64) -> [f64; 3] {
    let (la, lo) = (lat.to_radians(), lon.to_radians());
    let cos_lat = la.cos();
    [cos_lat * lo.cos(), cos_lat * lo.sin(), la.sin()]
}

/// Squared Euclidean (chord) distance between two unit vectors.
fn dist_sq(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    let (dx, dy, dz) = (a[0] - b[0], a[1] - b[1], a[2] - b[2]);
    dx * dx + dy * dy + dz * dz
}

/// Unit-sphere chord length for a great-circle distance of `km`.
fn chord_threshold(km: f64) -> f64 {
    let theta = (km / EARTH_RADIUS_KM).min(std::f64::consts::PI);
    2.0 * (theta / 2.0).sin()
}

/// Inverse of [`chord_threshold`]: unit-sphere chord length -> great-circle km.
fn chord_to_km(chord: f64) -> f64 {
    2.0 * (chord / 2.0).clamp(-1.0, 1.0).asin() * EARTH_RADIUS_KM
}

/// Great-circle distance in kilometres between two lat/lon points (Haversine).
/// Public utility, independent of the index.
pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * a.sqrt().clamp(-1.0, 1.0).asin() * EARTH_RADIUS_KM
}

#[cfg(test)]
mod tests {
    use super::*;

    // (node, lat, lon) for a few real cities.
    const LONDON: (u32, f64, f64) = (1, 51.5074, -0.1278);
    const PARIS: (u32, f64, f64) = (2, 48.8566, 2.3522);
    const NEW_YORK: (u32, f64, f64) = (3, 40.7128, -74.0060);
    const SYDNEY: (u32, f64, f64) = (4, -33.8688, 151.2093);

    fn cities() -> GeoIndex {
        GeoIndex::build([LONDON, PARIS, NEW_YORK, SYDNEY].into_iter())
    }

    fn sorted(ns: NodeSet) -> Vec<u32> {
        let mut v: Vec<u32> = ns.iter().collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn haversine_matches_known_distances() {
        // London–Paris ~344 km, London–New York ~5570 km.
        assert!((haversine_km(LONDON.1, LONDON.2, PARIS.1, PARIS.2) - 343.5).abs() < 3.0);
        assert!((haversine_km(LONDON.1, LONDON.2, NEW_YORK.1, NEW_YORK.2) - 5570.0).abs() < 20.0);
    }

    #[test]
    fn radius_returns_points_within_distance() {
        let g = cities();
        // 400 km of London includes London (0) and Paris (~344) but not NY/Sydney.
        assert_eq!(sorted(g.within_radius(LONDON.1, LONDON.2, 400.0)), [1, 2]);
        // Tighten below the London–Paris distance: only London remains.
        assert_eq!(sorted(g.within_radius(LONDON.1, LONDON.2, 100.0)), [1]);
        // Huge radius covers everything.
        assert_eq!(sorted(g.within_radius(LONDON.1, LONDON.2, 20_000.0)), [1, 2, 3, 4]);
    }

    #[test]
    fn knn_returns_nearest_in_order_with_distances() {
        let g = cities();
        let near = g.knn(LONDON.1, LONDON.2, 2);
        assert_eq!(near.len(), 2);
        assert_eq!(near[0].0, 1); // London itself, distance ~0
        assert!(near[0].1 < 1.0);
        assert_eq!(near[1].0, 2); // Paris next
        assert!((near[1].1 - 343.5).abs() < 3.0);
        // k larger than the set returns all four, nearest first.
        let all = g.knn(LONDON.1, LONDON.2, 10);
        assert_eq!(all.len(), 4);
        assert_eq!(all.iter().map(|(n, _)| *n).collect::<Vec<_>>(), [1, 2, 3, 4]);
    }

    #[test]
    fn knn_distance_agrees_with_haversine() {
        let g = cities();
        let near = g.knn(LONDON.1, LONDON.2, 4);
        for (node, km) in near {
            let city = [LONDON, PARIS, NEW_YORK, SYDNEY]
                .into_iter()
                .find(|c| c.0 == node)
                .unwrap();
            let expected = haversine_km(LONDON.1, LONDON.2, city.1, city.2);
            assert!((km - expected).abs() < 0.5, "node {node}: {km} vs {expected}");
        }
    }

    #[test]
    fn bbox_selects_rectangle() {
        let g = cities();
        // Western Europe box catches London and Paris only.
        assert_eq!(sorted(g.within_bbox(40.0, -10.0, 55.0, 10.0)), [1, 2]);
    }

    #[test]
    fn bbox_across_antimeridian() {
        // Points either side of 180°.
        let g = GeoIndex::build([(1, 0.0, 179.0), (2, 0.0, -179.0), (3, 0.0, 0.0)].into_iter());
        // min_lon > max_lon -> wraps the antimeridian, catching 1 and 2 but not 3.
        assert_eq!(sorted(g.within_bbox(-10.0, 170.0, 10.0, -170.0)), [1, 2]);
    }

    #[test]
    fn antimeridian_radius_is_correct() {
        // Two points 2° apart in longitude straddling 180° at the equator
        // (~222 km), which a planar lat/lon tree would mis-handle.
        let g = GeoIndex::build([(1, 0.0, 179.0), (2, 0.0, -179.0)].into_iter());
        assert_eq!(sorted(g.within_radius(0.0, 179.0, 300.0)), [1, 2]);
        assert_eq!(sorted(g.within_radius(0.0, 179.0, 100.0)), [1]);
    }

    #[test]
    fn empty_and_degenerate_inputs() {
        let g = GeoIndex::default();
        assert!(g.within_radius(0.0, 0.0, 100.0).is_empty());
        assert!(g.knn(0.0, 0.0, 5).is_empty());
        assert!(g.is_empty());
        let g = cities();
        assert!(g.knn(LONDON.1, LONDON.2, 0).is_empty());
        assert!(g.within_radius(LONDON.1, LONDON.2, -1.0).is_empty());
    }

    #[test]
    fn invalid_coordinates_are_skipped() {
        let g = GeoIndex::build(
            [
                (1, 51.5, -0.1),
                (2, f64::NAN, 0.0),
                (3, 200.0, 0.0),
                (4, 0.0, 999.0),
            ]
            .into_iter(),
        );
        assert_eq!(g.len(), 1);
        assert_eq!(sorted(g.within_radius(51.5, -0.1, 50.0)), [1]);
    }
}
