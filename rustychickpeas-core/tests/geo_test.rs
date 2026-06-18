//! End-to-end geo-spatial search over a finalized `GraphSnapshot`: radius,
//! k-NN, bbox, label scoping, and composition with full-text search (the
//! "documents near a place, matching a keyword" pattern).

use rustychickpeas_core::{GraphBuilder, GraphSnapshot};

fn city(b: &mut GraphBuilder, id: u32, name: &str, blurb: &str, lat: f64, lon: f64) {
    b.add_node(Some(id), &["City"]).unwrap();
    b.set_prop_str(id, "name", name).unwrap();
    b.set_prop_str(id, "blurb", blurb).unwrap();
    b.set_prop_f64(id, "lat", lat).unwrap();
    b.set_prop_f64(id, "lon", lon).unwrap();
}

fn cities() -> GraphSnapshot {
    let mut b = GraphBuilder::new(Some(8), Some(0));
    city(&mut b, 1, "London", "capital of england", 51.5074, -0.1278);
    city(&mut b, 2, "Paris", "capital of france", 48.8566, 2.3522);
    city(&mut b, 3, "New York", "city in america", 40.7128, -74.0060);
    // A non-City node at London's coordinates, to prove label scoping.
    b.add_node(Some(4), &["Other"]).unwrap();
    b.set_prop_f64(4, "lat", 51.5).unwrap();
    b.set_prop_f64(4, "lon", -0.12).unwrap();
    b.finalize(None)
}

fn sorted(ns: rustychickpeas_core::bitmap::NodeSet) -> Vec<u32> {
    let mut v: Vec<u32> = ns.iter().collect();
    v.sort_unstable();
    v
}

#[test]
fn radius_query_is_label_scoped() {
    let g = cities();
    // Within 400 km of London: London (0) and Paris (~344). New York excluded.
    assert_eq!(
        sorted(g.geo_within_radius("City", "lat", "lon", 51.5074, -0.1278, 400.0)),
        [1, 2]
    );
    // The "Other" node at London's coordinates must not surface under "City".
    let near: Vec<u32> = g
        .geo_within_radius("City", "lat", "lon", 51.5, -0.12, 1.0)
        .iter()
        .collect();
    assert_eq!(near, [1]);
}

#[test]
fn knn_returns_nearest_with_distances() {
    let g = cities();
    let near = g.geo_knn("City", "lat", "lon", 51.5074, -0.1278, 2);
    assert_eq!(near.iter().map(|(n, _)| *n).collect::<Vec<_>>(), [1, 2]);
    assert!(near[0].1 < 1.0); // London itself
    assert!((near[1].1 - 343.5).abs() < 3.0); // Paris
}

#[test]
fn bbox_query() {
    let g = cities();
    assert_eq!(
        sorted(g.geo_within_bbox("City", "lat", "lon", (40.0, -10.0), (55.0, 10.0))),
        [1, 2]
    );
}

#[test]
fn composes_with_full_text_search() {
    let g = cities();
    // "capitals within 400 km of London that mention 'france'": geo ∩ fts.
    let near = g.geo_within_radius("City", "lat", "lon", 51.5074, -0.1278, 400.0); // {1, 2}
    let french = g.full_text_search("City", "blurb", "france"); // {2}
    assert_eq!(sorted(&near & &french), [2]);

    // Broader keyword intersects to both nearby capitals.
    let capitals = g.full_text_search("City", "blurb", "capital"); // {1, 2}
    assert_eq!(sorted(&near & &capitals), [1, 2]);
}

#[test]
fn unknown_names_yield_empty() {
    let g = cities();
    assert!(g
        .geo_within_radius("Nope", "lat", "lon", 0.0, 0.0, 100.0)
        .is_empty());
    assert!(g
        .geo_within_radius("City", "nope", "lon", 0.0, 0.0, 100.0)
        .is_empty());
    assert!(g.geo_knn("City", "lat", "nope", 0.0, 0.0, 5).is_empty());
}
