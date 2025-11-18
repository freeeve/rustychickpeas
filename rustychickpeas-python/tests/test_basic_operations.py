"""Tests for basic graph operations using GraphSnapshotBuilder and GraphSnapshot

Note: This file has been updated to use the new immutable GraphSnapshot API.
The old mutable ChickpeasGraph API (create_node, delete_node, add_label, etc.)
has been removed. GraphSnapshotBuilder is used for building, and GraphSnapshot
is used for querying.
"""

import pytest
from rustychickpeas import GraphSnapshot, GraphSnapshotBuilder, Direction, RustyChickpeas


def test_create_builder():
    """Test creating a new builder"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    assert builder is not None


def test_add_node():
    """Test adding a node to builder"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person"])
    builder.add_node(1, ["Person"])
    
    # Finalize and retrieve snapshot
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    assert snapshot.n_nodes() == 2


def test_add_node_with_multiple_labels():
    """Test adding a node with multiple labels"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person", "User"])
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    
    # Check that node has the labels (using node ID 0)
    labels = snapshot.get_node_labels(0)
    assert "Person" in labels
    assert "User" in labels


def test_add_relationship():
    """Test adding a relationship"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person"])
    builder.add_node(1, ["Person"])
    builder.add_rel(0, 1, "KNOWS")
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    assert snapshot.n_rels() == 1
    
    # Check relationships (using node IDs 0->1)
    neighbors = snapshot.get_neighbors(0, Direction.Outgoing)
    assert 1 in neighbors
    
    neighbors = snapshot.get_neighbors(1, Direction.Incoming)
    assert 0 in neighbors


def test_set_node_property():
    """Test setting a node property"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person"])
    # Use generic set_prop which automatically detects types
    builder.set_prop(0, "name", "Alice")
    builder.set_prop(0, "age", 30)
    builder.set_prop(0, "active", True)
    builder.set_prop(0, "score", 95.5)
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    
    # Use node ID 0
    name = snapshot.get_node_property(0, "name")
    assert name is not None
    assert name == "Alice"
    
    age = snapshot.get_node_property(0, "age")
    assert age == 30
    
    active = snapshot.get_node_property(0, "active")
    assert active is True
    
    score = snapshot.get_node_property(0, "score")
    assert score == 95.5


def test_get_node_properties():
    """Test getting all node properties"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person"])
    builder.set_prop(0, "name", "Alice")
    builder.set_prop(0, "age", 30)
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    
    # GraphSnapshot doesn't have get_node_properties - test individual property access
    name = snapshot.get_node_property(0, "name")
    assert name == "Alice"
    
    age = snapshot.get_node_property(0, "age")
    assert age == 30


def test_get_all_nodes():
    """Test getting all nodes"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs (API now requires u32 IDs directly)
    builder.add_node(0, ["Person"])
    builder.add_node(1, ["Person"])
    builder.add_node(2, ["Admin"])
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    
    all_nodes = snapshot.get_all_nodes()
    assert len(all_nodes) == 3
    # GraphSnapshot returns node IDs (0, 1, 2)
    assert 0 in all_nodes
    assert 1 in all_nodes
    assert 2 in all_nodes


def test_get_nodes_with_label():
    """Test querying nodes by label"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person"])
    builder.add_node(1, ["Person"])
    builder.add_node(2, ["Admin"])
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    
    person_nodes = snapshot.get_nodes_with_label("Person")
    assert len(person_nodes) == 2
    # Node IDs 0, 1 have Person label
    assert 0 in person_nodes
    assert 1 in person_nodes
    assert 2 not in person_nodes
    
    admin_nodes = snapshot.get_nodes_with_label("Admin")
    assert len(admin_nodes) == 1
    # Node ID 2 has Admin label
    assert 2 in admin_nodes


def test_get_degree():
    """Test getting node degree"""
    manager = RustyChickpeas()
    builder = manager.create_builder(version="v1.0")
    
    # Use 0-indexed node IDs directly
    builder.add_node(0, ["Person"])
    builder.add_node(1, ["Person"])
    builder.add_node(2, ["Person"])
    
    builder.add_rel(0, 1, "KNOWS")
    builder.add_rel(0, 2, "KNOWS")
    
    builder.set_version("test_v1")
    builder.finalize_into(manager)
    
    snapshot = manager.get_graph_snapshot("test_v1")
    assert snapshot is not None
    
    # Node ID 0 has 2 outgoing edges
    degree = snapshot.get_degree(0, Direction.Outgoing)
    assert degree == 2
    
    # Node ID 1 has 1 incoming edge
    degree = snapshot.get_degree(1, Direction.Incoming)
    assert degree == 1
    
    degree = snapshot.get_degree(0, Direction.Both)
    assert degree == 2


def test_version_management():
    """Test managing multiple versions"""
    manager = RustyChickpeas()
    
    # Create first version
    builder1 = manager.create_builder(version="v1.0")
    builder1.add_node(0, ["Person"])
    builder1.set_version("v1.0")
    builder1.finalize_into(manager)
    
    # Create second version
    builder2 = manager.create_builder(version="v2.0")
    builder2.add_node(0, ["Person"])
    builder2.add_node(1, ["Person"])
    builder2.set_version("v2.0")
    builder2.finalize_into(manager)
    
    # Check versions
    versions = manager.versions()
    assert "v1.0" in versions
    assert "v2.0" in versions
    assert manager.len() == 2
    
    # Retrieve snapshots
    snap1 = manager.get_graph_snapshot("v1.0")
    snap2 = manager.get_graph_snapshot("v2.0")
    
    assert snap1 is not None
    assert snap2 is not None
    assert snap1.n_nodes() == 1
    assert snap2.n_nodes() == 2
