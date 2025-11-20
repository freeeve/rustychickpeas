# Performance Benchmarks

### Graph Builder Operations

**Adding Nodes**

Benchmark: Adding 100,000 nodes to a graph builder. Each node has a single "Person" label.

<img src="img/builder_add_nodes_100000_time_badge.svg" alt="time badge" />

<img src="img/builder_add_nodes_100000_time.svg" alt="time sparkline" />

**Adding Relationships**

Benchmark: Adding 100,000 relationships to a graph with 100,000 nodes (2x relationships per node). Creates a ring structure where each node connects to the next.

<img src="img/builder_add_relationships_100000_time_badge.svg" alt="time badge" />

<img src="img/builder_add_relationships_100000_time.svg" alt="time sparkline" />

### Graph Snapshot Operations

**Getting Neighbors**

Benchmark: Querying neighbors for nodes in a graph with 100,000 nodes and 200,000 relationships (average degree of 2).

<img src="img/snapshot_get_neighbors_100000_time_badge.svg" alt="time badge" />

<img src="img/snapshot_get_neighbors_100000_time.svg" alt="time sparkline" />

**Getting Property**

Benchmark: Retrieving string properties from nodes in a graph with 100,000 nodes. Each node has a "name" property.

<img src="img/snapshot_get_property_100000_time_badge.svg" alt="time badge" />

<img src="img/snapshot_get_property_100000_time.svg" alt="time sparkline" />

### Bulk Loading

**Loading Nodes**

Benchmark: Bulk loading 1,000,000 nodes from Parquet format into a graph snapshot.

<img src="img/bulk_load_nodes_1000000_time_badge.svg" alt="time badge" />

<img src="img/bulk_load_nodes_1000000_time.svg" alt="time sparkline" />

### Graph Traversal

**BFS Traversal**

Benchmark: Breadth-first search on a graph with 1,000,000 nodes and ~8,000,000 relationships (average degree of 8). The graph uses realistic branching with a mix of local connections (ring structure) and long-range connections. Traversal starts from the first 10% of nodes (100,000 start nodes).

<img src="img/bfs_1000000_time_badge.svg" alt="time badge" />

<img src="img/bfs_1000000_time.svg" alt="time sparkline" />

**Bidirectional BFS**

Benchmark: Bidirectional breadth-first search on a graph with 1,000,000 nodes and ~8,000,000 relationships (average degree of 8). The graph uses realistic branching with a mix of local connections and long-range connections. Source nodes are the first 10% (100,000 nodes), target nodes are the last 10% (100,000 nodes).

<img src="img/bidirectional_bfs_1000000_time_badge.svg" alt="time badge" />

<img src="img/bidirectional_bfs_1000000_time.svg" alt="time sparkline" />

*Note: These charts show performance trends over time. Badge colors indicate performance changes: 🟢 improvement, 🟡 neutral, 🔴 regression.*

