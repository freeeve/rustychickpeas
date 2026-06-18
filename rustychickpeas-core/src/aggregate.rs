//! General parallel grouped reductions over dense `i64` node columns.
//!
//! A fluent, immutable [`Aggregation`] builder (from [`GraphSnapshot::aggregate`])
//! scans the nodes of one or more labels, filters them, groups by columns / binned
//! columns / the source label, and reduces each group to a count and an optional
//! sum. The scan runs in parallel via [`NodeSet::par_fold`], so it is the fast path
//! for analytic rollups without leaving the crate (no dataframe dependency).

use crate::bitmap::NodeSet;
use crate::error::{GraphError, Result};
use crate::graph_snapshot::GraphSnapshot;
use crate::types::{Direction, NodeId, RelationshipType};
use hashbrown::{HashMap, HashSet};

/// Comparison operator for [`Aggregation`] filters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl AggOp {
    /// Parse from a symbol (`<`, `<=`, `>`, `>=`, `==`, `!=`).
    pub fn parse(s: &str) -> Result<AggOp> {
        Ok(match s {
            "<" => AggOp::Lt,
            "<=" => AggOp::Le,
            ">" => AggOp::Gt,
            ">=" => AggOp::Ge,
            "==" => AggOp::Eq,
            "!=" => AggOp::Ne,
            other => {
                return Err(GraphError::SchemaError(format!(
                    "unknown comparison op '{}' (use <, <=, >, >=, ==, !=)",
                    other
                )))
            }
        })
    }

    #[inline]
    pub fn test(self, a: i64, b: i64) -> bool {
        match self {
            AggOp::Lt => a < b,
            AggOp::Le => a <= b,
            AggOp::Gt => a > b,
            AggOp::Ge => a >= b,
            AggOp::Eq => a == b,
            AggOp::Ne => a != b,
        }
    }
}

/// One group dimension: a raw `i64` column value, or a column bucketed at ascending
/// `edges` (bucket = count of `edges <= value`).
#[derive(Clone)]
enum GroupDim {
    Col(String),
    Bin(String, Vec<i64>),
}

/// A fluent, parallel grouped reduction over dense `i64` node columns. Build with
/// [`GraphSnapshot::aggregate`], chain the steps, then call [`run`](Self::run).
#[derive(Clone)]
pub struct Aggregation<'a> {
    graph: &'a GraphSnapshot,
    labels: Vec<String>,
    filters: Vec<(String, AggOp, i64)>,
    having: Vec<(String, AggOp, i64)>,
    by_label: bool,
    group: Vec<GroupDim>,
    sum: Option<String>,
    /// When set, count *edges* of this relationship type/direction out of each
    /// source node, grouping additionally by the neighbor (the `"neighbor"` field).
    through: Option<(String, Direction)>,
    /// With `through`, restrict counting to neighbors in this set (others skipped).
    neighbor_filter: Option<HashSet<NodeId>>,
}

/// One output group from [`Aggregation::run`]: the group-key values in field order
/// (the source label as its index when grouping by label), the row count, and the
/// summed value (0 when no `sum` column was set).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggRow {
    pub key: Vec<i64>,
    pub count: u64,
    pub sum: i64,
}

/// Result of [`Aggregation::run`].
#[derive(Clone, Debug)]
pub struct AggResult {
    /// Rows passing the population (`filter`) predicates, content or not.
    pub total: u64,
    /// One [`AggRow`] per group (unordered).
    pub rows: Vec<AggRow>,
    /// Name for each key position: `"label"`, a column name, or `"{col}_bin"`.
    pub fields: Vec<String>,
}

impl GraphSnapshot {
    /// Start a [parallel grouped reduction](Aggregation) over the given node labels.
    pub fn aggregate<I, S>(&self, labels: I) -> Aggregation<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Aggregation {
            graph: self,
            labels: labels.into_iter().map(Into::into).collect(),
            filters: Vec::new(),
            having: Vec::new(),
            by_label: false,
            group: Vec::new(),
            sum: None,
            through: None,
            neighbor_filter: None,
        }
    }
}

impl<'a> Aggregation<'a> {
    /// Population predicate `column op value`; rows passing all of these are counted
    /// in [`AggResult::total`].
    pub fn filter(mut self, column: impl Into<String>, op: AggOp, value: i64) -> Self {
        self.filters.push((column.into(), op, value));
        self
    }

    /// Extra predicate applied to grouped rows only (after the population filters).
    pub fn having(mut self, column: impl Into<String>, op: AggOp, value: i64) -> Self {
        self.having.push((column.into(), op, value));
        self
    }

    /// Group by the source node label (key position is its index; field `"label"`).
    pub fn by_label(mut self) -> Self {
        self.by_label = true;
        self
    }

    /// Group by a dense `i64` column's value (field = the column name).
    pub fn by(mut self, column: impl Into<String>) -> Self {
        self.group.push(GroupDim::Col(column.into()));
        self
    }

    /// Group by a column bucketed at ascending `edges` (field = `"{column}_bin"`).
    pub fn bin(mut self, column: impl Into<String>, edges: Vec<i64>) -> Self {
        self.group.push(GroupDim::Bin(column.into(), edges));
        self
    }

    /// Also sum this `i64` column per group.
    pub fn sum(mut self, column: impl Into<String>) -> Self {
        self.sum = Some(column.into());
        self
    }

    /// Count *edges* of `rel_type`/`direction` out of each (filtered) source node
    /// instead of counting nodes, grouping additionally by the neighbor node id
    /// (appended as the `"neighbor"` field). `total` still counts source nodes.
    pub fn through(mut self, rel_type: impl Into<String>, direction: Direction) -> Self {
        self.through = Some((rel_type.into(), direction));
        self
    }

    /// With [`through`](Self::through), count only neighbors in `ids` (others are
    /// skipped) — so the result has just those neighbors, not every reachable one.
    pub fn only_neighbors(mut self, ids: impl IntoIterator<Item = NodeId>) -> Self {
        self.neighbor_filter = Some(ids.into_iter().collect());
        self
    }

    /// Execute the reduction in parallel and collect the groups.
    pub fn run(&self) -> Result<AggResult> {
        let g = self.graph;
        let filters = resolve_filters(g, &self.filters)?;
        let having = resolve_filters(g, &self.having)?;
        let gspecs: Vec<ResolvedGroup> = self
            .group
            .iter()
            .map(|d| match d {
                GroupDim::Col(c) => Ok(ResolvedGroup::Col(dense_i64(g, c)?)),
                GroupDim::Bin(c, edges) => {
                    Ok(ResolvedGroup::Bin(dense_i64(g, c)?, edges.as_slice()))
                }
            })
            .collect::<Result<_>>()?;
        let sum: Option<&[i64]> = match &self.sum {
            Some(c) => Some(dense_i64(g, c)?),
            None => None,
        };
        let sets: Vec<&NodeSet> = self
            .labels
            .iter()
            .map(|l| {
                g.nodes_with_label(l)
                    .ok_or_else(|| GraphError::SchemaError(format!("label '{}' not found", l)))
            })
            .collect::<Result<_>>()?;

        let by_label = self.by_label;
        let source_dims = by_label as usize + gspecs.len();
        // `through` switches the reduction from counting nodes to counting edges of a
        // relationship type, grouping additionally by the neighbor. A missing type
        // simply yields no edges.
        let is_through = self.through.is_some();
        let through_rt: Option<RelationshipType> =
            self.through.as_ref().and_then(|(name, _)| g.rel_type(name));
        let direction: Direction = self
            .through
            .as_ref()
            .map(|(_, d)| *d)
            .unwrap_or(Direction::Outgoing);
        let neighbor_filter = self.neighbor_filter.as_ref();

        // Each label's nodes are folded in parallel into a partial (map, total); the
        // partials are merged across labels.
        let mut groups: HashMap<GroupKey, (u64, i64)> = HashMap::new();
        let mut total: u64 = 0;
        for (label_idx, set) in sets.iter().enumerate() {
            let (part, sub_total) = set.par_fold(
                || (HashMap::<GroupKey, (u64, i64)>::new(), 0u64),
                |mut acc, node| {
                    let i = node as usize;
                    if !filters.iter().all(|(s, op, v)| op.test(s[i], *v)) {
                        return acc;
                    }
                    acc.1 += 1;
                    if !having.iter().all(|(s, op, v)| op.test(s[i], *v)) {
                        return acc;
                    }
                    let (buf, overflow, k) = source_key_parts(by_label, label_idx, &gspecs, i);
                    if is_through {
                        if let Some(rt) = through_rt {
                            for nb in g.neighbors_by_type(node, direction, rt) {
                                if let Some(allow) = neighbor_filter {
                                    if !allow.contains(&nb) {
                                        continue;
                                    }
                                }
                                let key = key_with_neighbor(k, &buf, &overflow, nb as i64);
                                let e = acc.0.entry(key).or_insert((0, 0));
                                e.0 += 1;
                                if let Some(s) = sum {
                                    e.1 += s[i];
                                }
                            }
                        }
                    } else {
                        let key = GroupKey::new(k, &buf, overflow);
                        let e = acc.0.entry(key).or_insert((0, 0));
                        e.0 += 1;
                        if let Some(s) = sum {
                            e.1 += s[i];
                        }
                    }
                    acc
                },
                |mut a, b| {
                    for (k, (c, s)) in b.0 {
                        let e = a.0.entry(k).or_insert((0, 0));
                        e.0 += c;
                        e.1 += s;
                    }
                    a.1 += b.1;
                    a
                },
            );
            for (k, (c, s)) in part {
                let e = groups.entry(k).or_insert((0, 0));
                e.0 += c;
                e.1 += s;
            }
            total += sub_total;
        }

        let rows = groups
            .into_iter()
            .map(|(k, (count, sum))| AggRow {
                key: k.into_vec(),
                count,
                sum,
            })
            .collect();

        let mut fields = Vec::with_capacity(source_dims + is_through as usize);
        if by_label {
            fields.push("label".to_string());
        }
        for d in &self.group {
            match d {
                GroupDim::Col(c) => fields.push(c.clone()),
                GroupDim::Bin(c, _) => fields.push(format!("{}_bin", c)),
            }
        }
        if is_through {
            fields.push("neighbor".to_string());
        }

        Ok(AggResult {
            total,
            rows,
            fields,
        })
    }

    /// The source labels, in key order — lets a caller map a `"label"` key index
    /// back to its name.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }
}

/// A group column resolved to its dense `i64` slice (and bin edges for `Bin`).
enum ResolvedGroup<'a> {
    Col(&'a [i64]),
    Bin(&'a [i64], &'a [i64]),
}

/// Resolve `(column, op, value)` predicates to `(slice, op, value)`.
fn resolve_filters<'g>(
    g: &'g GraphSnapshot,
    filters: &[(String, AggOp, i64)],
) -> Result<Vec<(&'g [i64], AggOp, i64)>> {
    filters
        .iter()
        .map(|(c, op, v)| Ok((dense_i64(g, c)?, *op, *v)))
        .collect()
}

/// The dense `i64` slice for a node column, or a `SchemaError`.
fn dense_i64<'g>(g: &'g GraphSnapshot, key: &str) -> Result<&'g [i64]> {
    g.col(key)
        .map(|c| c.i64())
        .and_then(|c| c.as_slice())
        .ok_or_else(|| {
            GraphError::SchemaError(format!("column '{}' is not a dense i64 column", key))
        })
}

/// Compute the source group-key dimension values for node position `i`: the first
/// up to four in `buf`, the rest in `overflow`; returns the dimension count `k`.
#[inline]
fn source_key_parts(
    by_label: bool,
    label_idx: usize,
    gspecs: &[ResolvedGroup],
    i: usize,
) -> ([i64; 4], Vec<i64>, usize) {
    let mut buf = [0i64; 4];
    let mut overflow: Vec<i64> = Vec::new();
    let mut k = 0usize;
    let mut push = |v: i64| {
        if k < 4 {
            buf[k] = v;
        } else {
            overflow.push(v);
        }
        k += 1;
    };
    if by_label {
        push(label_idx as i64);
    }
    for g in gspecs {
        let v = match g {
            ResolvedGroup::Col(s) => s[i],
            ResolvedGroup::Bin(s, edges) => {
                let val = s[i];
                edges.iter().filter(|&&e| val >= e).count() as i64
            }
        };
        push(v);
    }
    (buf, overflow, k)
}

/// Build a group key from `sk` source dimensions (in `buf`/`overflow`) plus one
/// appended neighbor value — for the `through` edge-counting path.
#[inline]
fn key_with_neighbor(sk: usize, buf: &[i64; 4], overflow: &[i64], neighbor: i64) -> GroupKey {
    if sk < 4 {
        let mut b = *buf;
        b[sk] = neighbor;
        GroupKey::new(sk + 1, &b, Vec::new())
    } else {
        let mut v = Vec::with_capacity(sk + 1);
        v.extend_from_slice(buf);
        v.extend_from_slice(overflow);
        v.push(neighbor);
        GroupKey::Kn(v)
    }
}

/// Inline composite group key — no per-row heap allocation for up to four group
/// dimensions (the common case), falling back to a `Vec` beyond.
#[derive(PartialEq, Eq, Hash)]
enum GroupKey {
    K0,
    K1(i64),
    K2(i64, i64),
    K3(i64, i64, i64),
    K4(i64, i64, i64, i64),
    Kn(Vec<i64>),
}

impl GroupKey {
    fn new(dims: usize, buf: &[i64; 4], overflow: Vec<i64>) -> GroupKey {
        match dims {
            0 => GroupKey::K0,
            1 => GroupKey::K1(buf[0]),
            2 => GroupKey::K2(buf[0], buf[1]),
            3 => GroupKey::K3(buf[0], buf[1], buf[2]),
            4 => GroupKey::K4(buf[0], buf[1], buf[2], buf[3]),
            _ => {
                let mut v = Vec::with_capacity(dims);
                v.extend_from_slice(buf);
                v.extend(overflow);
                GroupKey::Kn(v)
            }
        }
    }

    fn into_vec(self) -> Vec<i64> {
        match self {
            GroupKey::K0 => Vec::new(),
            GroupKey::K1(a) => vec![a],
            GroupKey::K2(a, b) => vec![a, b],
            GroupKey::K3(a, b, c) => vec![a, b, c],
            GroupKey::K4(a, b, c, d) => vec![a, b, c, d],
            GroupKey::Kn(v) => v,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_builder::GraphBuilder;

    fn graph() -> GraphSnapshot {
        let mut b = GraphBuilder::new(Some(4), Some(0));
        // (id, label, day, content, len)
        let rows = [
            (0u32, "Post", 5i64, 1i64, 10i64),
            (1, "Post", 5, 1, 50),
            (2, "Comment", 5, 0, 20),
            (3, "Comment", 9, 1, 100),
        ];
        for (id, label, day, content, len) in rows {
            b.add_node(Some(id), &[label]).unwrap();
            b.set_prop_i64(id, "day", day).unwrap();
            b.set_prop_i64(id, "content", content).unwrap();
            b.set_prop_i64(id, "len", len).unwrap();
        }
        b.finalize(None)
    }

    #[test]
    fn aggregate_groups_filters_and_bins() {
        let g = graph();
        let res = g
            .aggregate(["Post", "Comment"])
            .filter("day", AggOp::Lt, 8)
            .having("content", AggOp::Ne, 0)
            .by_label()
            .bin("len", vec![40, 80, 160])
            .sum("len")
            .run()
            .unwrap();

        assert_eq!(res.total, 3); // nodes 0,1,2 are before the cutoff
        assert_eq!(res.fields, vec!["label", "len_bin"]);
        // label 0 = Post; node0 len10 -> bin0, node1 len50 -> bin1.
        let mut got: Vec<(Vec<i64>, u64, i64)> =
            res.rows.into_iter().map(|r| (r.key, r.count, r.sum)).collect();
        got.sort();
        assert_eq!(got, vec![(vec![0, 0], 1, 10), (vec![0, 1], 1, 50)]);
    }

    #[test]
    fn aggregate_missing_column_errors() {
        let g = graph();
        assert!(g.aggregate(["Post"]).by("nope").run().is_err());
    }

    #[test]
    fn aggregate_through_counts_edges_by_neighbor() {
        let mut b = GraphBuilder::new(Some(4), Some(4));
        for (id, label) in [(0u32, "Post"), (1, "Post"), (2, "Tag"), (3, "Tag")] {
            b.add_node(Some(id), &[label]).unwrap();
        }
        // Post0 -> Tag2, Tag3 ; Post1 -> Tag2
        b.add_relationship(0, 2, "hasTag").unwrap();
        b.add_relationship(0, 3, "hasTag").unwrap();
        b.add_relationship(1, 2, "hasTag").unwrap();
        let g = b.finalize(None);

        let res = g
            .aggregate(["Post"])
            .through("hasTag", Direction::Outgoing)
            .run()
            .unwrap();

        assert_eq!(res.total, 2); // two Post source nodes
        assert_eq!(res.fields, vec!["neighbor"]);
        let mut got: Vec<(i64, u64)> = res.rows.iter().map(|r| (r.key[0], r.count)).collect();
        got.sort();
        assert_eq!(got, vec![(2, 2), (3, 1)]); // tag2 has 2 edges, tag3 has 1

        // only_neighbors restricts the count to the given set.
        let res = g
            .aggregate(["Post"])
            .through("hasTag", Direction::Outgoing)
            .only_neighbors([3u32])
            .run()
            .unwrap();
        let got: Vec<(i64, u64)> = res.rows.iter().map(|r| (r.key[0], r.count)).collect();
        assert_eq!(got, vec![(3, 1)]); // only tag3 counted
    }
}
