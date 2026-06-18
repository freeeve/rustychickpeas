//! Decision measurement for the "Roaring-backed presence for very-sparse
//! columns" follow-up (task 025).
//!
//! The rank/select column marks present positions with a `BitVec` plus a
//! block-rank index (O(1) `rank`). For very-sparse columns a `RoaringBitmap`
//! presence might store the same set in far fewer bytes — but Roaring's `rank`
//! is slower. This compares the two on **presence bytes** and **rank latency**
//! across fill rates, so we only add a Roaring presence if it actually pays off.
//!
//! Run: `cargo run --release -p rustychickpeas-core --example roaring_presence_eval`

use std::time::Instant;

use bitvec::vec::BitVec;
use roaring::RoaringBitmap;

const RANK_BLOCK: usize = 512;
/// Node-id space (SF1-ish).
const N: usize = 2_900_000;
/// Random rank lookups timed per config.
const LOOKUPS: usize = 1_000_000;

/// block-rank index over `present`: prefix popcount at each RANK_BLOCK boundary.
fn build_block_rank(present: &BitVec) -> Vec<u32> {
    let nblocks = present.len() / RANK_BLOCK + 1;
    let mut br = Vec::with_capacity(nblocks);
    let mut running = 0u32;
    for b in 0..nblocks {
        br.push(running);
        let start = b * RANK_BLOCK;
        let end = (start + RANK_BLOCK).min(present.len());
        if start < present.len() {
            running += present[start..end].count_ones() as u32;
        }
    }
    br
}

/// values-index of `pos` (assumes present): block_rank + partial-block popcount.
#[inline]
fn rank_at(present: &BitVec, block_rank: &[u32], pos: usize) -> usize {
    let b = pos / RANK_BLOCK;
    let block_start = b * RANK_BLOCK;
    block_rank[b] as usize + present[block_start..pos].count_ones()
}

fn main() {
    println!("N = {N} nodes, RANK_BLOCK = {RANK_BLOCK}, {LOOKUPS} random present lookups/config");
    println!("Full i64 COLUMN bytes (presence + 8-byte values) and rank latency.");
    println!(
        "  bitvec = rank/select (BitVec+block-rank)   roaring = roaring presence   sparse = Vec<(u32,i64)> binary search\n"
    );
    println!(
        "{:>6}  {:>8} | {:>10} {:>10} {:>10} | {:>9} {:>9} {:>9}",
        "fill%", "present", "bitvec B", "roaring B", "sparse B", "bitvec", "roaring", "sparse"
    );

    for &fill in &[0.001f64, 0.005, 0.01, 0.015, 0.03, 0.05, 0.10] {
        let count = ((N as f64) * fill) as usize;
        // Spread `count` present positions evenly across 0..N (deterministic).
        let step = (N / count.max(1)).max(1);
        let positions: Vec<u32> = (0..count).map(|i| (i * step) as u32).collect();
        let values_bytes = count * 8; // Vec<i64> values, shared by bitvec + roaring

        // --- bitvec + block-rank (rank/select) ---
        let mut present: BitVec = BitVec::repeat(false, N);
        for &p in &positions {
            present.set(p as usize, true);
        }
        let block_rank = build_block_rank(&present);
        let bitvec_bytes =
            std::mem::size_of_val(present.as_raw_slice()) + block_rank.len() * 4 + values_bytes;

        // --- roaring presence ---
        let mut rb = RoaringBitmap::new();
        for &p in &positions {
            rb.insert(p);
        }
        let roaring_bytes = rb.serialized_size() + values_bytes;

        // --- sparse Vec<(u32, i64)> (what <1.56% fill uses today) ---
        let sparse: Vec<(u32, i64)> = positions.iter().map(|&p| (p, p as i64)).collect();
        let sparse_bytes = sparse.len() * std::mem::size_of::<(u32, i64)>();

        // --- rank latency over random present positions (LCG-sampled) ---
        let mut lcg: u64 = 0x1234_5678_9abc_def1;
        let mut sample = || {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            positions[((lcg >> 33) as usize) % positions.len()]
        };
        let queries: Vec<u32> = (0..LOOKUPS).map(|_| sample()).collect();

        let t0 = Instant::now();
        let mut acc = 0usize;
        for &q in &queries {
            acc = acc.wrapping_add(rank_at(&present, &block_rank, q as usize));
        }
        let bitvec_ns = t0.elapsed().as_nanos() as f64 / LOOKUPS as f64;

        let t1 = Instant::now();
        let mut acc2 = 0u64;
        for &q in &queries {
            acc2 = acc2.wrapping_add(rb.rank(q));
        }
        let roaring_ns = t1.elapsed().as_nanos() as f64 / LOOKUPS as f64;

        let t2 = Instant::now();
        let mut acc3 = 0usize;
        for &q in &queries {
            acc3 = acc3.wrapping_add(sparse.binary_search_by_key(&q, |(id, _)| *id).unwrap());
        }
        let sparse_ns = t2.elapsed().as_nanos() as f64 / LOOKUPS as f64;
        std::hint::black_box((acc, acc2, acc3));

        println!(
            "{:>6.2}  {:>8} | {:>8} B {:>8} B {:>8} B | {:>6.1}ns {:>6.1}ns {:>6.1}ns",
            fill * 100.0,
            count,
            bitvec_bytes,
            roaring_bytes,
            sparse_bytes,
            bitvec_ns,
            roaring_ns,
            sparse_ns,
        );
    }

    println!(
        "\nFor <1.56% fill the column is SPARSE today (binary search). Roaring presence is\nworth adding only if it beats `sparse` on bytes AND latency in that range."
    );
}
