# Forest-safe subtree iterator (LDBC opportunity D, LOW) — SKIPPED (wontfix)

Decision (maintainer sign-off): do **not** add a core subtree-walk method.
No code change; Q3/Q4/Q9 keep their hand-rolled DFS loops.

Rationale — of the three consumers, only one is a clean fit, so a permanent
public core method isn't justified:
- **Q3** `popular_topics` — DFS with a `seen` set; the only clean adopter.
- **Q4** `top_creators` — deliberately reuses ONE stack across all top
  forums (no `seen`, forest property); a per-call `descendants()` iterator
  re-allocates a stack per forum, undoing that tuned detail.
- **Q9** `thread_initiators` — prunes subtrees past `end_day`. A
  yield-everything iterator can't express "don't descend here" and would
  over-walk every thread to its leaves (a real regression). A prune-visitor
  callback could cover it, but that leans "framework," which 024's own note
  warns against. And task 021 already sliced Q9's hot path.

After 021, 024's remaining value is cosmetic (dedup ~10-30 lines of stack
boilerplate), not worth a permanent public core method gated by the
sign-off convention. If a clean, broadly-reusable shape emerges later (e.g.
several non-pruning forest walks), revisit then.
