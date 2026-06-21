// Demo of the shared-ID, split-residency model:
//  - index.rrs (search) and graph.rcpg (topology) are fetched ONCE and live in
//    wasm memory; search and traversal run locally with no network round trips
//  - records.idx is fetched once (it's small); records.bin is NEVER fetched
//    whole — each record read is a coalesced HTTP Range request
//  - the same u32 id is the search doc id, the graph node id, and the record id,
//    so a hit flows search -> traverse -> record with no remapping
import init, { WasmGraph, WasmSearch, WasmRecordIndex } from "../pkg/rustychickpeas_reader.js";

const $ = (id) => document.getElementById(id);
const log = (msg) => { $("log").textContent = msg; };

let graph, search, recordIndex;

async function fetchBytes(url, range) {
  const headers = range ? { Range: `bytes=${range[0]}-${range[1] - 1}` } : {};
  const resp = await fetch(url, { headers });
  if (!resp.ok && resp.status !== 206) throw new Error(`${url}: ${resp.status}`);
  return new Uint8Array(await resp.arrayBuffer());
}

// Fetch many records in few range requests: plan coalesces nearby records into
// shared reads, then each record is sliced out of the chunk that covers it.
async function fetchRecords(ids) {
  const out = new Map();
  if (ids.length === 0) return out;
  const plan = recordIndex.planRanges(new Uint32Array(ids), 4096);
  const chunks = [];
  for (let i = 0; i < plan.length; i += 2) {
    const [s, e] = [plan[i], plan[i + 1]];
    chunks.push({ s, e, bytes: await fetchBytes("records.bin", [s, e]) });
  }
  log(`fetched ${ids.length} records in ${chunks.length} range request(s)`);
  for (const id of ids) {
    const rr = recordIndex.recordRange(id); // [start, end] or []
    if (rr.length === 0) continue;
    const chunk = chunks.find((c) => rr[0] >= c.s && rr[1] <= c.e);
    if (!chunk) continue;
    const bytes = recordIndex.extract(id, chunk.s, chunk.bytes);
    if (bytes) out.set(id, JSON.parse(new TextDecoder().decode(bytes)));
  }
  return out;
}

function wireNodeLinks(container) {
  for (const a of container.querySelectorAll("a[data-node]")) {
    a.onclick = (e) => { e.preventDefault(); $("node").value = a.dataset.node; show(); };
  }
}

async function runSearch() {
  const q = $("q").value.trim();
  const ids = Array.from(search.search(q, 25));
  if (ids.length === 0) { $("hits").innerHTML = "<p>(no hits)</p>"; return; }
  const records = await fetchRecords(ids);
  $("hits").innerHTML = ids
    .map((id) => {
      const r = records.get(id);
      const title = r ? r.title : "(record missing)";
      return `<a class="hit" href="#" data-node="${id}"><b>#${id}</b> ${title}</a>`;
    })
    .join("");
  wireNodeLinks($("hits"));
}

function renderNodes(title, ids) {
  const links = [...ids].map((n) => `<a class="nbr" href="#" data-node="${n}">${n}</a>`).join(" ");
  $("out").innerHTML = `<p>${title}</p><p>${links || "(none)"}</p>`;
  wireNodeLinks($("out"));
}

async function show() {
  const id = Number($("node").value);
  const labels = graph.nodeLabels(id).join(", ") || "(no labels)";
  const out = graph.neighbors(id, 0); // 0 = outgoing
  renderNodes(
    `node ${id} [${labels}] — cites ${out.length}, cited-by ${graph.neighbors(id, 1).length}`,
    out,
  );
  const record = (await fetchRecords([id])).get(id);
  $("record").textContent = record ? JSON.stringify(record, null, 2) : "(no record)";
}

// Boot with the failing step surfaced on the page (the browser console hides it
// otherwise — a blank "loading…" tells you nothing).
async function boot() {
  try {
    $("stats").textContent = "initializing wasm…";
    await init();
    $("stats").textContent = "loading graph.rcpg…";
    graph = new WasmGraph(await fetchBytes("graph.rcpg"));
    $("stats").textContent = "loading index.rrs…";
    search = new WasmSearch(await fetchBytes("index.rrs"));
    $("stats").textContent = "loading records.idx…";
    recordIndex = new WasmRecordIndex(await fetchBytes("records.idx"));
    $("stats").textContent =
      `resident: ${graph.nodeCount()} nodes, ${graph.relationshipCount()} rels, ` +
      `labels [${graph.labels().join(", ")}], types [${graph.relationshipTypes().join(", ")}]; ` +
      `${recordIndex.len()} records remote`;

    $("search").onclick = runSearch;
    $("q").onkeydown = (e) => { if (e.key === "Enter") runSearch(); };
    $("show").onclick = show;
    $("bfs").onclick = () => {
      const id = Number($("node").value);
      const t0 = performance.now();
      const reached = graph.bfs(id, 2, 0);
      const ms = (performance.now() - t0).toFixed(2);
      renderNodes(`bfs(node ${id}, depth 2, outgoing) reached ${reached.length} nodes in ${ms} ms (zero network)`, reached);
    };

    await runSearch();
    await show();
  } catch (e) {
    $("stats").textContent = "ERROR: " + (e && (e.stack || e.message) ? e.stack || e.message : e);
    console.error(e);
  }
}

await boot();
