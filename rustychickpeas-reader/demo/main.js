// Demo of the split-residency model:
//  - graph.rcpg is fetched ONCE and lives in wasm memory (traversals are local)
//  - records.idx is fetched once (it's small); records.bin is NEVER fetched
//    whole — each record read is a coalesced HTTP Range request
import init, { WasmGraph, WasmRecordIndex } from "../pkg/rustychickpeas_reader.js";

const $ = (id) => document.getElementById(id);
const log = (msg) => { $("log").textContent = msg; };

async function fetchBytes(url, range) {
  const headers = range ? { Range: `bytes=${range[0]}-${range[1] - 1}` } : {};
  const resp = await fetch(url, { headers });
  if (!resp.ok && resp.status !== 206) throw new Error(`${url}: ${resp.status}`);
  return new Uint8Array(await resp.arrayBuffer());
}

await init();

const graph = new WasmGraph(await fetchBytes("graph.rcpg"));
const recordIndex = new WasmRecordIndex(await fetchBytes("records.idx"));
$("stats").textContent =
  `resident: ${graph.nNodes()} nodes, ${graph.nRels()} rels, ` +
  `labels [${graph.labels().join(", ")}], types [${graph.relationshipTypes().join(", ")}]; ` +
  `${recordIndex.len()} records remote`;

async function fetchRecord(id) {
  // plan_ranges returns [start0, end0, ...]; a single id yields one range
  const plan = recordIndex.planRanges(new Uint32Array([id]), 0);
  if (plan.length === 0) return null;
  const [start, end] = [plan[0], plan[1]];
  const bytes = await fetchBytes("records.bin", [start, end]);
  log(`fetched bytes ${start}-${end - 1} of records.bin (${end - start} bytes)`);
  const record = recordIndex.extract(id, start, bytes);
  return record ? JSON.parse(new TextDecoder().decode(record)) : null;
}

function renderNodes(title, ids) {
  const links = [...ids]
    .map((n) => `<a class="nbr" href="#" data-node="${n}">${n}</a>`)
    .join(" ");
  $("out").innerHTML = `<p>${title}</p><p>${links || "(none)"}</p>`;
  for (const a of $("out").querySelectorAll("a[data-node]")) {
    a.onclick = (e) => { e.preventDefault(); $("node").value = a.dataset.node; show(); };
  }
}

async function show() {
  const id = Number($("node").value);
  const labels = graph.nodeLabels(id).join(", ") || "(no labels)";
  renderNodes(
    `node ${id} [${labels}] — out: ${graph.outNeighbors(id).length}, in: ${graph.inNeighbors(id).length}`,
    graph.outNeighbors(id),
  );
  const record = await fetchRecord(id);
  $("record").textContent = record ? JSON.stringify(record, null, 2) : "(no record)";
}

$("show").onclick = show;
$("bfs").onclick = () => {
  const id = Number($("node").value);
  const t0 = performance.now();
  const reached = graph.bfs(id, 2, 0);
  const ms = (performance.now() - t0).toFixed(2);
  renderNodes(`bfs(node ${id}, depth 2, outgoing) reached ${reached.length} nodes in ${ms} ms (zero network)`, reached);
};

await show();
