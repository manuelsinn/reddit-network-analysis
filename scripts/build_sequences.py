#!/usr/bin/env python3
"""
Erzeugt aus einer gefilterten JSONL (author, subreddit, created_utc)
einen gerichteten, gewichteten Übergangs‑Graphen und speichert ihn
als GraphML – Gephi‑fertig.

Aufrufbeispiel (Testlauf):
    python build_sequences.py \
        --source filtered/cohort_nofap_TEST.jsonl \
        --out    outputs/nofap_TEST.graphml
Vollständiger Lauf:
    python build_sequences.py \
        --source filtered/cohort_nofap.jsonl \
        --out    outputs/nofap.graphml


python scripts/build_sequences.py --source filtered/cohort_nofap_TEST.jsonl --out outputs/nofap_TEST.graphml
"""
import argparse, json, networkx as nx, itertools, hashlib, pathlib, time, sys

# ---------- CLI ----------
cli = argparse.ArgumentParser()
cli.add_argument("--source", required=True,  help="cohort_*.jsonl")
cli.add_argument("--out",    required=True,  help="GraphML output path")
args = cli.parse_args()

SRC  = pathlib.Path(args.source)
OUT  = pathlib.Path(args.out)
OUT.parent.mkdir(parents=True, exist_ok=True)

start = time.time()
print(f"Loading {SRC} ...", file=sys.stderr)

rows = []                                # (uid, subreddit, ts)
with SRC.open("r", encoding="utf-8") as fh:
    for ln in fh:
        rec = json.loads(ln)
        rows.append((rec["author"], rec["subreddit"], rec["created_utc"]))

print(f" Parsed {len(rows):,} lines in {time.time()-start:.1f}s", file=sys.stderr)

# ----- User-ID anonymisieren & sortieren -----
rows = [(hashlib.sha256(a.encode()).hexdigest(), s, t) for a, s, t in rows]
rows.sort(key=lambda x: (x[0], x[2]))    # Sortieren by uid, timestamp


# --- Übergänge zählen --- (Paare bilden)
G = nx.DiGraph()
for (uid1, sub1, _), (uid2, sub2, _) in zip(rows, rows[1:]):
    if uid1 != uid2:
        continue
    G.add_edge(sub1, sub2, weight=G[sub1][sub2]['weight']+1
               if G.has_edge(sub1, sub2) else 1)

nx.set_edge_attributes(G, { (u,v): float(d['weight']) for u,v,d in G.edges(data=True) }, name='weight')

nx.write_graphml(G, OUT)


nx.write_graphml(G, OUT)
print("Saved GraphML to", OUT, file=sys.stderr)
print(f"Total runtime {time.time()-start:.1f}s")
