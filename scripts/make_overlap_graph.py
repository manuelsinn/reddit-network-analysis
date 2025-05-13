#!/usr/bin/env python3
"""
Create an overlap graph (undirected) from a concatenated cohort JSONL.

Example:

    python scripts/make_overlap_graph.py --cohort filtered/cohort_2024-Q4-wo-12.jsonl --out outputs/overlap_Q4-no12.gexf --weight-min 30 --jaccard-min 0.05
"""

import argparse, json, time, itertools, numpy as np
import networkx as nx
from collections import defaultdict, Counter

# ---------- CLI ------------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--cohort", required=True, help="input JSONL with author, subreddit, created_utc")
cli.add_argument("--out",    required=True, help="output graph file (.gexf or .graphml)")
cli.add_argument("--weight-min", type=int, default=30,
                 help="min shared-user count to keep an edge")
cli.add_argument("--jaccard-min", type=float, default=0.0,
                 help="min Jaccard to keep an edge (0 = ignore)")
cli.add_argument("--log-step", type=int, default=1_000_000,
                 help="progress print every N lines")
args = cli.parse_args()

COHORT = args.cohort
OUT    = args.out
W_MIN  = args.weight_min
J_MIN  = args.jaccard_min
STEP   = args.log_step

# ---------- 1) Subreddit-ID mapping & user→subs index ----------------------
sub2id, id2sub = {}, []
def sid(name: str) -> int:
    if name not in sub2id:
        sub2id[name] = len(sub2id)
        id2sub.append(name)
    return sub2id[name]

user_subs = defaultdict(set)

t0 = time.time()
with open(COHORT, encoding="utf-8") as fh:
    for i, ln in enumerate(fh, 1):
        if i % STEP == 0:
            print(f"{i:,} lines read  [{time.time()-t0:.1f}s]")
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            continue
        user_subs[rec["author"]].add(sid(rec["subreddit"]))

print(f"Index ready – users: {len(user_subs):,}  | elapsed {time.time()-t0:.1f}s")

# ---------- 2) Overlap counting -------------------------------------------
edge_cnt      = Counter()
sub_usercount = Counter()
pair_updates  = 0

for u_idx, subs in enumerate(user_subs.values(), 1):
    subs = list(subs)
    for s in subs:
        sub_usercount[s] += 1
    if len(subs) < 2:
        continue
    arr = np.fromiter(subs, dtype=np.uint32)
    for a, b in itertools.combinations(arr, 2):
        if a > b:
            a, b = b, a
        edge_cnt[(a, b)] += 1
        pair_updates += 1
        if pair_updates % (STEP // 5) == 0:
            print(f"{pair_updates:,} pair updates   "
                  f"| unique pairs: {len(edge_cnt):,}", end="\r")

print(f"\nCounting done – unique pairs: {len(edge_cnt):,} "
      f"| total {time.time()-t0:.1f}s")

# ---------- 3) Filter + Graph build ---------------------------------------
G = nx.Graph()
kept = 0
for (a, b), overlap in edge_cnt.items():
    if overlap < W_MIN:
        continue
    union = sub_usercount[a] + sub_usercount[b] - overlap
    j     = overlap / union
    if j < J_MIN:
        continue
    G.add_edge(id2sub[a], id2sub[b],
               weight=float(overlap),
               jaccard=round(j, 4))
    kept += 1

print(f"Edges kept after filters: {kept:,}")

# ---------- 4) Export ------------------------------------------------------
if OUT.endswith(".gexf"):
    nx.write_gexf(G, OUT, encoding="utf-8", prettyprint=False)
else:
    nx.write_graphml(G, OUT)
print(f"Graph saved: {OUT}  | Nodes: {G.number_of_nodes():,}  "
      f"| Edges: {G.number_of_edges():,}  "
      f"| total runtime {time.time()-t0:.1f}s")
