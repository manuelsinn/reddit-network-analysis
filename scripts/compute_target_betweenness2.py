#!/usr/bin/env python3
"""
Berechnet Target-Betweenness für ein gegebenes Netz und exportiert
zwei CSV-Dateien:
 1) target_btw.csv (id,label,target_btw)
 2) top10_targets_neighbors.csv (target,neighbor,method,weight,jaccard,weight_prop,jaccard_prop,count,prop)

Beispiel:
    python scripts/compute_target_betweenness.py \
          --graph outputs/overlap_Q4-no12.gexf \
          --targets MensRights,marriedredpill,TheRedPill,PickUpArtist \
          --out outputs/tb_Q4-no12.csv \
          --neighbors-out outputs/tb_Q4-no12_neighbors.csv

    python scripts/compute_target_betweenness.py --graph outputs/overlap_Q4.gexf --targets MensRights,marriedredpill,TheRedPill,PickUpArtist --out outputs/tb_Q4_new.csv --neighbors-out outputs/tb_Q4_new_neighbors.csv
"""
import argparse
import csv
import sys
import networkx as nx
import time
import pathlib
import pandas as pd
from collections import Counter

# ---------- CLI ------------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--graph",  required=True,
                 help="Graphdatei (.gexf, .graphml, .gexf.gz …)")
cli.add_argument("--targets", required=True,
                 help="Komma-Liste oder Pfad zu TXT mit 1 Node pro Zeile")
cli.add_argument("--out", default="target_btw.csv",
                 help="Name der Betweenness-CSV (Default target_btw.csv)")
cli.add_argument("--neighbors-out", default="top10_targets_neighbors.csv",
                 help="Name der Nachbar-CSV für Top-10 (Default top10_targets_neighbors.csv)")
cli.add_argument("--weight", default="weight",
                 help="Edge-Attribut für Pfadlänge (Default 'weight')")
cli.add_argument("--normalized", action="store_true",
                 help="Werte auf [0,1] normieren (NetworkX default False)")
args = cli.parse_args()

# ---------- Targets lesen --------------------------------------------------
if pathlib.Path(args.targets).is_file():
    TARGETS = {ln.strip() for ln in open(args.targets) if ln.strip()}
else:
    TARGETS = {x.strip() for x in args.targets.split(",") if x.strip()}

print(f"Targets ({len(TARGETS)}):", ", ".join(list(TARGETS)[:10]), "…")

# ---------- Graph laden ----------------------------------------------------
ext = pathlib.Path(args.graph).suffix.lower()
t0  = time.time()
if ext in (".gexf", ".gz"):
    G = nx.read_gexf(args.graph)
else:
    G = nx.read_graphml(args.graph)
print(f"Graph geladen: {G.number_of_nodes():,} Nodes, {G.number_of_edges():,} Edges  [{time.time()-t0:.1f}s]")

# ---------- Target-Betweenness --------------------------------------------
sources = set(G.nodes())
targets = TARGETS & set(G.nodes())
missing = TARGETS - targets
if missing:
    print("Warnung: folgende Targets fehlen im Netz:", ", ".join(missing), file=sys.stderr)

print("Berechne Target-Betweenness …")
t0 = time.time()
tb = nx.betweenness_centrality_subset(
        G, sources=sources, targets=targets,
        weight=args.weight, normalized=args.normalized)
print(f"Fertig  [{time.time()-t0:.1f}s]")

# ---------- Betweenness-CSV schreiben --------------------------------------
with open(args.out, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["id", "label", "target_btw"])
    for node, score in tb.items():
        label = G.nodes[node].get("label", node)
        w.writerow([node, label, f"{score:.6f}"])
print("Export Betweenness →", args.out, "| Zeilen:", len(tb))

# ---------- Top 10 Auswahl -------------------------------------------------
btw_items = sorted(tb.items(), key=lambda x: x[1], reverse=True)
top10 = [node for node, _ in btw_items[:10]]
print("Top-10 Targets:", ", ".join(top10))

# ---------- Nachbarschaftsanalyse für Top-10 -------------------------------
records = []
pairs = [(u, v) for i, u in enumerate(top10) for v in top10[i+1:]]
for target in top10:
    # Direkte Kanten
    direct_edges = [(u, v, data) for u, v, data in G.edges(target, data=True)]
    if direct_edges:
        total_w = sum(data.get('weight', 0) for _, _, data in direct_edges)
        total_j = sum(data.get('jaccard', 0) for _, _, data in direct_edges)
        for u, v, data in direct_edges:
            neighbor = v if u == target else u
            w = data.get('weight', 0)
            j = data.get('jaccard', 0)
            records.append({
                'target': target,
                'neighbor': neighbor,
                'method': 'direct',
                'weight': w,
                'jaccard': j,
                'weight_prop': w/total_w if total_w else 0,
                'jaccard_prop': j/total_j if total_j else 0,
                'count': None,
                'prop': None
            })
    else:
        # Kürzeste Pfade
        counter = Counter()
        for u, v in pairs:
            if target in (u, v):
                continue
            for path in nx.all_shortest_paths(G, u, v, weight=args.weight):
                if target in path:
                    idx = path.index(target)
                    if idx > 0:
                        counter[path[idx-1]] += 1
                    if idx < len(path)-1:
                        counter[path[idx+1]] += 1
        total_c = sum(counter.values()) or 1
        for neighbor, cnt in counter.items():
            records.append({
                'target': target,
                'neighbor': neighbor,
                'method': 'path',
                'weight': None,
                'jaccard': None,
                'weight_prop': None,
                'jaccard_prop': None,
                'count': cnt,
                'prop': cnt/total_c
            })

# DataFrame und CSV
nbrs_df = pd.DataFrame.from_records(records)
nbrs_df.to_csv(args.neighbors_out, index=False)
print("Export Nachbarschaft →", args.neighbors_out, "| Zeilen:", len(nbrs_df))
