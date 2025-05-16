#!/usr/bin/env python3
"""
Berechnet Target-Betweenness und Nachbarschafts-Analyse für ein gegebenes Netz

Exports:
 1) target_btw.csv (id,label,target_btw)
 2) top10_targets_neighbors.csv (target,neighbor,method,weight,jaccard,weight_prop,jaccard_prop,count,prop)
 3) Optional: Subgraph mit Top-10 + Targets + Nachbarn

Beispiel:
    python scripts/compute_target_betweenness.py \
          --graph outputs/overlap_Q4.gexf \
          --targets targets.txt \
          --out tb_Q4.csv \
          --neighbors-out tb_Q4_neighbors.csv \
          --subgraph-out tb_Q4_subgraph.gexf

    python scripts/compute_target_betweenness2.py --graph outputs/overlap_total.gexf --targets MensRights,marriedredpill,TheRedPill,PickUpArtist --out outputs/tb_total.csv --neighbors-out outputs/tb_total_neighbors.csv --subgraph-out tb_total_subgraph.gexf
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
cli.add_argument("--graph", required=True, help="Graphdatei (.gexf, .graphml …)")
cli.add_argument("--targets", required=True,
                 help="Komma-Liste oder Pfad zu TXT mit 1 Node pro Zeile")
cli.add_argument("--out", default="target_btw.csv",
                 help="Betweenness-CSV")
cli.add_argument("--neighbors-out", default="top10_targets_neighbors.csv",
                 help="Nachbar-CSV für Top-10")
cli.add_argument("--subgraph-out", default=None,
                 help="Optional: Ausgabe-Datei für Subgraph mit Top10 + Targets + Nachbarn")
cli.add_argument("--weight", default="weight", help="Edge-Attribut für Pfadlänge")
cli.add_argument("--normalized", action="store_true", help="Normierung auf [0,1]")
args = cli.parse_args()

# ---------- Targets lesen --------------------------------------------------
if pathlib.Path(args.targets).is_file():
    TARGETS = {ln.strip() for ln in open(args.targets) if ln.strip()}
else:
    TARGETS = {x.strip() for x in args.targets.split(",") if x.strip()}
print(f"Targets ({len(TARGETS)}): {TARGETS}")

# ---------- Graph laden ----------------------------------------------------
ext = pathlib.Path(args.graph).suffix.lower()
G = nx.read_gexf(args.graph) if ext in ('.gexf','.gz') else nx.read_graphml(args.graph)
print(f"Graph: {G.number_of_nodes()} Nodes, {G.number_of_edges()} Edges")

# ---------- Betweenness berechnen -----------------------------------------
sources = set(G.nodes())
targets = TARGETS & sources
missing = TARGETS - targets
if missing:
    print("Warnung: fehlende Targets:", missing, file=sys.stderr)
print("Berechne Target-Betweenness …")
t0 = time.time()
tb = nx.betweenness_centrality_subset(
    G, sources=sources, targets=targets,
    weight=args.weight, normalized=args.normalized)
print(f"Dauer: {time.time()-t0:.1f}s")

# ---------- CSV Betweenness -----------------------------------------------
with open(args.out, 'w', newline='', encoding='utf-8') as fh:
    w = csv.writer(fh)
    w.writerow(['id','label','target_btw'])
    for node, score in sorted(tb.items(), key=lambda x: x[1], reverse=True):
        label = G.nodes[node].get('label', node)
        w.writerow([node,label,f'{score:.6f}'])
print("->", args.out)

# ---------- Top-10-Auswahl ------------------------------------------------
top10 = [node for node,_ in sorted(tb.items(), key=lambda x: x[1], reverse=True)[:10]]
print("Top-10 Targets:", top10)

# ---------- Nachbarschaftsanalyse -----------------------------------------
records = []
pairs = [(u,v) for i,u in enumerate(top10) for v in top10[i+1:]]
for target in top10:
    # Direkte Kanten **nur** zu anderen Targets
    direct_edges = [(u,v,data) for u,v,data in G.edges(target, data=True)
                    if (u==target and v in targets) or (v==target and u in targets)]
    if direct_edges:
        total_w = sum(data.get('weight', 0) for _,_,data in direct_edges)
        total_j = sum(data.get('jaccard', 0) for _,_,data in direct_edges)
        for u,v,data in direct_edges:
            neighbor = v if u==target else u
            w = data.get('weight',0); j = data.get('jaccard',0)
            records.append({
                'target':target, 'neighbor':neighbor, 'method':'direct',
                'weight':w, 'jaccard':j,
                'weight_prop': w/total_w if total_w else 0,
                'jaccard_prop': j/total_j if total_j else 0,
                'count':None, 'prop':None
            })
    else:
        # Kürzeste Pfade über target
        counter = Counter()
        for u,v in pairs:
            if target in (u,v): continue
            for path in nx.all_shortest_paths(G, u, v, weight=args.weight):
                if target in path:
                    idx = path.index(target)
                    if idx>0:                counter[path[idx-1]] += 1
                    if idx<len(path)-1:    counter[path[idx+1]] += 1
        total_c = sum(counter.values()) or 1
        for neigh,cnt in counter.items():
            records.append({
                'target':target, 'neighbor':neigh, 'method':'path',
                'weight':None,'jaccard':None,'weight_prop':None,'jaccard_prop':None,
                'count':cnt, 'prop':cnt/total_c
            })
# DataFrame und CSV speichern
nbrs_df = pd.DataFrame.from_records(records)
nbrs_df.to_csv(args.neighbors_out, index=False)
print("->", args.neighbors_out)

# ---------- Optional: Subgraph erstellen ----------------------------------
if args.subgraph_out:
    sub_nodes = set(top10) | targets | set(nbrs_df['neighbor'])
    SG = G.subgraph(sub_nodes).copy()
    ext_out = pathlib.Path(args.subgraph_out).suffix.lower()
    if ext_out in ('.gexf','.gz'): nx.write_gexf(SG, args.subgraph_out)
    else:                  nx.write_graphml(SG, args.subgraph_out)
    print("-> Subgraph mit", len(SG.nodes()),"Nodes und",len(SG.edges()),"Edges →",args.subgraph_out)
