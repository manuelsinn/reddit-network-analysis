#!/usr/bin/env python3
"""
Berechnet (relative) Target-Betweenness und Nachbarschafts-Analyse für ein gegebenes Netz.

Exports:
 1) A summary CSV with subreddit,tbc,rtbc,users_avg
 2) tb_neighbors.csv (target,neighbor,method,weight,jaccard,weight_prop,jaccard_prop,count,prop)
 3) Optional: Subgraph mit Top-20 + Targets + Nachbarn

Beispiel:

    python scripts/compute_target_betweennessNEW.py 
            --graph outputs/drifts/transition_graph.gexf 
            --targets MensRights,marriedredpill,TheRedPill,PickUpArtist 
            --out outputs/tb/tb_all.csv 
            --user-counts outputs/stats/sub_user_counts_average.csv
            --neighbors-out outputs/tb/tb_neighbors.csv
            --subgraph-out outputs/tb/tb_subgraph.gexf
    
    python scripts/compute_target_betweennessNEW.py --graph outputs/drifts/transition_graph.gexf --targets MensRights,marriedredpill,TheRedPill,PickUpArtist --out outputs/tb/tb_all.csv --user-counts outputs/stats/sub_user_counts_average.csv --neighbors-out outputs/tb/tb_neighbors.csv --subgraph-out outputs/tb/tb_subgraph.gexf
"""
import argparse
import csv
import sys
import networkx as nx
import time
import pathlib
from pathlib import Path
import pandas as pd
from collections import Counter
from tqdm import tqdm  


# ---------- CLI ------------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--graph", required=True, help="Graphdatei (.gexf, .graphml …)")
cli.add_argument("--targets", required=True,
                 help="Komma-Liste oder Pfad zu TXT mit 1 Node pro Zeile")
cli.add_argument("--out", required=True,
                 help="Betweenness-CSV")
cli.add_argument("--neighbors-out", default="outputs/tb/tb_neighbors.csv",
                 help="Nachbar-CSV für Top-20")
cli.add_argument("--subgraph-out", default=None,
                 help="Optional: Ausgabe-Datei für Subgraph mit Top20 + Targets + Nachbarn")
cli.add_argument("--weight", default="weight", help="Edge-Attribut für Pfadlänge")
cli.add_argument("--normalized", action="store_true", help="Normierung auf [0,1]")
cli.add_argument("--user-counts", required=True, help="CSV mit Spalten 'subreddit,users_avg' für rTBC-Nenner")

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


# ---------------------------------------------------------------------
# Vorarbeit:  Graph G mit Attributen 'weight' (user overlap)
#                              und 'jaccard' (edge similarity)
#
# targets = {...}           # deine Seed-Subs
# all_nodes = list(G.nodes) # oder spezifische source-Menge
# ---------------------------------------------------------------------
import collections, networkx as nx

# Initiale Dicts
tbc  = collections.Counter()
rtbc = collections.Counter()
MIN_USERS = 500


# Get average user counts per subreddit
df_users = (pd.read_csv(args.user_counts)
            .set_index("subreddit")["users_avg"])
user_total = df_users.to_dict()
print(f"[rTBC]  Lade Avg-User-Counts für {len(user_total):,} Subs")


# Einmal über alle kürzesten Pfade von irgendeinem Node → irgendein Target
all_nodes = list(G.nodes)

for src in tqdm(all_nodes, desc="Sources processed"):
    for tgt in TARGETS:
        if src == tgt or not nx.has_path(G, src, tgt):
            continue
        for path in nx.all_shortest_paths(G, src, tgt, weight=None):
            # alle Zwischenknoten (exkl. src, tgt)
            for i in range(1, len(path) - 1):
                node   = path[i]

                # add plain hit
                tbc[node]  += 1
                # relative Hit (schützt vor Masse-Bias)
                if user_total.get(node, 0) < MIN_USERS:
                    continue          # rTBC gar nicht berechnen
                den = user_total.get(node)          # kann None sein
                if not den:                         # None, 0 oder andere „Falsey“-Werte
                    continue

                rtbc[node] += 1 / max(den, 1)


# 4) DataFrame + CSV-Export
import pandas as pd
df_tbc = (pd.DataFrame({
            "subreddit": list(tbc.keys()),
            "tbc":  [tbc[n]  for n in tbc],
            "rtbc": [rtbc[n] for n in tbc],
            # "jtbc": [jtbc[n] for n in tbc],
          })
          .sort_values("tbc", ascending=False)
)


# ---------- Merging TBC with User Counts ---------------------------------

print("Merging Target-Betweenness mit User-Counts …")

# Merge auf 'subreddit'
df_full = (df_tbc
           .merge(df_users, on="subreddit", how="left")
           .sort_values("tbc", ascending=False))

# fehlende Userzahlen (falls es Subs im TBC gibt, aber nicht im User-CSV)
df_full["users_avg"].fillna(0, inplace=True)

# Ausgabe
Path(Path(args.out).parent).mkdir(parents=True, exist_ok=True)
df_full.to_csv(args.out, index=False)
print(f"✔  {len(df_full):,} Zeilen → {args.out}")


print("Top-10 plain TBC:\n", df_tbc.nlargest(10, "tbc")[["subreddit","tbc"]])
print("Top-10 rTBC:\n",      df_tbc.nlargest(10, "rtbc")[["subreddit","rtbc"]])



# ---------------------------------------------------------------------------
# 5) NEIGHBOR-Tabelle und (optional) Subgraph -------------------------------
# ---------------------------------------------------------------------------
if args.neighbors_out or args.subgraph_out:
    print("\nErzeuge Nachbarschaftsanalyse …")
    # ---------- Top-20 auswählen  (hier nach 'tbc'; ändere auf 'rtbc' wenn gewünscht)
    top20 = df_full.nlargest(20, "rtbc")["subreddit"].tolist()
    print("Top-20:", top20)

    records = []
    pairs   = [(u, v) for i, u in enumerate(top20) for v in top20[i + 1:]]
    for target in top20:
        # ---------- 5a) Direkte Kanten zu *anderen* Targets -----------------
        direct_edges = [
            (u, v, d)
            for u, v, d in G.edges(target, data=True)
            if (u == target and v in TARGETS) or (v == target and u in TARGETS)
        ]
        if direct_edges:
            tot_w = sum(d.get("weight", 0)   for _, _, d in direct_edges) or 1
            tot_j = sum(d.get("jaccard", 0)  for _, _, d in direct_edges) or 1
            for u, v, d in direct_edges:
                nei = v if u == target else u
                w   = d.get("weight", 0)
                j   = d.get("jaccard", 0)
                records.append({
                    "target": target, "neighbor": nei, "method": "direct",
                    "weight": w, "jaccard": j,
                    "weight_prop": w / tot_w,
                    "jaccard_prop": j / tot_j,
                    "count": None, "prop": None
                })
        else:
            # ---------- 5b) Knoten liegt nur *auf Pfaden* -------------------
            counter = Counter()
            for u, v in pairs:
                if target in (u, v):
                    continue
                for path in nx.all_shortest_paths(G, u, v, weight=args.weight):
                    if target in path:
                        idx = path.index(target)
                        if idx > 0:
                            counter[path[idx - 1]] += 1
                        if idx < len(path) - 1:
                            counter[path[idx + 1]] += 1
            tot_c = sum(counter.values()) or 1
            for nei, cnt in counter.items():
                records.append({
                    "target": target, "neighbor": nei, "method": "path",
                    "weight": None, "jaccard": None,
                    "weight_prop": None, "jaccard_prop": None,
                    "count": cnt, "prop": cnt / tot_c
                })

    # ---------- 5c) CSV schreiben ------------------------------------------
    if args.neighbors_out:
        nbr_df = pd.DataFrame.from_records(records)
        nbr_df.to_csv(args.neighbors_out, index=False)
        print("→", args.neighbors_out, "| Zeilen:", len(nbr_df))

    # ---------- 5d) Optional Subgraph --------------------------------------
    print("\nErzeuge Subgraph …")    
    if args.subgraph_out:
        sub_nodes = set(top20) | TARGETS | {r["neighbor"] for r in records}
        SG = G.subgraph(sub_nodes).copy()
        if Path(args.subgraph_out).suffix.lower() in (".gexf", ".gz"):
            nx.write_gexf(SG, args.subgraph_out)
        else:
            nx.write_graphml(SG, args.subgraph_out)
        print("→ Subgraph:", len(SG.nodes()), "Nodes –", len(SG.edges()),
              "Edges  ⇒", args.subgraph_out)


