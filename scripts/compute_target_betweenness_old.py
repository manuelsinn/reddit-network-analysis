#!/usr/bin/env python3
"""
Berechnet Target-Betweenness für ein gegebenes Netz und exportiert
eine CSV (id,label,target_btw) – ideal zum Merge in Gephi.

Beispiel:
    python scripts/compute_target_betweenness.py --graph outputs/overlap_Q4-no12.gexf --targets MensRights,marriedredpill,TheRedPill,PickUpArtist --out outputs/tb_Q4-no12.csv
"""

import argparse, csv, sys, networkx as nx, time, pathlib

# ---------- CLI ------------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--graph",  required=True,
                 help="Graphdatei (.gexf, .graphml, .gexf.gz …)")
cli.add_argument("--targets", required=True,
                 help="Komma-Liste oder Pfad zu TXT mit 1 Node pro Zeile")
cli.add_argument("--out", default="target_btw.csv",
                 help="Name der Ausgabe-CSV (Default target_btw.csv)")
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
if ext == ".gexf" or ext == ".gz":
    G = nx.read_gexf(args.graph)
else:
    G = nx.read_graphml(args.graph)
print(f"Graph geladen: {G.number_of_nodes():,} Nodes, "
      f"{G.number_of_edges():,} Edges  [{time.time()-t0:.1f}s]")

# ---------- Target-Betweenness --------------------------------------------
sources = set(G.nodes())                 # alle Nodes als Quellen
targets = TARGETS & set(G.nodes())       # sicherstellen, dass sie existieren
missing = TARGETS - targets
if missing:
    print("Warnung: folgende Targets fehlen im Netz:", ", ".join(missing),
          file=sys.stderr)

print("Berechne Target-Betweenness …")
t0 = time.time()
tb = nx.betweenness_centrality_subset(
        G, sources=sources, targets=targets,
        weight=args.weight, normalized=args.normalized)
print(f"Fertig  [{time.time()-t0:.1f}s]")

# ---------- CSV schreiben --------------------------------------------------
with open(args.out, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["id", "label", "target_btw"])
    for node, score in tb.items():
        label = G.nodes[node].get("label", node)
        w.writerow([node, label, f"{score:.6f}"])

print("Export →", args.out, "| Zeilen:", len(tb))



# ---------- Top 10 Berechnung und Kantenexploration ------------------------------------

top10 = ...

