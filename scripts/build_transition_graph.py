#!/usr/bin/env python3
"""
build_transition_graph.py – Erzeugt einen gerichteten Übergangs‑Graphen aus
der von *user_drift_paths.py* erzeugten user_paths.csv.

Knoten  : Subreddits (einmalig angelegt)
Kanten  : A → B, wenn mindestens ein User zuerst in A und danach in B postet
Gewicht : standardmäßig #Users, die den Übergang vollzogen
Option   --weight transitions   → #Transitions (mehrfach Besuche gezählt)

Edge‑Attribute
--------------
weight_users        # eindeutige User
weight_transitions  # Komment‑Übergänge (optional)

Node‑Attribute
--------------
users_total         # #User, die den Sub überhaupt betreten
first_entries       # Anzahl erster Eintritte (erste Station)

Ausgabe
-------
GraphML oder GEXF (–format graphml|gexf), nutzbar in Gephi / NetworkX.

Beispiel
--------
python build_transition_graph.py \
  --paths-csv outputs/drifts/user_paths.csv \
  --output    outputs/drifts/transition_graph.gexf \
  --format    gexf \
  --weight    users

python scripts/build_transition_graph.py --paths-csv outputs/drifts/user_paths.csv --output outputs/drifts/transition_graph.gexf
"""
import argparse, collections, csv
from pathlib import Path
import pandas as pd
import networkx as nx

# ------------------------- CLI -------------------------------------------
cli = argparse.ArgumentParser("Transition‑Graph Builder")
cli.add_argument("--paths-csv", required=True, help="user_paths.csv aus Drift‑Skript")
cli.add_argument("--output", required=True, help="Dateiname (.gexf oder .graphml)")
cli.add_argument("--format", choices=["gexf","graphml"], default="gexf")
cli.add_argument("--weight", choices=["users","transitions"], default="users",
                help="Edge‑Weight: eindeutige User (default) oder alle Transitions")
args = cli.parse_args()

# ------------------------- Daten laden -----------------------------------
df = pd.read_csv(args.paths_csv, parse_dates=["first_ts_here"])
req_cols = {"author","subreddit","rel_days","comment_count"}
if not req_cols.issubset(df.columns):
    raise SystemExit(f"CSV fehlt Spalten: {req_cols - set(df.columns)}")

# Sortiere pro Author chronologisch (rel_days absteigend → älteste zuerst)
df_user_sorted = df.sort_values(["author","rel_days"], ascending=[True, False])

# ------------------------- Übergänge sammeln -----------------------------
edge_users = collections.defaultdict(set)      # (A,B) → {user1,user2}
edge_trans = collections.Counter()             # (A,B) → #Transitions
node_users = collections.defaultdict(set)      # Sub → {user}
first_entries = collections.Counter()          # Sub → count erster Eintritte

for author, grp in df_user_sorted.groupby("author", sort=False):
    # Liste der Subs in Besuchsreihenfolge
    subs = grp["subreddit"].tolist()
    # register nodes
    for s in subs:
        node_users[s].add(author)
    # erster Sub
    if subs:
        first_entries[subs[0]] += 1
    # Übergänge
    for src, dst in zip(subs, subs[1:]):
        edge_users[(src, dst)].add(author)
        edge_trans[(src, dst)] += 1

# ------------------------- Graph bauen -----------------------------------
G = nx.DiGraph()

for sub, userset in node_users.items():
    G.add_node(sub,
               users_total=len(userset),
               first_entries=first_entries[sub])

for (src, dst), userset in edge_users.items():
    G.add_edge(src, dst,
               weight_users=len(userset),
               weight_transitions=edge_trans[(src, dst)])

# Set main weight field
for u, v, data in G.edges(data=True):
    data["weight"] = data["weight_users"] if args.weight == "users" else data["weight_transitions"]

# ------------------------- Speichern -------------------------------------
path_out = Path(args.output)
path_out.parent.mkdir(parents=True, exist_ok=True)
if args.format == "gexf":
    nx.write_gexf(G, path_out)
else:
    nx.write_graphml(G, path_out)
print(f"Graph gespeichert → {path_out} | Nodes: {G.number_of_nodes()} Edges: {G.number_of_edges()}")
