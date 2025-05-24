#!/usr/bin/env python
"""
---------------------------------------------------------------------------
add_deciles_to_graph.py
---------------------------------------------------------------------------
Attach decile-vectors from CSVs to an existing overlap graph.

Example:
    python scripts/add_deciles_to_graph.py
        --graph         outputs/overlap/overlap_allseedusers_filtered30_.05.gexf
        --users-csv     outputs/deciles/users_decile_matrix.csv
        --comments-csv  outputs/deciles/comments_decile_matrix.csv
        --out           outputs/overlap/overlap_graph_with_deciles.gexf

    python scripts/add_deciles_to_graph.py --graph         outputs/overlap/overlap_allseedusers_filtered30_.05.gexf --users-csv     outputs/deciles/users_decile_matrix.csv --comments-csv  outputs/deciles/comments_decile_matrix.csv --out           outputs/overlap/overlap_graph_with_deciles.gexf
---------------------------------------------------------------------------
"""
import argparse, pathlib, pandas as pd, networkx as nx
from tqdm import tqdm

# ---------- CLI -----------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--graph",        required=True,
                 help="input GEXF / GraphML")
cli.add_argument("--users-csv",    required=True,
                 help="users_decile_matrix.csv (mandatory)")
cli.add_argument("--comments-csv", required=False,
                 help="comments_decile_matrix.csv (optional)")
cli.add_argument("--out",          required=True,
                 help="output graph with new attributes")
args = cli.parse_args()

# ---------- load graph ----------------------------------------------------
print("Loading graph …")
G = nx.read_gexf(args.graph) if args.graph.endswith(".gexf") else nx.read_graphml(args.graph)
print(f"  nodes: {len(G):,}   edges: {G.number_of_edges():,}")

# ---------- load decile tables -------------------------------------------
u_df = pd.read_csv(args.users_csv).set_index("subreddit")
c_df = (pd.read_csv(args.comments_csv).set_index("subreddit")
        if args.comments_csv else None)

# ---------- attach attributes --------------------------------------------
for node in tqdm(G.nodes, desc="annotating"):
    if node not in u_df.index:
        # fill zeros for unseen subs
        for d in range(10):
            G.nodes[node][f"users_decile_{d}"] = 0
            if c_df is not None:
                G.nodes[node][f"comments_decile_{d}"] = 0
        G.nodes[node]["users_total"] = 0
        continue

    u_row = u_df.loc[node]
    total = int(u_row.filter(like="users_decile_").sum())
    G.nodes[node]["users_total"] = total
    for d in range(10):
        G.nodes[node][f"users_decile_{d}"] = int(u_row[f"users_decile_{d}"])

    if c_df is not None and node in c_df.index:
        c_row = c_df.loc[node]
        for d in range(10):
            G.nodes[node][f"comments_decile_{d}"] = int(c_row[f"comments_decile_{d}"])

# ---------- write graph ---------------------------------------------------
path_out = pathlib.Path(args.out)
path_out.parent.mkdir(parents=True, exist_ok=True)

if path_out.suffix.lower() == ".gexf":
    nx.write_gexf(G, path_out)
else:
    nx.write_graphml(G, path_out)

print(f"✔  saved   {path_out}   ({len(G):,} nodes)")
