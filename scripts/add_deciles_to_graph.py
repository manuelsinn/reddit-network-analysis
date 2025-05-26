#!/usr/bin/env python
"""
---------------------------------------------------------------------------
add_deciles_to_graph.py
---------------------------------------------------------------------------
Attach decile-vectors from CSVs to an existing overlap graph.

Example:

    DECILES: python scripts/add_deciles_to_graph.py --graph outputs/overlap/overlap_allseedusers_filtered30_.05.gexf --users-csv     outputs/deciles/users_decile_matrix.csv --comments-csv  outputs/deciles/comments_decile_matrix.csv --out           outputs/overlap/overlap_graph_with_deciles.gexf
    QUARTILES: python scripts/add_deciles_to_graph.py --graph outputs/overlap/overlap_allseedusers_filtered30_.05.gexf --users-csv     outputs/deciles/users_quartile_matrix.csv --comments-csv  outputs/deciles/comments_quartile_matrix.csv --out           outputs/overlap/overlap_graph_with_quartiles.gexf
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
uq_df = pd.read_csv(args.users_csv).set_index("subreddit")   # 4 Spalten


global_high_users=0; global_users=0
global_q_sum = [0, 0, 0, 0]   # Sammeln pro Quartil
global_users = 0
global_ge25_users = 0
global_ge50_users = 0
global_ge75_users = 0

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

    # add user deciles absolute
    # u_row = u_df.loc[node]
    # total = int(u_row.filter(like="users_decile_").sum())
    # G.nodes[node]["users_total"] = total
    # for d in range(10):
    #     G.nodes[node][f"users_decile_{d}"] = int(u_row[f"users_decile_{d}"])


    # -----------------------------------------------------------------
    #  Quartil-Vektoren (absolute + Anteil)
    # -----------------------------------------------------------------
    u_row = uq_df.loc[node] if node in uq_df.index else [0, 0, 0, 0]
    q0, q1, q2, q3 = map(int, u_row) 
    tot_users = q0 + q1 + q2 + q3 or 1      # niemals 0

    # absolute user counts per quartile
    G.nodes[node]["users_q0_25"]   = q0
    G.nodes[node]["users_q25_50"]  = q1
    G.nodes[node]["users_q50_75"]  = q2
    G.nodes[node]["users_q75_100"] = q3

    #relative user shares per quartile
    G.nodes[node]["share_q0_25"]   = q0 / tot_users
    G.nodes[node]["share_q25_50"]  = q1 / tot_users
    G.nodes[node]["share_q50_75"]  = q2 / tot_users
    G.nodes[node]["share_q75_100"] = q3 / tot_users

    # ---------- globale Summen für spätere Lift-Berechnung ----------
    global_q_sum[0] += q0
    global_q_sum[1] += q1
    global_q_sum[2] += q2
    global_q_sum[3] += q3
    global_users    += tot_users

    # --------- additive shares & globale Summen ----------------------
    share_ge25 = (q1 + q2 + q3) / tot_users     # share of users who post min. 25% of their comments in seed subs
    share_ge50 = (q2 + q3)      / tot_users
    share_ge75 =  q3            / tot_users

    G.nodes[node]["share_ge25"] = round(share_ge25, 4)
    G.nodes[node]["share_ge50"] = round(share_ge50, 4)
    G.nodes[node]["share_ge75"] = round(share_ge75, 4)

    global_ge25_users += (q1 + q2 + q3)
    global_ge50_users += (q2 + q3)
    global_ge75_users += q3


    # # -------- absolute Quartile --------------------------------------
    # u_vec = uq_df.loc[node] if node in uq_df.index else [0,0,0,0]
    # q0,q1,q2,q3 = map(int, u_vec)
    # G.nodes[node]["users_q0_25"]   = q0
    # G.nodes[node]["users_q25_50"]  = q1
    # G.nodes[node]["users_q50_75"]  = q2
    # G.nodes[node]["users_q75_100"] = q3

    # tot = q0+q1+q2+q3 or 1
    # G.nodes[node]["share_q75_100"] = round(q3 / tot, 4)

    # -------- globale Summen für Lift --------------------------------
    # global_high_users += q3
    # global_users      += tot


    # add share (percentage) for every decile 
    # total_users = G.nodes[node]["users_total"]
    # for d in range(10):
    #     abs_val = G.nodes[node][f"users_decile_{d}"]
    #     if total_users:
    #         share = abs_val / total_users
    #     else:
    #         share = 0.0
    #     G.nodes[node][f"share_users_decile_{d}"] = round(share, 4)  # 4 Nachkommastellen

    # # add comments absolute
    # if c_df is not None and node in c_df.index:
    #     c_row = c_df.loc[node]
    #     for d in range(10):
    #         G.nodes[node][f"comments_decile_{d}"] = int(c_row[f"comments_decile_{d}"])

    #     # add comment share %
    #     total_comments = sum(
    #         G.nodes[node].get(f"comments_decile_{d}", 0) for d in range(10)
    #     )
    #     for d in range(10):
    #         abs_com = G.nodes[node].get(f"comments_decile_{d}", 0)
    #         share_c = abs_com / total_comments if total_comments else 0.0
    #         G.nodes[node][f"share_comments_decile_{d}"] = round(share_c, 4)

# Baseline & Lift
baseline_q = [q / global_users if global_users else 1e-9 for q in global_q_sum]

for n in G.nodes:
    for i, label in enumerate(("q0_25", "q25_50", "q50_75", "q75_100")):
        share = G.nodes[n][f"share_{label}"]
        lift  = share / baseline_q[i] if baseline_q[i] else 0
        G.nodes[n][f"lift_{label}"] = round(lift, 3)
        
baseline_ge25 = global_ge25_users / global_users if global_users else 1e-9
baseline_ge50 = global_ge50_users / global_users if global_users else 1e-9
baseline_ge75 = global_ge75_users / global_users if global_users else 1e-9

for n in G.nodes:
    G.nodes[n]["lift_ge25"] = round(G.nodes[n]["share_ge25"] / baseline_ge25, 3)
    G.nodes[n]["lift_ge50"] = round(G.nodes[n]["share_ge50"] / baseline_ge50, 3)
    G.nodes[n]["lift_ge75"] = round(G.nodes[n]["share_ge75"] / baseline_ge75, 3)


# ---------- write graph ---------------------------------------------------
path_out = pathlib.Path(args.out)
path_out.parent.mkdir(parents=True, exist_ok=True)

if path_out.suffix.lower() == ".gexf":
    nx.write_gexf(G, path_out)
else:
    nx.write_graphml(G, path_out)

print(f"Graph saved to:     {path_out}   ({len(G):,} nodes)")
