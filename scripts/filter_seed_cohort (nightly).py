#!/usr/bin/env python3
"""
Over-night batch:  n Monate  ×  m Seed-Subs  →  EIN GraphML
"""

import json, zstandard as zstd, io, pathlib, sys, time, hashlib
from collections import defaultdict

# ---------- Konfiguration  -------------------------------------------------
MONTH_FILES = [
    "raw/RC_2024-09.zst",
    "raw/RC_2024-10.zst",
    "raw/RC_2024-11.zst",
    # "raw/RC_2025-01.zst"
]
SEEDS = {"MensRights", "marriedredpill", "PickUpArtist", "TheRedPill"}
RADICAL = SEEDS                                   # T0-Set, falls du später C-Variante nutzt
BAD_USERS = {"AutoModerator", "[deleted]"}        # wird überall ausgeschlossen


OUT_USERLIST = pathlib.Path("filtered/userlist_sep_til_nov.txt")
OUT_COHORT   = pathlib.Path("filtered/cohort__sep_til_nov.jsonl")
OUT_GRAPH    = pathlib.Path("outputs/manosphere__overlap_sep_til_nov.gexf")

# ---------- Hilfsfunktionen  ------------------------------------------------
def stream_zst(path):
    """liefert Zeilen aus .zst"""
    d = zstd.ZstdDecompressor()
    with open(path, "rb") as fh, d.stream_reader(fh) as r:
        yield from io.TextIOWrapper(r, encoding="utf-8", errors="ignore")

# ---------- Pass 1 + 2: Seeds und Cohort in einem ---------------------------
authors=set()
OUT_COHORT.parent.mkdir(parents=True, exist_ok=True)
with OUT_COHORT.open("w", encoding="utf-8") as cohort:
    for zst in MONTH_FILES:
        print(">> Scanning", zst)
        for raw in stream_zst(zst):
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                continue
            user = rec["author"]
            if user in BAD_USERS:                        # Bot raus
                continue
            # -- Seed-Check ---------------------------------------------------
            if rec["subreddit"] in SEEDS:
                authors.add(user)                        # 1) User merken
                cohort.write(json.dumps({                # 2) Zeile sofort ins Cohort
                    "author": user,
                    "subreddit": rec["subreddit"],
                    "created_utc": rec["created_utc"]
                }) + "\n")
            # -- Cohort-Check (nur wenn wir schon wissen, dass user dazugehört)-
            elif user in authors:
                cohort.write(json.dumps({
                    "author": user,
                    "subreddit": rec["subreddit"],
                    "created_utc": rec["created_utc"]
                }) + "\n")

print("Seed-+-Cohort-Pass fertig – Autoren:", len(authors))
OUT_USERLIST.parent.mkdir(parents=True, exist_ok=True)
OUT_USERLIST.write_text("\n".join(sorted(authors)))




# ---------- Pass 3 : FAST Overlap + Fortschritts-Counter  -------------------
import json, time, itertools, numpy as np
import networkx as nx
from collections import defaultdict, Counter

COHORT_FILE = "filtered/cohort_FULL.jsonl"

LINE_LOG  = 1_000_000    # alle 1 M Zeilen eine Meldung
USER_LOG  = 100_000      # alle 100 k User eine Meldung
PAIR_LOG  = 5_000_000    # alle 5 M Paar-Updates eine Meldung

t0 = time.time()

# --- 1) Sub‐IDs auf Integer mappen ----------------------------------------
sub2id, id2sub = {}, []
def sid(name: str) -> int:
    if name not in sub2id:
        sub2id[name] = len(sub2id)
        id2sub.append(name)
    return sub2id[name]

user_index = defaultdict(set)          # author -> set(int subID)

print(">> Building user → subs index …")
with open(COHORT_FILE, encoding="utf-8") as fh:
    for i, ln in enumerate(fh, 1):
        if i % LINE_LOG == 0:
            print(f"{i:,} lines read  [{time.time()-t0:.1f}s]")
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            continue
        user_index[rec["author"]].add(sid(rec["subreddit"]))

print(f"Index ready – users: {len(user_index):,}  "
      f"| elapsed {time.time()-t0:.1f}s")

# --- 2) Overlap-Zählen -----------------------------------------------------
edge_cnt = Counter()
sub_usercount = Counter()
pair_updates = 0

for u_idx, subs in enumerate(user_index.values(), 1):
    if u_idx % USER_LOG == 0:
        print(f"{u_idx:,} users processed  "
              f"| unique pairs so far: {len(edge_cnt):,}  "
              f"[{time.time()-t0:.1f}s]")
    if len(subs) < 2:
        continue
    # user -> alle 2er-Kombis durchgehen (NumPy beschleunigt itertools)
    for s in subs:
        sub_usercount[s] += 1
    arr = np.fromiter(subs, dtype=np.uint32)
    for a, b in itertools.combinations(arr, 2):
        if a > b:                       # Paar sortieren (a < b)
            a, b = b, a
        edge_cnt[(a, b)] += 1
        pair_updates += 1
        if pair_updates % PAIR_LOG == 0:
            print(f"{pair_updates:,} pair updates  "
                  f"| unique pairs: {len(edge_cnt):,}")

print(f"Counting done – unique pairs: {len(edge_cnt):,}  "
      f"| total time {time.time()-t0:.1f}s")

# --- 3) Graph schreiben ----------------------------------------------------

THRESH_WEIGHT  = 30         # nur sinnvolle Overlaps
THRESH_JACCARD = 0.05       # filtert Mainstream-Außenäste

G = nx.Graph()
for (a, b), overlap in edge_cnt.items():
    if overlap < THRESH_WEIGHT:
        continue
    union = sub_usercount[a] + sub_usercount[b] - overlap # weil sonst doppelt
    j     = overlap / union
    if j < THRESH_JACCARD:
        continue
    G.add_edge(id2sub[a], id2sub[b],
               weight=float(overlap),
               jaccard=round(j, 4))

nx.write_gexf(G, OUT_GRAPH, prettyprint=False)


# nx.write_graphml(G, OUT_GRAPH)

# STATTDESSEN:
# # edge_cnt wie gehabt
# node_file = 'outputs/nodes.csv'
# edge_file = 'outputs/edges.csv'

# # Nodes-Tabelle (optional – für Labels)
# with open(node_file, 'w') as f:
#     f.write('id,label\n')
#     for idx, name in enumerate(id2sub):
#         f.write(f'{name},{name}\n')

# # Edge-Tabelle
# with open(edge_file, 'w') as f:
#     f.write('source,target,weight\n')
#     for (a, b), w in edge_cnt.items():
#         f.write(f'{id2sub[a]},{id2sub[b]},{w}\n')
# print("CSV-Files ready – importieren: Gephi → File → Import Spreadsheet")




print(f"GraphML saved: {OUT_GRAPH}  "
      f"| Nodes: {G.number_of_nodes():,}  "
      f"| Edges: {G.number_of_edges():,}  "
      f"| total runtime {time.time()-t0:.1f}s")
