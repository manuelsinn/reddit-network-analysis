#!/usr/bin/env python3
"""
Over-night batch:  n Monate  ×  m Seed-Subs  →  EIN GraphML
"""

import json, zstandard as zstd, io, pathlib, sys, time, hashlib
from collections import defaultdict

# ---------- Konfiguration  -------------------------------------------------
MONTH_FILES = [
    "raw/RC_2025-02.zst",
    "raw/RC_2025-03.zst"
]
SEEDS = {"MensRights", "marriedredpill", "PickUpArtist", "TheRedPill"}
RADICAL = SEEDS                                   # T0-Set, falls du später C-Variante nutzt
BAD_USERS = {"AutoModerator", "[deleted]"}        # wird überall ausgeschlossen

OUT_USERLIST = pathlib.Path("filtered/userlist.txt")
OUT_COHORT   = pathlib.Path("filtered/cohort_FULL.jsonl")
OUT_GRAPH    = pathlib.Path("outputs/manosphere_WINDOWED.graphml")

# ---------- Hilfsfunktionen  ------------------------------------------------
def stream_zst(path):
    """liefert Zeilen aus .zst"""
    d = zstd.ZstdDecompressor()
    with open(path, "rb") as fh, d.stream_reader(fh) as r:
        yield from io.TextIOWrapper(r, encoding="utf-8", errors="ignore")

# ---------- Pass 1 + 2: Seeds und Cohort in einem ---------------------------
# authors=set()
# OUT_COHORT.parent.mkdir(parents=True, exist_ok=True)
# with OUT_COHORT.open("w", encoding="utf-8") as cohort:
#     for zst in MONTH_FILES:
#         print(">> Scanning", zst)
#         for raw in stream_zst(zst):
#             try:
#                 rec = json.loads(raw)
#             except json.JSONDecodeError:
#                 continue
#             user = rec["author"]
#             if user in BAD_USERS:                        # Bot raus
#                 continue
#             # -- Seed-Check ---------------------------------------------------
#             if rec["subreddit"] in SEEDS:
#                 authors.add(user)                        # 1) User merken
#                 cohort.write(json.dumps({                # 2) Zeile sofort ins Cohort
#                     "author": user,
#                     "subreddit": rec["subreddit"],
#                     "created_utc": rec["created_utc"]
#                 }) + "\n")
#             # -- Cohort-Check (nur wenn wir schon wissen, dass user dazugehört)-
#             elif user in authors:
#                 cohort.write(json.dumps({
#                     "author": user,
#                     "subreddit": rec["subreddit"],
#                     "created_utc": rec["created_utc"]
#                 }) + "\n")

# print("Seed-+-Cohort-Pass fertig – Autoren:", len(authors))
# OUT_USERLIST.parent.mkdir(parents=True, exist_ok=True)
# OUT_USERLIST.write_text("\n".join(sorted(authors)))


# ---------- Pass 3 : Windowed Drift (A→B binnen 7 Tagen) -------------------
import networkx as nx, time, json, hashlib, sys, pathlib
from collections import defaultdict

WINDOW_DAYS = 7
WINDOW_SEC  = WINDOW_DAYS * 24 * 60 * 60
CHUNK_LOG   = 1_000_000               # Fortschrittsintervall

t0 = time.time()
rows = []                              # (uid, subreddit, ts)
line_ct = 0

print(">> Loading cohort for windowed graph …")
with OUT_COHORT.open("r", encoding="utf-8") as fh:
    for ln in fh:
        line_ct += 1
        if line_ct % CHUNK_LOG == 0:
            print(f"{line_ct:,} lines read [{time.time()-t0:.1f}s]",
                  file=sys.stderr)
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            continue
        uid = hashlib.sha256(rec["author"].encode()).hexdigest()
        rows.append((uid, rec["subreddit"], rec["created_utc"]))

print("Sorting …", file=sys.stderr)
rows.sort(key=lambda x: (x[0], x[2]))     # sort by uid + Zeit

edge_users = defaultdict(set)             # (A,B) -> {uid}
pair_ct = 0

print("Counting windowed transitions …", file=sys.stderr)
for (uid1, sub1, ts1), (uid2, sub2, ts2) in zip(rows, rows[1:]):
    if uid1 != uid2 or sub1 == sub2:
        continue                          # anderer User oder doppeltes Sub
    if 0 < ts2 - ts1 <= WINDOW_SEC:
        pair_ct += 1
        edge_users[(sub1, sub2)].add(uid1)
        if pair_ct % CHUNK_LOG == 0:
            print(f"{pair_ct:,} transitions found", file=sys.stderr)

# --------- Graph aufbauen --------------------------------------------------
G = nx.DiGraph()
for (a, b), uids in edge_users.items():
    G.add_edge(a, b, weight=float(len(uids)))

OUT_GRAPH.parent.mkdir(parents=True, exist_ok=True)
nx.write_graphml(G, OUT_GRAPH)
print("GraphML saved:", OUT_GRAPH,
      "| Nodes:", G.number_of_nodes(),
      "| Edges:", G.number_of_edges(),
      "| Runtime:", f"{time.time()-t0:.1f}s")
