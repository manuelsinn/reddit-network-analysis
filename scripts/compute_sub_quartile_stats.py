#!/usr/bin/env python
"""
---------------------------------------------------------------------------
compute_sub_decile_stats.py
---------------------------------------------------------------------------
Using the per-user rad-share CSV, create per-subreddit decile histograms:

    python scripts/compute_sub_quartile_stats.py --inputs filtered/cohorts/all_seed_users/cohort_20*.jsonl --radshare outputs/deciles/user_radshare.csv --out-users outputs/deciles/users_quartile_matrix.csv --out-comments outputs/deciles/comments_quartile_matrix.csv
---------------------------------------------------------------------------
Each output CSV has columns

subreddit,
users_decile_0, users_decile_1, … users_decile_9

(or comments_* respectively)
"""
import argparse, glob, io, csv, collections, pathlib, orjson, zstandard as zstd
from tqdm import tqdm
import pandas as pd

# ---------- CLI -----------------------------------------------------------
ap = argparse.ArgumentParser()
ap.add_argument("--inputs",      nargs="+", required=True,
                help="raw dumps (.zst / .jsonl)")
ap.add_argument("--radshare",    required=True,
                help="CSV from compute_rad_share_per_user.py")
ap.add_argument("--out-users",   required=True,
                help="CSV with user-level decile counts per sub")
ap.add_argument("--out-comments", required=True,
                help="CSV with comment-level decile counts per sub")
args = ap.parse_args()

# ---------- load user → decile map ----------------------------------------
df = pd.read_csv(args.radshare, usecols=["author","rad_share"])
rad_share_of = dict(zip(df.author, df.rad_share))
USER_SET   = set(rad_share_of.keys())

# ---------- accumulators ---------------------------------------------------
# statt users_dec / comments_dec je Dezil
users_q     = collections.defaultdict(lambda: [0, 0, 0, 0])   # [0-25, 25-50, 50-75, 75-100]
comments_q  = collections.defaultdict(lambda: [0, 0, 0, 0])
seen        = set()

# ---------- helper ---------------------------------------------------------
def line_stream(path):
    p = pathlib.Path(path)
    if p.suffix == ".zst":
        dctx = zstd.ZstdDecompressor(max_window_size=2**31-1)
        with p.open("rb") as fh, dctx.stream_reader(fh) as reader:
            text = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text:
                yield line
    else:
        with p.open() as fh:
            for line in fh:
                yield line

# ---------- PASS 2: count -----------------------------------------------
files = [f for pat in args.inputs for f in glob.glob(pat)]
for file in tqdm(files, desc="files", unit="file"):
    for raw in line_stream(file):
        try:
            obj = orjson.loads(raw)
        except Exception:
            continue
        author = obj.get("author")
        if author not in USER_SET:
            continue
        sub   = obj.get("subreddit")
        rs    = rad_share_of[author]      # echtes Float 0…1

        # -------- Quartilindex bestimmen ---------------------------------
        if   rs < 0.25:  q = 0
        elif rs < 0.50:  q = 1
        elif rs < 0.75:  q = 2
        else:            q = 3

        # -------- comments-Matrix (jede Zeile) ----------------------------
        comments_q[sub][q] += 1

        # -------- users-Matrix (einmal pro User×Sub×Quartil) -------------
        key = (author, sub, q)
        if key not in seen:
            users_q[sub][q] += 1
            seen.add(key)


# ---------- helper to flatten --------------------------------------------
def write_matrix(matrix, path, label):
    cols = [f"{label}_q0_25", f"{label}_q25_50",
            f"{label}_q50_75", f"{label}_q75_100"]
    with pathlib.Path(path).open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["subreddit"] + cols)
        for sub, vec in matrix.items():
            wr.writerow([sub] + vec)

write_matrix(users_q,    args.out_users,   "users")
write_matrix(comments_q, args.out_comments,"comments")

print("✔  wrote")
print("   ", args.out_users)
print("   ", args.out_comments)
