#!/usr/bin/env python
"""
---------------------------------------------------------------------------
compute_sub_decile_stats.py
---------------------------------------------------------------------------
Using the per-user rad-share CSV, create per-subreddit decile histograms:

    python scripts/compute_sub_decile_stats.py
        --inputs filtered/cohorts/all_seed_users/cohort_*.jsonl
        --radshare    outputs/deciles/user_radshare.csv
        --out-users   outputs/deciles/users_decile_matrix.csv
        --out-comments outputs/deciles/comments_decile_matrix.csv
    
    python scripts/compute_sub_decile_stats.py --inputs filtered/cohorts/all_seed_users/cohort_20*.jsonl --radshare outputs/deciles/user_radshare.csv --out-users   outputs/deciles/users_decile_matrix.csv --out-comments outputs/deciles/comments_decile_matrix.csv
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
df = pd.read_csv(args.radshare, usecols=["author","decile"])
decile_of = dict(zip(df.author, df.decile))
USER_SET   = set(decile_of.keys())

# ---------- accumulators ---------------------------------------------------
users_dec   = collections.defaultdict(collections.Counter)   # {sub: Counter(decile)}
comments_dec= collections.defaultdict(collections.Counter)
seen        = set()  # (author, sub, decile)  to deduplicate

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
        dec   = int(decile_of[author])
        # ---- comments matrix (every line counts) -------------------------
        comments_dec[sub][dec] += 1
        # ---- users matrix (deduplicated) ---------------------------------
        key = (author, sub, dec)
        if key not in seen:
            users_dec[sub][dec] += 1
            seen.add(key)

# ---------- helper to flatten --------------------------------------------
def write_matrix(matrix, path, label):
    cols = [f"{label}_decile_{d}" for d in range(10)]
    with pathlib.Path(path).open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["subreddit"] + cols)
        for sub, counter in matrix.items():
            row = [counter.get(d, 0) for d in range(10)]
            wr.writerow([sub] + row)

write_matrix(users_dec,   args.out_users,   "users")
write_matrix(comments_dec,args.out_comments,"comments")

print("✔  wrote")
print("   ", args.out_users)
print("   ", args.out_comments)
