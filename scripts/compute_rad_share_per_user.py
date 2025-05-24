#!/usr/bin/env python
"""
---------------------------------------------------------------------------
compute_rad_share_per_user.py
---------------------------------------------------------------------------
Create a CSV with, for every seed-poster, the share of comments written in
seed subreddits.

    python scripts/compute_rad_share_per_user.py
        --inputs filtered/cohorts/all_seed_users/cohort_*.jsonl       # Reddit dumps, filtered or raw 
        --seed-subs TheRedPill,MensRights,marriedredpill,PickUpArtist       # or path to TXT
        --seed-users filtered/userlists/all_seed_users.txt                  # one author per line
        --min-comments 10                                                   # (optional) drop low-volume accts
        --out outputs/user_radshare.csv

    python scripts/compute_rad_share_per_user.py --inputs filtered/cohorts/all_seed_users/cohort_all.jsonl --seed-subs TheRedPill,MensRights,marriedredpill,PickUpArtist --seed-users filtered/userlists/all_seed_users.txt --min-comments 10 --out outputs/user_radshare.csv
---------------------------------------------------------------------------
CSV columns
-----------
author,total_comments,seed_comments,rad_share,decile
"""
# --------------- imports --------------------------------------------------
import argparse, glob, io, csv, sys, pathlib, orjson, zstandard as zstd
from tqdm import tqdm

# --------------- CLI ------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--inputs",      nargs="+", required=True,
                 help="raw dumps (.zst, .jsonl) – glob wildcards allowed")
cli.add_argument("--seed-subs",   required=True,
                 help="comma list OR path to TXT with radical sub names")
cli.add_argument("--seed-users",  required=True,
                 help="TXT file with one author per line (all_seed_users)")
cli.add_argument("--min-comments", type=int, default=0,
                 help="drop users with < N total comments (default: 0 = keep all)")
cli.add_argument("--out",         required=True,
                 help="output CSV file")
args = cli.parse_args()

# --------------- seed lists -----------------------------------------------
def load_list(maybe_path: str):
    """Return set of strings from comma list OR plaintext file."""
    path = pathlib.Path(maybe_path)
    if path.exists():
        return {ln.strip() for ln in path.open()}
    return {x.strip() for x in maybe_path.split(",") if x.strip()}

SEED_SUBS   = load_list(args.seed_subs)
SEED_USERS  = load_list(args.seed_users)

# --------------- accumulators ---------------------------------------------
total     = {}          # author -> total comments
seedcount = {}          # author -> comments in seed subs

# --------------- streaming helper -----------------------------------------
def line_stream(path):
    """Yield text lines from .zst or plain file."""
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

# --------------- PASS 1: count comments -----------------------------------
files = [f for pat in args.inputs for f in glob.glob(pat)]
for file in tqdm(files, desc="files", unit="file"):
    for raw in line_stream(file):
        try:
            obj = orjson.loads(raw)
        except Exception:
            continue
        a   = obj.get("author")
        if a not in SEED_USERS:
            continue
        sub = obj.get("subreddit")
        total[a]      = total.get(a, 0) + 1
        if sub in SEED_SUBS:
            seedcount[a] = seedcount.get(a, 0) + 1

# --------------- write CSV ------------------------------------------------
out = pathlib.Path(args.out)
out.parent.mkdir(parents=True, exist_ok=True)

with out.open("w", newline="", encoding="utf-8") as fh:
    wr = csv.writer(fh)
    wr.writerow(["author","total_comments","seed_comments","rad_share","decile"])
    for a, tot in total.items():
        if tot < args.min_comments:
            continue
        sc  = seedcount.get(a, 0)
        rs  = sc / tot
        dec = min(int(rs * 10), 9)       # 0 … 9
        wr.writerow([a, tot, sc, f"{rs:.5f}", dec])

print(f"✔  wrote {out}  |  authors kept: {sum(t>=args.min_comments for t in total.values())}")
