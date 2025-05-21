#!/usr/bin/env python
# scripts/compute_users_per_sub.py
# ------------------------------------------------------------------
#  pip install zstandard orjson tqdm
# ------------------------------------------------------------------
import argparse, glob, re, io, collections, orjson, zstandard as zstd
from pathlib import Path
from tqdm import tqdm

# ---------- CLI ----------
ap = argparse.ArgumentParser()
ap.add_argument("--inputs",  nargs="+", required=True,
                help="*.zst raw comment dumps (glob ok)")
ap.add_argument("--out", required=True,
                help="CSV output file (month,subreddit,users_total)")
args = ap.parse_args()

# ---------- helpers ----------
month_re = re.compile(r"(\d{4}-\d{2})")       # RC_YYYY-MM.zst
def month_from(fname):                        # → 'YYYY-MM'
    m = month_re.search(Path(fname).name)
    return m.group(1) if m else "unknown"

# ---------- main ----------
rows = []
all_files = [p for pattern in args.inputs for p in glob.glob(pattern)]
for file in tqdm(all_files, desc="Files", unit="file"):
    month, subs = month_from(file), collections.defaultdict(set)

    with open(file, "rb") as fh:
        dctx   = zstd.ZstdDecompressor(max_window_size=2**31-1)
        with dctx.stream_reader(fh) as stream:
            text = io.TextIOWrapper(stream, encoding="utf-8")

            # Inner progress-bar für Zeilen
            for line in tqdm(text, desc=f"{Path(file).name}", leave=False,
                             unit="line"):
                try:
                    obj = orjson.loads(line)
                    subs[obj["subreddit"]].add(obj["author"])
                except Exception:
                    continue      # skip malformed

    for sub, users in subs.items():
        rows.append((month, sub, len(users)))

# ---------- CSV ----------
Path(Path(args.out).parent).mkdir(parents=True, exist_ok=True)
with open(args.out, "w", encoding="utf-8") as fh:
    fh.write("month,subreddit,users_total\n")
    for m, s, n in rows:
        fh.write(f"{m},{s},{n}\n")

print(f"\nDone.  Files: {len(all_files)}  |  CSV rows: {len(rows)}")
