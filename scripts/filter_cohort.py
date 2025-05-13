#!/usr/bin/env python3
"""
Filter a monthly Pushshift .zst so that we keep only rows
whose author is in the given user list and output a slim JSONL
with author, subreddit, created_utc (optionally body).

Quick‑test mode:  --max-lines 200000   (or any small number)
Full run        :  --max-lines 0       (default)

Usage
-----
python scripts/filter_cohort.py --zst raw/RC_2025-03.zst --userlist filtered/NoFap_userlist.txt --out filtered/cohort_nofap_TEST.jsonl --max-lines 2000000

für cohort_marriedredpill.jsonl:
Gesamtzeilen   : 325,778,061
Passende Zeilen: 22,979,990
Laufzeit       : 4694.9s

"""

import argparse, json, zstandard as zstd, io, pathlib, time, sys

# ---------- CLI ----------
pr = argparse.ArgumentParser()
pr.add_argument("--zst", required=True, help=".zst dump file")
pr.add_argument("--userlist", required=True,
                help="text file: one author per line")
pr.add_argument("--out", required=True, help="output .jsonl")
pr.add_argument("--keep-body", action="store_true",
                help="retain comment body text")
pr.add_argument("--max-lines", type=int, default=0,
                help="stop after N lines (0 = full file)")
args = pr.parse_args()

ZST_FILE   = pathlib.Path(args.zst)
OUT_JSON   = pathlib.Path(args.out)
USERS      = set(pathlib.Path(args.userlist).read_text().split())
MAX_LINES  = args.max_lines if args.max_lines > 0 else None

OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

dctx = zstd.ZstdDecompressor()
line_ct = match_ct = 0
t0 = time.time()

with ZST_FILE.open('rb') as fh, dctx.stream_reader(fh) as reader, \
     io.TextIOWrapper(reader, encoding='utf-8', errors='ignore') as ts, \
     OUT_JSON.open('w', encoding='utf-8') as fout:

    for raw in ts:
        line_ct += 1
        if MAX_LINES and line_ct >= MAX_LINES:
            print(f"Reached test limit --max-lines={MAX_LINES}", file=sys.stderr)
            break
        if line_ct % 100_000 == 0:
            print(f"{line_ct:,} scanned  ({match_ct:,} matches)  "
                  f"[{time.time()-t0:.1f}s]", file=sys.stderr)

        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if rec["author"] in USERS and rec["author"] not in ("AutoModerator","[deleted]"):
            match_ct += 1
            out = {
                "author"      : rec["author"],
                "subreddit"   : rec["subreddit"],
                "created_utc" : rec["created_utc"],
            }
            if args.keep_body and "body" in rec:
                out["body"] = rec["body"]
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

print("\nDone")
print(f"Lines scanned   : {line_ct:,}")
print(f"Lines matched   : {match_ct:,}")
print(f"Elapsed seconds : {time.time()-t0:.1f}")
print(f"Saved to        : {OUT_JSON}")
