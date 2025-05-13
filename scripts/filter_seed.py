#!/usr/bin/env python3
"""
Filter a monthly Pushshift .zst dump:
1.  collect all authors who posted in the SEED_SUB
2.  write those authors to --userlist (filtered/mrp_userlist.txt)
3.  (optional) stop early after --max-lines for a quick test run

Usage
-----
python filter_seed.py --zst raw/RC_2025-03.zst
                      --seed marriedredpill
                      --out filtered/marriedredpill_seed.jsonl
                      --max-lines 200000

python scripts/filter_seed.py --zst raw/RC_2025-03.zst --seed marriedredpill --out filtered/marriedredpill_seed.jsonl --userlist filtered/mariedredpill_userlist.txt --max-lines 200000

Remove --max-lines (or set it to 0) for the full pass
"""
import json, zstandard as zstd, io, pathlib, argparse, time, sys

# ---------- CLI ----------
cli = argparse.ArgumentParser()
cli.add_argument("--zst",   required=True, help="RC_YYYY-MM.zst file")
cli.add_argument("--seed",  required=True, help="seed subreddit name")
cli.add_argument("--out",   required=True, help="output seed‑jsonl path")
cli.add_argument("--userlist", default="filtered/userlist.txt",
                 help="where to write the author list")
cli.add_argument("--max-lines", type=int, default=200_000,
                 help="stop after N lines (0 = full file)")
args = cli.parse_args()

ZST_FILE   = pathlib.Path(args.zst)
OUT_JSON   = pathlib.Path(args.out)
USER_TXT   = pathlib.Path(args.userlist)
SEED_SUB   = args.seed
MAX_LINES  = args.max_lines if args.max_lines > 0 else None

# ---------- prepare ----------
OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
USER_TXT.parent.mkdir(parents=True, exist_ok=True)

matcher_cnt = 0
line_cnt    = 0
authors     = set()
t0 = time.time()

dctx = zstd.ZstdDecompressor()  # single‑thread decompress
with ZST_FILE.open('rb') as fh, dctx.stream_reader(fh) as reader, \
     io.TextIOWrapper(reader, encoding='utf-8', errors='ignore') as ts, \
     OUT_JSON.open('w', encoding='utf-8') as jout:

    for raw in ts:
        line_cnt += 1
        if line_cnt % 100_000 == 0:
            print(f"{line_cnt:,} scanned "
                  f"({matcher_cnt:,} matches) "
                  f"[{time.time()-t0:.1f}s]", file=sys.stderr)

        if MAX_LINES and line_cnt >= MAX_LINES:
            print("Reached test limit --max-lines =", MAX_LINES, file=sys.stderr)
            break

        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if rec.get("subreddit") == SEED_SUB:
            if rec["author"] in ("AutoModerator", "[deleted]"):
                continue
            matcher_cnt += 1
            authors.add(rec["author"])
            jout.write(raw)                    # save full original line


print(f"\nFinished. Lines scanned : {line_cnt:,}")
print(f"Seed matches           : {matcher_cnt:,}")
print(f"Unique authors         : {len(authors):,}")

# ---------- write user list ----------
USER_TXT.write_text("\n".join(sorted(authors)))
print(f"Author list saved to   : {USER_TXT}")
print(f"Seed JSONL saved to    : {OUT_JSON}")
print(f"Elapsed time           : {time.time()-t0:.1f}s")
