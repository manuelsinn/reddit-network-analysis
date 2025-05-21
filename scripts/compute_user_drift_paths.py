#!/usr/bin/env python3
"""
compute_user_drift_paths.py  —  Rekonstruiert Wege *bis* zum ersten Post in einem
Seed‑Subreddit.  Neu: **--min-comments** Flag, um einmalige „Vorbei-Poster“
herauszufiltern.

Pro Nutzer (author):
  • *Pre‑T0*: erstes Posting je Subreddit vor T0 **und** comment_count ≥ min-comments
  • *T0*: die Seed‑Zeile (rel_days = 0, comment_count = 1) wird immer behalten.

Nutzung
───────
python user_drift_paths.py \
    --inputs data/2024-*.jsonl data/2025-*.jsonl \
    --seed-subs TheRedPill,MensRights,PickUpArtist \
    --output-dir outputs/drifts \
    --min-comments 3 \
    --bins

python scripts/compute_user_drift_paths.py --inputs filtered/cohorts/all_seed_users/cohort_*.jsonl --seed-subs TheRedPill,MensRights,PickUpArtist --output-dir outputs/drifts_new --min-comments 3 --bins

"""
import argparse, os, glob, json
from pathlib import Path
import pandas as pd

# -------------------------- CLI -------------------------------------------
cli = argparse.ArgumentParser("User-Drift Pfad-Extraktor + Min-Comments")
cli.add_argument("--inputs", "-i", nargs="+", required=True,
                help="JSONL-Dateien oder Globs")
cli.add_argument("--output-dir", "-o", required=True,
                help="Verzeichnis für Ergebnis-CSVs")
cli.add_argument("--seed-subs", "-s", required=True,
                help="Komma-Liste oder TXT mit 1 Seed-Sub pro Zeile")
cli.add_argument("--min-comments", type=int, default=1,
                help="minimale Kommentarzahl je Subreddit (vor T0), um in den Pfad aufgenommen zu werden; Default 1 ⇒ keine Filterung")
cli.add_argument("--bins", action="store_true",
                help="erzeuge aggregate_bins.csv")
args = cli.parse_args()

# -------------------------- Seed-Liste ------------------------------------
if os.path.isfile(args.seed_subs):
    SEED_SUBS = {ln.strip() for ln in open(args.seed_subs, encoding="utf-8") if ln.strip()}
else:
    SEED_SUBS = {x.strip() for x in args.seed_subs.split(",") if x.strip()}
if not SEED_SUBS:
    raise SystemExit("Seed-Subreddit-Liste ist leer!")

# -------------------------- Dateien sammeln -------------------------------
files = []
for patt in args.inputs:
    files.extend(glob.glob(patt))
if not files:
    raise SystemExit("Keine Eingabedateien gefunden.")

# -------------------------- JSONL einlesen --------------------------------
records = []
for path in files:
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not all(k in obj for k in ("author", "subreddit", "created_utc")):
                continue
            records.append((obj["author"], obj["subreddit"], int(obj["created_utc"])))
print(f"Kommentare geladen: {len(records):,}")

# DataFrame
cols = ["author", "subreddit", "created_utc"]
df = pd.DataFrame.from_records(records, columns=cols)
df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)

# -------------------------- T0 je User bestimmen --------------------------
seed_df = (df[df["subreddit"].isin(SEED_SUBS)]
             .sort_values(["author", "created_utc"])
             .drop_duplicates("author", keep="first")
             .rename(columns={"created_utc": "first_seed_ts",
                              "subreddit": "seed_sub"}))
print(f"Seed-Poster gesamt: {len(seed_df):,}")
if seed_df.empty:
    raise SystemExit("Kein Seed-Poster gefunden.")

# -------------------------- Pre-T0-Kommentare -----------------------------
pre_df = df.merge(seed_df[["author", "first_seed_ts"]], on="author")
pre_df = pre_df[pre_df["created_utc"] < pre_df["first_seed_ts"]]

agg = (pre_df.groupby(["author", "subreddit"])
             .agg(first_ts_here=("created_utc", "min"),
                  comment_count=("created_utc", "size"))
             .reset_index())

# ---------- Min-Comments Filter (nur Pre‑T0) ------------------------------
if args.min_comments > 1:
    before = len(agg)
    agg = agg[agg["comment_count"] >= args.min_comments]
    print(f"Min-Comments Filter {args.min_comments}: {before} → {len(agg)} Zeilen")

# rel_days
agg = agg.merge(seed_df[["author", "first_seed_ts"]], on="author")
agg["rel_days"] = (agg["first_seed_ts"] - agg["first_ts_here"]).dt.days
agg.drop(columns="first_seed_ts", inplace=True)

# -------------------------- T0-Zeile --------------------------------------
t0_rows = seed_df.rename(columns={"first_seed_ts": "first_ts_here",
                                  "seed_sub": "subreddit"})
t0_rows["comment_count"] = 1
t0_rows["rel_days"] = 0

# Gleiche Spalten
t0_rows = t0_rows[agg.columns]

# -------------------------- Speichern -------------------------------------
out = pd.concat([agg, t0_rows], ignore_index=True)
Path(args.output_dir).mkdir(parents=True, exist_ok=True)
path_out = os.path.join(args.output_dir, "user_paths.csv")
out.to_csv(path_out, index=False)
print("→", path_out, "| Zeilen:", len(out))

# -------------------------- optional Binning ------------------------------
# if args.bins and not out.empty:
#         # Zeit-Bins bilden
#     bins = pd.cut(out["rel_days"],
#                   bins=[-1, 30, 60, 90, 180, 365, 730, 1e9],
#                   labels=["<1M", "1-2M", "2-3M", "3-6M", "6-12M", "12-24M", ">24M"],
#                   right=True)
#     # DataFrame mit rel_days beibehalten, damit Query funktioniert
#     tmp = pd.concat([out[["author", "subreddit", "rel_days"]], bins.rename("bin")], axis=1)
#     bin_df = (tmp.query("bin.notnull() and rel_days > 0")
#                  .groupby(["bin", "subreddit"], observed=True)
#                  .agg(n_users=("author", "nunique"))
#                  .reset_index())
