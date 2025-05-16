#!/usr/bin/env python3
"""
user_drift_paths.py  —  Rekonstruiert den Weg *bis* zum ersten Post in einem
gegebenen „Seed‑Subreddit“ (Radikal-Seed).  Es werden alle Seed‑Poster im
Datensatz betrachtet, egal wann ihr T0 fällt.

Für jeden Nutzer:
────────────────
• **T0**   = erste Zeile in einem der Seed‑Subs  → rel_days = 0
• **Pre‑T0**  = erstes Posting in *jedem* anderen Sub vor T0
  (Zeitspanne reicht vom Anfang der eingelesenen Daten bis T0).

Ergebnisdatei *user_paths.csv*
──────────────────────────────
 author, subreddit, first_ts_here, comment_count, rel_days

Option *aggregate_bins.csv* (Zeit‑Bins vs. Subreddits) mit --bins.

Aufrufbeispiel
──────────────
python user_drift_paths.py \
    --inputs  data/2024-*.jsonl data/2025-*.jsonl \
    --seed-subs TheRedPill,MensRights,PickUpArtist \
    --output-dir outputs/drifts \
    --bins
"""
import argparse, os, glob, json
from pathlib import Path
import pandas as pd

# -------------------------- CLI -------------------------------------------
cli = argparse.ArgumentParser("User-Drift Pfad-Extraktor")
cli.add_argument("--inputs", "-i", nargs="+", required=True,
                help="JSONL-Dateien oder Globs für den 12‑Monats‑Zeitraum")
cli.add_argument("--output-dir", "-o", required=True,
                help="Verzeichnis für Ergebnis‑CSVs")
cli.add_argument("--seed-subs", "-s", required=True,
                help="Komma-Liste oder TXT mit 1 Seed-Sub pro Zeile")
cli.add_argument("--bins", action="store_true",
                help="Erzeuge zusätzlich aggregate_bins.csv")
args = cli.parse_args()

# -------------------------- Seed-Liste ------------------------------------
if os.path.isfile(args.seed_subs):
    SEED_SUBS = {ln.strip() for ln in open(args.seed_subs, encoding="utf-8") if ln.strip()}
else:
    SEED_SUBS = {x.strip() for x in args.seed_subs.split(",") if x.strip()}
if not SEED_SUBS:
    raise SystemExit("Seed-Subreddit-Liste ist leer!")
print("Seed-Subs:", ", ".join(sorted(SEED_SUBS)))

# -------------------------- Dateien sammeln -------------------------------
files = []
for patt in args.inputs:
    files.extend(glob.glob(patt))
if not files:
    raise SystemExit("Keine Eingabedateien gefunden.")
print(f"Dateien: {len(files)}")

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
            if not all(k in obj for k in ("author","subreddit","created_utc")):
                continue
            records.append((obj["author"], obj["subreddit"], int(obj["created_utc"])))
print(f"Kommentare geladen: {len(records):,}")

# DataFrame
cols = ["author","subreddit","created_utc"]
df = pd.DataFrame.from_records(records, columns=cols)
df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)

# -------------------------- T0 je User bestimmen --------------------------
seed_df = (df[df["subreddit"].isin(SEED_SUBS)]
             .sort_values(["author","created_utc"])
             .drop_duplicates("author", keep="first")
             .rename(columns={"created_utc":"first_seed_ts",
                              "subreddit":"seed_sub"}))
print(f"Seed‑Poster gesamt: {len(seed_df):,}")
if seed_df.empty:
    raise SystemExit("Kein Seed‑Poster gefunden – Seed-Liste oder Daten prüfen.")

# -------------------------- Pre‑T0‑Kommentare -----------------------------
pre_df = df.merge(seed_df[["author","first_seed_ts"]], on="author")
pre_df = pre_df[pre_df["created_utc"] < pre_df["first_seed_ts"]]

tmp = pre_df.groupby('author').size().sort_values(ascending=False)
print(tmp.head(10))
print('User mit ≥1 Pre‐Post:', (tmp>0).sum())


# Erstes Posting & Engagement in jedem Sub vor T0
agg = (pre_df.groupby(["author","subreddit"])
             .agg(first_ts_here=("created_utc","min"),
                  comment_count=("created_utc","size"))
             .reset_index())

# rel_days (wie viele Tage vor T0)
agg = agg.merge(seed_df[["author","first_seed_ts"]], on="author")
agg["rel_days"] = (agg["first_seed_ts"] - agg["first_ts_here"]).dt.days
agg.drop(columns="first_seed_ts", inplace=True)

# -------------------------- T0‑Zeile anhängen -----------------------------
t0_rows = seed_df.rename(columns={"first_seed_ts":"first_ts_here",
                                  "seed_sub":"subreddit"})
t0_rows["comment_count"] = 1
t0_rows["rel_days"] = 0
t0_rows = t0_rows[agg.columns]

# -------------------------- Speichern -------------------------------------
out = pd.concat([agg, t0_rows], ignore_index=True)
Path(args.output_dir).mkdir(parents=True, exist_ok=True)
path_out = os.path.join(args.output_dir, "user_paths.csv")
out.to_csv(path_out, index=False)
print("→", path_out, "| Zeilen:", len(out))

# -------------------------- optional Binning ------------------------------
if args.bins and not out.empty:
    bins = pd.cut(out["rel_days"],
                  bins=[-1,30,60,90,180,365,730,1e9],
                  labels=["<1M","1-2M","2-3M","3-6M","6-12M","12-24M",">24M"],
                  right=True)
    bin_df = (pd.concat([out[["author","subreddit"]], bins.rename("bin")], axis=1)
                .query("bin.notnull() and rel_days > 0")
                .groupby(["bin","subreddit"], observed=True)
                .agg(n_users=("author","nunique"))
                .reset_index())
    bin_df["pct_of_seed_posters"] = bin_df["n_users"] / len(seed_df)
    bin_path = os.path.join(args.output_dir, "aggregate_bins.csv")
    bin_df.to_csv(bin_path, index=False)
    print("→", bin_path, "| Zeilen:", len(bin_df))
