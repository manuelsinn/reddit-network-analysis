#!/usr/bin/env python
# compute_average_users.py
# ---------------------------------------------------------------
#  python scripts/compute_average_users.py --inputs outputs/stats/sub_user_counts*.csv --out outputs/stats/sub_user_counts_average.csv
# ---------------------------------------------------------------
import argparse, glob, pandas as pd
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--inputs",  nargs="+", required=True,
                help="CSV-Dateien wie 'month,subreddit,users_total'")
ap.add_argument("--out",     required=True,
                help="Ziel-CSV für Mittelwerte pro Subreddit")
args = ap.parse_args()

# alle Eingabe-CSVs zu einem DataFrame stapeln
df = pd.concat([pd.read_csv(f) for pattern in args.inputs 
                                for f in glob.glob(pattern)],
               ignore_index=True)

avg = (df.groupby("subreddit", as_index=False)["users_total"]
         .mean()
         .round()
         .rename(columns={"users_total": "users_avg"}))

Path(Path(args.out).parent).mkdir(parents=True, exist_ok=True)
avg.to_csv(args.out, index=False)
print(f"✔  {len(avg):,} Subreddits → {args.out}")
