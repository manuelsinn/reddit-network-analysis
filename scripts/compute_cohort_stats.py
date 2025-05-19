#!/usr/bin/env python3
"""


ACHTUNG: Flaw im Programm! Berechnet scheinbar pro Subreddit die aktiven User und Kommentare, aber die Cohort Files beinhalten *nur* die User, die auch in einem Seed-Subreddit aktiv waren.


------------------------------------
Skript zur Aggregation von Statistik-Kennzahlen für JSONL-Kohorten.

Für jede eingegebene JSONL-Datei (z.B. Q3, Q4, Q1):
 - Kommentare pro Subreddit (gesamt und pro Monat)
 - Aktive Benutzer pro Subreddit (gesamt und pro Monat)

Nutzung:
    python scripts/compute_cohort_stats.py --inputs filtered/cohort_2024_Q3.jsonl filtered/cohort_2024_Q4.jsonl filtered/cohort_2025_Q1.jsonl --output-dir outputs/stats
    python scripts/compute_cohort_stats.py --inputs filtered/cohort_2024_Q3.jsonl --output-dir outputs/stats

Es werden folgende CSVs erzeugt im output-dir:
 - comments_per_subreddit.csv
 - active_users_per_subreddit.csv
 - comments_per_subreddit_monthly.csv
 - active_users_per_subreddit_monthly.csv
"""
import argparse
import os
import json
import pandas as pd

# ---------- CLI ------------------------------------------------------------
parser = argparse.ArgumentParser(description="Statistiken aus JSONL-Kohorten erzeugen")
parser.add_argument(
    '--inputs', '-i', nargs='+', required=True,
    help='Liste der JSONL-Dateien mit Kommentar-Daten'
)
parser.add_argument(
    '--output-dir', '-o', required=True,
    help='Verzeichnis zur Ablage der Ergebnis-CSV-Dateien'
)
args = parser.parse_args()

# ---------- Daten sammeln -------------------------------------------------
all_records = []
for path in args.inputs:
    cohort_name = os.path.splitext(os.path.basename(path))[0]
    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            # Prüfen erforderlicher Felder
            if 'created_utc' not in obj or 'subreddit' not in obj or 'author' not in obj:
                continue
            # Datum & Cohort anreichern
            dt = pd.to_datetime(obj['created_utc'], unit='s', utc=True)
            month_str = str(dt.to_period('M'))  # YYYY-MM
            all_records.append({
                'cohort': cohort_name,
                'month': month_str,
                'subreddit': obj['subreddit'],
                'author': obj['author']
            })

# ---------- DataFrame -----------------------------------------------------
if not all_records:
    raise ValueError("Keine gültigen Datensätze gefunden. Prüfe die Eingabedateien.")

df_all = pd.DataFrame(all_records)

# ---------- Aggregationen -------------------------------------------------
# 1) Kommentare pro Subreddit gesamt pro Cohort
comments_cohort = (
    df_all
    .groupby(['cohort', 'subreddit'])
    .size()
    .reset_index(name='comment_count')
)
# 2) Aktive Benutzer pro Subreddit gesamt pro Cohort
active_cohort = (
    df_all
    .groupby(['cohort', 'subreddit'])['author']
    .nunique()
    .reset_index(name='active_users')
)
# 3) Kommentare pro Subreddit pro Monat
comments_monthly = (
    df_all
    .groupby(['cohort', 'month', 'subreddit'])
    .size()
    .reset_index(name='comment_count')
)
# 4) Aktive Benutzer pro Subreddit pro Monat
active_monthly = (
    df_all
    .groupby(['cohort', 'month', 'subreddit'])['author']
    .nunique()
    .reset_index(name='active_users')
)

# ---------- CSV-Ausgabe ---------------------------------------------------
os.makedirs(args.output_dir, exist_ok=True)
comments_cohort.to_csv(os.path.join(args.output_dir, 'comments_per_subreddit.csv'), index=False)
active_cohort.to_csv(os.path.join(args.output_dir, 'active_users_per_subreddit.csv'), index=False)
comments_monthly.to_csv(os.path.join(args.output_dir, 'comments_per_subreddit_monthly.csv'), index=False)
active_monthly.to_csv(os.path.join(args.output_dir, 'active_users_per_subreddit_monthly.csv'), index=False)

print(f"CSV-Dateien geschrieben nach: {args.output_dir}")
