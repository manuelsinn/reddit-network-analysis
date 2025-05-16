#!/usr/bin/env python3
"""
build_userlists.py  –  Hilfsskript, um
1) aus einer großen *cohort_YYYY_Q?.jsonl*‑Datei alle `author`‑Namen zu ziehen und als
   TXT zu speichern (ein Name pro Zeile)
2) beliebige vorhandene User‑Listen (TXT) plus die neu erzeugte Liste zu
   einer einzigen Datei zu vereinen (Duplikate werden entfernt).

Beispielaufruf
--------------
python build_userlists.py \
    --cohort-jsonl   data/cohort_2025_Q1.jsonl \
    --this-txt-out   userlist_2025_Q1.txt \
    --existing-txts  userlists/month_*.txt \
    --merged-out     all_seed_users.txt

python build_userlists.py --cohort-jsonl filtered/cohorts/cohort_2025_Q1.jsonl --this-txt-out filtered/userlists/userlist_2025_Q1.txt --existing-txts filtered/userlists/userlist_*.txt --merged-out filtered/userlists/all_seed_users.txt


"""
import argparse, glob, json, os

# ------------------- CLI --------------------------------------------------
cli = argparse.ArgumentParser("User‑List Erzeuger & Merger")
cli.add_argument("--cohort-jsonl", required=True, help="Pfad zur cohort_*.jsonl")
cli.add_argument("--this-txt-out", required=True, help="TXT‑Datei für die extrahierten User")
cli.add_argument("--existing-txts", nargs="+", default=[], help="andere TXT‑Files oder Globs")
cli.add_argument("--merged-out", required=True, help="Ausgabe‑Datei für vereinigte User‑Liste")
args = cli.parse_args()

# ------------------- 1) User aus JSONL ziehen -----------------------------
users_current = set()
with open(args.cohort_jsonl, encoding="utf-8") as fh:
    for line in fh:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "author" in obj:
            users_current.add(obj["author"])
print(f"User aus {os.path.basename(args.cohort_jsonl)}: {len(users_current):,}")

with open(args.this_txt_out, "w", encoding="utf-8") as out:
    for u in sorted(users_current):
        out.write(u + "\n")
print("→ geschrieben:", args.this_txt_out)

# ------------------- 2) Merge mit bestehenden Listen ----------------------
all_users = set(users_current)
for pattern in args.existing_txts:
    for path in glob.glob(pattern):
        with open(path, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if ln:
                    all_users.add(ln)
print(f"Gesamt‑User (vereint): {len(all_users):,}")

with open(args.merged_out, "w", encoding="utf-8") as out:
    for u in sorted(all_users):
        out.write(u + "\n")
print("→ geschrieben:", args.merged_out)
