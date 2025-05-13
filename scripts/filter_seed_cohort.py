#!/usr/bin/env python3
"""
filter_seed_cohort.py
─────────────────────
Streaming‑Script, das **Seed‑Subreddits** und **Monats‑Dumps** entgegennimmt und
in *einem* Durchlauf

1. alle Kommentare der Seed‑Subs aufnimmt,
2. daraus eine Autor*innen‑Liste bildet und
3. alle Kommentare dieser Autor*innen (egal in welchem Sub) in eine
   Cohort‑Datei schreibt.

Aufrufbeispiel
--------------

python scripts/filter_seed_cohort.py --months raw/RC_2024-07.zst raw/RC_2024-08.zst --seeds MensRights,marriedredpill,PickUpArtist,TheRedPill --cohort filtered/cohort_Q3-07-08.jsonl --userlist filtered/userlists/userlist_Q3-07-08.txt

Optionen
~~~~~~~~
--bad-users    Liste von Usernamen, die ignoriert werden (Default
               AutoModerator,[deleted])
--log-step     Fortschritts‑Ausgabe alle N Zeilen (Default 1 Mio)
"""

import argparse, json, time, io, sys, pathlib, zstandard as zstd
from typing import Set

# ---------------------- CLI ------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument('--months', nargs='+', required=True,
                    help='Pfad(e) zu RC_YYYY-MM.zst Dump‑Dateien')
parser.add_argument('--seeds', required=True,
                    help='Komma‑getrennte Liste von Seed‑Subreddits')
parser.add_argument('--cohort', required=True,
                    help='Ziel‑JSONL für Cohort‑Kommentare')
parser.add_argument('--userlist', required=True,
                    help='Ziel‑Datei für Autor*innen‑Liste')
parser.add_argument('--bad-users', default='AutoModerator,[deleted]',
                    help='Komma‑Liste von Usernamen, die ignoriert werden')
parser.add_argument('--log-step', type=int, default=1_000_000,
                    help='Fortschrittsprint alle N Zeilen')
args = parser.parse_args()

SEEDS: Set[str] = {s.strip() for s in args.seeds.split(',') if s.strip()}
BAD:   Set[str] = {u.strip() for u in args.bad_users.split(',') if u.strip()}
STEP   = args.log_step

cohort_path   = pathlib.Path(args.cohort)
userlist_path = pathlib.Path(args.userlist)
cohort_path.parent.mkdir(parents=True, exist_ok=True)
userlist_path.parent.mkdir(parents=True, exist_ok=True)

authors: Set[str] = set()

# -------------------- Helper: streaming reader -----------------------------

def stream_zst(path: str):
    d = zstd.ZstdDecompressor()
    with open(path, 'rb') as fh, d.stream_reader(fh) as r:
        yield from io.TextIOWrapper(r, encoding='utf-8', errors='ignore')

# -------------------- Pass: Seeds + Cohort ---------------------------------

start = time.time()
line_ct = 0

with cohort_path.open('w', encoding='utf-8') as cout:
    for zst_file in args.months:
        print(f">> Scanning {zst_file}")
        for ln in stream_zst(zst_file):
            line_ct += 1
            if line_ct % STEP == 0:
                print(f"{line_ct:,} lines  |  authors: {len(authors):,}  "
                      f"[{time.time()-start:.1f}s]")
            try:
                rec = json.loads(ln)
            except json.JSONDecodeError:
                continue
            user = rec.get('author')
            if user in BAD:
                continue
            sub  = rec.get('subreddit')
            ts   = rec.get('created_utc')
            if sub in SEEDS:
                authors.add(user)
                cout.write(json.dumps({'author': user,
                                       'subreddit': sub,
                                       'created_utc': ts}) + '\n')
            elif user in authors:
                cout.write(json.dumps({'author': user,
                                       'subreddit': sub,
                                       'created_utc': ts}) + '\n')

print(f"Finished scan – total lines: {line_ct:,} | authors: {len(authors):,}")

# -------------------- Save userlist ----------------------------------------

userlist_path.write_text('\n'.join(sorted(authors)))
print(f"Cohort file   : {cohort_path}")
print(f"Userlist file : {userlist_path}")
print(f"Elapsed time  : {time.time()-start:.1f}s")
