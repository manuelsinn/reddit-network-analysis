# Reddit Network Analysis

This repository contains standalone Python scripts for filtering Pushshift Reddit
comment dumps, building subreddit graphs, and calculating cohort and graph
statistics. The graph files can be opened in Gephi; most scripts also write
CSV/JSONL files for further analysis.

The scripts expect to be run from the repository root. They take file paths and
options on the command line. Run a script with `--help` to see its arguments.

## Data and directory layout

The raw Pushshift data is **not included in this repository**. Obtain the
required monthly archives from the Pushshift archive/torrent distribution and
put them under `raw/`. Comment archives should be (natively) named like `RC_YYYY-MM.zst` and need not be unzipped. Some scripts can also use submission archives (includes posts) named `RS_YYYY-MM.zst`. Scripts generate intermediate data saved to `filtered/` on the local machine. 

Typical local layout:

```text
raw/
  RC_2025-01.zst
  RC_2025-02.zst
  submissions/
    RS_2025-01.zst
filtered/
  userlists/
  cohorts/
outputs/
  overlap/
  drifts/
  deciles/
  tb/
scripts/
```

`outputs/` contains generated results and examples; output files will be replaced when you rerun a script with the same destination.

## Requirements

Use Python 3 and install the third-party packages used by the scripts:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install networkx numpy orjson pandas tqdm zstandard
```

Run the commands below with the environment activated. The scripts can read
very large compressed dumps and some load complete cohorts into memory, so plan
for substantial disk space, memory, and processing time. For a quick test,
`filter_seed.py` and `filter_cohort.py` support `--max-lines`; use `0` for a
full-file run in either script.

## Typical workflow







### 1. Make a seed-user list and a filtered cohort

**Seed subreddit:** a subreddit chosen as the starting point for the analysis.
The seed-user set consists of authors who posted in one or more seed subreddits during the selected period.

**User list:** a text file containing Reddit author names, one per line. It
identifies the users to keep when filtering comment dumps (i.e., all users who commented in seed subreddits).

**Cohort:** the comments by the selected seed users across the chosen months
and subreddits. Cohort files are JSON, with one JSON object per line that has `author`, `subreddit`, and `created_utc` fields. 


For example, first scan a comment dump for seed-subreddit posts:

```bash
python scripts/filter_seed.py \
  --zst raw/RC_2025-01.zst \
  --seed TheRedPill \
  --out filtered/seeds/the_red_pill_2025-01.jsonl \
  --userlist filtered/userlists/the_red_pill_2025-01.txt \
  --max-lines 0
```

Repeat this for the relevant months and seed subreddits. Combine the resulting
seed-user lists into one text file (one author per line), removing duplicates.
Then use that list to filter each monthly comment dump into cohort JSONL:

```bash
python scripts/filter_cohort.py \
  --zst raw/RC_2025-01.zst \
  --userlist filtered/userlists/all_seed_users.txt \
  --out filtered/cohorts/all_seed_users/cohort_2025-01.jsonl \
  --max-lines 0
```

Repeat `filter_cohort.py` for each month that should be included. Add
`--keep-body` only if downstream work needs the comment body; by default the
output is limited to author, subreddit, and timestamp.

For a combined seed-and-cohort scan, `filter_seed_cohort.py` accepts multiple
monthly dumps and comma-separated seeds:

```bash
python scripts/filter_seed_cohort.py \
  --months raw/RC_2025-01.zst raw/RC_2025-02.zst raw/RC_2025-03.zst \
  --seeds TheRedPill,MensRights,marriedredpill,PickUpArtist \
  --cohort filtered/cohorts/cohort_Q1.jsonl \
  --userlist filtered/userlists/userlist_Q1.txt
```

This script writes cohort rows as it discovers seed users while scanning. If
the cohort must include a user's activity from before their first seed post,
use a two-pass process: build the complete seed-user list first, then filter
the monthly dumps with `filter_cohort.py`.

`build_userlists.py` can extract authors from a cohort and merge them with
existing text lists. Its `--existing-txts` option accepts file paths or glob
patterns.


### 2. Build subreddit graphs

Scripts can build two types of network graphs: overlap and transition graphs. An **overlap graph** is undirected: each edge connects two subreddits with
shared cohort users. `weight` is the shared-user count, and `jaccard` is the
Jaccard similarity. The filters below keep edges with at least 30 shared users
and Jaccard similarity of at least 0.05:

```bash
python scripts/build_overlap_graph.py \
  --cohort filtered/cohorts/all_seed_users/cohort_2025-01.jsonl \
  --out outputs/overlap/overlap_2025-01.gexf \
  --weight-min 30 \
  --jaccard-min 0.05
```

The output extension selects GEXF or GraphML; GEXF is convenient for Gephi.
Build from a combined cohort file to include more than one month.

A **transition graph** is directed and is built from paths generated by
`compute_user_drift_paths.py`. The drift script finds each user's first post in
one of the seed subreddits (time `T0`) and summarizes their earlier subreddit
activity:

```bash
python scripts/compute_user_drift_paths.py \
  --inputs 'filtered/cohorts/all_seed_users/cohort_*.jsonl' \
  --seed-subs TheRedPill,MensRights,marriedredpill,PickUpArtist \
  --output-dir outputs/drifts \
  --min-comments 3

python scripts/build_transition_graph.py \
  --paths-csv outputs/drifts/user_paths.csv \
  --output outputs/drifts/transition_graph.gexf \
  --format gexf \
  --weight users
```

The transition graph's edge `weight` can represent unique users (`users`) or
all transitions (`transitions`). `build_windowed_graph.py` is another graph
builder: it reads a cohort JSONL and writes a directed, weighted GraphML
subreddit-transition graph.

### 3. Calculate statistics

Useful analysis scripts include:

| Script | Purpose |
| --- | --- |
| `compute_cohort_stats.py` | Comment and active-user counts by subreddit, cohort, and month |
| `compute_users_per_sub.py` | Unique authors per subreddit in raw `.zst` comment dumps, by month |
| `compute_average_users.py` | Average monthly subreddit user counts from CSV files |
| `compute_rad_share_per_user.py` | Per-user share of comments posted in the selected seed subreddits |
| `compute_sub_decile_stats.py` | Per-subreddit user/comment counts by user seed-comment-share decile |
| `compute_sub_quartile_stats.py` | The same type of per-subreddit counts grouped into four share quartiles |
| `add_deciles_to_graph.py` | Add user-share quartile attributes and lift values to an existing graph |
| `compute_target_betweenness.py` | Target betweenness and neighbor/subgraph exports for a graph |

Each script's `--help` output lists its required arguments. For example, to
calculate user seed-share values and then per-subreddit quartile matrices:

```bash
python scripts/compute_rad_share_per_user.py \
  --inputs 'filtered/cohorts/all_seed_users/cohort_*.jsonl' \
  --seed-subs TheRedPill,MensRights,marriedredpill,PickUpArtist \
  --seed-users filtered/userlists/all_seed_users.txt \
  --min-comments 10 \
  --out outputs/deciles/user_radshare.csv

python scripts/compute_sub_quartile_stats.py \
  --inputs 'filtered/cohorts/all_seed_users/cohort_*.jsonl' \
  --radshare outputs/deciles/user_radshare.csv \
  --out-users outputs/deciles/users_quartile_matrix.csv \
  --out-comments outputs/deciles/comments_quartile_matrix.csv
```

To calculate target betweenness, supply a graph, target subreddit names, and a
user-count CSV with `subreddit` and `users_avg` columns:

```bash
python scripts/compute_target_betweenness.py \
  --graph outputs/drifts/transition_graph.gexf \
  --targets TheRedPill,MensRights,marriedredpill,PickUpArtist \
  --user-counts outputs/sub_user_count/sub_user_counts_average.csv \
  --out outputs/tb/tb_all.csv \
  --neighbors-out outputs/tb/tb_neighbors.csv \
  --subgraph-out outputs/tb/tb_subgraph.gexf
```

## Open and inspect graphs

Open `.gexf` or `.graphml` outputs in [Gephi](https://gephi.org/) for layout,
filtering, and visualization. The repository also contains saved Gephi project
files and example graph images; they are not required to run the scripts.

## Script reference

All analysis scripts are in [`scripts/`](scripts/). Use the script's
`--help` for the authoritative list of command-line options and defaults.
