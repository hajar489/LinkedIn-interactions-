#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Combineer het laatste history-bestand met de nieuwste LinkedIn-maand-dump
en schrijf een nieuw, opgehoogd history-bestand terug.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import json, re, sys

# ──────────────────────────────────────────────────────────────────────
# 1.  BASIS-MAPPEN (PAS DIT EVENTUEEL AAN)
# ──────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).resolve().parent
HISTORY_DIR  = BASE_DIR / "history"
MONTHLY_DIR  = BASE_DIR / "monthly_data"


# ──────────────────────────────────────────────────────────────────────
# 2.  HULPFUNCTIES
# ──────────────────────────────────────────────────────────────────────
HIST_RX   = re.compile(r"linkedin_likes_history_(\d+)\.csv$", re.I)
MONTH_RX  = re.compile(r"LinkedIn_interactions_(\d{2})_(\d{2})_(\d{4})\.json$", re.I)

def latest_history(path: Path) -> tuple[Path | None, int]:
    """Return (pad, hoogste_n) of (None, 0) als er nog geen history is."""
    candidates = []
    for f in path.glob("linkedin_likes_history_*.csv"):
        m = HIST_RX.match(f.name)
        if m:
            candidates.append( (int(m.group(1)), f) )
    if not candidates:
        return None, 0
    n, p = max(candidates)          # hoogste nummer
    return p, n

def latest_monthly(path: Path) -> Path:
    """Return pad van maand-dump met laatste datum. Raise als niets gevonden."""
    best: tuple[datetime, Path] | None = None
    for f in path.glob("LinkedIn_interactions_*.json"):
        m = MONTH_RX.match(f.name)
        if not m:         # naam past niet
            continue
        day, month, year = map(int, m.groups())
        d = datetime(year, month, day)
        if best is None or d > best[0]:
            best = (d, f)
    if best is None:
        sys.exit("❌  Geen bestand LinkedIn_interactions_*.json gevonden!")
    return best[1]

def split_posts(cell) -> set[str]:
    """Kolom liked_posts (string) → set van URL's."""
    if pd.isna(cell):
        return set()
    return set(filter(None, re.split(r'[\s,]+', str(cell).strip())))

# ──────────────────────────────────────────────────────────────────────
# 3.  LAATSTE HISTORY + MEEST RECENTE MAAND-DUMP OPHALEN
# ──────────────────────────────────────────────────────────────────────
hist_path, hist_nr = latest_history(HISTORY_DIR)
if hist_path is None:
    print("⚠️  Nog geen history-bestand gevonden; er wordt een nieuw gestart.")
    hist_df = pd.DataFrame(
        columns=["first_name","last_name","linkedin_url","liked_posts","total_likes"]
    )
else:
    hist_df = pd.read_csv(hist_path)

monthly_path = latest_monthly(MONTHLY_DIR)

print(f"➜  Gebruik history : {hist_path or '(nieuw)'}")
print(f"➜  Gebruik monthly : {monthly_path}")

# ──────────────────────────────────────────────────────────────────────
# 4.  HISTORY INLEZEN EN OMZETTEN NAAR DICT
# ──────────────────────────────────────────────────────────────────────
hist_df["liked_posts_set"] = hist_df["liked_posts"].apply(split_posts)
people = {
    row.linkedin_url: {
        "first_name":  row.first_name,
        "last_name":   row.last_name,
        "liked_posts": row.liked_posts_set
    }
    for _, row in hist_df.iterrows()
}

# ──────────────────────────────────────────────────────────────────────
# 5.  MAAND-JSON VERWERKEN
# ──────────────────────────────────────────────────────────────────────
with open(monthly_path, encoding="utf-8") as fh:
    monthly = json.load(fh)

for post in monthly:
    post_url = post["socialContent"]["shareUrl"]
    for reaction in post["reactionElements"]:
        mini = reaction["image"]["attributes"][0]["miniProfile"]
        profile_url = f"https://www.linkedin.com/in/{mini['publicIdentifier']}"
        person = people.setdefault(
            profile_url,
            {"first_name": mini["firstName"],
             "last_name":  mini["lastName"],
             "liked_posts": set()}
        )
        person["liked_posts"].add(post_url)

# ──────────────────────────────────────────────────────────────────────
# 6.  NIEUW DATAFRAME BOUWEN
# ──────────────────────────────────────────────────────────────────────
rows = []
for url, info in people.items():
    posts_sorted = sorted(info["liked_posts"])
    rows.append({
        "first_name":  info["first_name"],
        "last_name":   info["last_name"],
        "linkedin_url": url,
        "liked_posts": "\n".join(posts_sorted),
        "total_likes": len(posts_sorted)
    })

new_hist_df = (
    pd.DataFrame(rows)
      .sort_values(["first_name","last_name"], ignore_index=True)
)

# ──────────────────────────────────────────────────────────────────────
# 7.  OPSLAAN ALS VOLGENDE HISTORY-BESTAND
# ──────────────────────────────────────────────────────────────────────
next_nr   = hist_nr + 1            # eerste keer wordt dat 1
out_path  = HISTORY_DIR / f"linkedin_likes_history_{next_nr}.csv"
new_hist_df.to_csv(out_path, index=False, encoding="utf-8")

print(f"✅  Nieuwe historie opgeslagen als: {out_path}")

