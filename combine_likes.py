#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Combineer de nieuwste maand-JSON met de bestaande likes-historie.

• Zoekt automatisch het laatst gedateerde JSON-bestand in monthly_data/
• Leest de laatste linkedin_likes_history_<n>.csv uit history/
• Combineert per persoon (linkedin_url) de unieke liked_posts
• Schrijft een nieuw history-bestand weg als linkedin_likes_history_<n+1>.csv
"""

import os
import re
import json
import ast
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import pandas as pd

# ───────── Config ─────────
ROOT_DIR   = Path(__file__).parent
MONTHLY_DIR = ROOT_DIR / "monthly_data"
HISTORY_DIR = ROOT_DIR / "history"

# Regex­patronen voor bestandsnamen
MONTH_RE = re.compile(r"LinkedIn_interactions_(\d{2})[-_](\d{2})[-_](\d{4})\.json", re.I)
HIST_RE  = re.compile(r"linkedin_likes_history_(\d+)\.csv", re.I)

# ───────── Hulpfuncties ─────────
def str_to_list(x):
    """Zet CSV-string terug naar list, of laat list ongemoeid."""
    if isinstance(x, list):
        return x
    if isinstance(x, float) and pd.isna(x):
        return []
    try:
        return ast.literal_eval(x)
    except Exception:
        return []

def find_latest_month_file() -> Path | None:
    """Pak het maand-JSON met de nieuwste datum in de bestandsnaam."""
    latest_fp, latest_dt = None, None
    for fp in MONTHLY_DIR.glob("LinkedIn_interactions_*.json"):
        m = MONTH_RE.match(fp.name)
        if not m:
            continue
        dd, mm, yyyy = map(int, m.groups())
        dt = datetime(yyyy, mm, dd)
        if latest_dt is None or dt > latest_dt:
            latest_dt, latest_fp = dt, fp
    return latest_fp

def find_latest_history_file() -> tuple[Path | None, int]:
    """Vind het hoogste genummerde history-CSV (retour (pad, n))."""
    latest_fp, latest_n = None, 0
    for fp in HISTORY_DIR.glob("linkedin_likes_history_*.csv"):
        m = HIST_RE.match(fp.name)
        if not m:
            continue
        n = int(m.group(1))
        if n > latest_n:
            latest_n, latest_fp = n, fp
    return latest_fp, latest_n

def process_json_to_df(json_path: Path) -> pd.DataFrame:
    """Zet de ruwe LinkedIn JSON om naar een DataFrame met likes per persoon."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    people = defaultdict(lambda: {
        "first_name": "",
        "last_name": "",
        "linkedin_url": "",
        "liked_posts": set()
    })

    for entry in data:
        post_url = entry.get("socialContent", {}).get("shareUrl", "")
        for reaction in entry.get("reactionElements", []):
            mini = reaction.get("image", {}).get("attributes", [{}])[0].get("miniProfile", {})
            first = mini.get("firstName", "")
            last  = mini.get("lastName", "")
            pid   = mini.get("publicIdentifier", "")
            url   = f"https://www.linkedin.com/in/{pid}" if pid else ""

            p = people[url]
            p["first_name"] = first
            p["last_name"]  = last
            p["linkedin_url"] = url
            p["liked_posts"].add(post_url)

    rows = [{
        "first_name": p["first_name"],
        "last_name":  p["last_name"],
        "linkedin_url": p["linkedin_url"],
        "liked_posts": list(p["liked_posts"]),
        "total_likes": len(p["liked_posts"])
    } for p in people.values()]

    return pd.DataFrame(rows)

# ───────── Start verwerking ─────────
latest_json = find_latest_month_file()
if latest_json is None:
    raise FileNotFoundError("Geen maand-JSON gevonden in ‘monthly_data/’")

print(f"📥 Nieuwste JSON: {latest_json.name}")
df_new = process_json_to_df(latest_json)

# Historie inladen
hist_fp, hist_n = find_latest_history_file()
if hist_fp:
    df_hist = pd.read_csv(hist_fp)
    df_hist["liked_posts"] = df_hist["liked_posts"].apply(str_to_list)
    print(f"🔄 Historie geladen: {hist_fp.name}")
else:
    df_hist = pd.DataFrame(columns=["first_name", "last_name",
                                    "linkedin_url", "liked_posts", "total_likes"])
    print("🆕 Geen bestaand history-bestand gevonden – nieuwe historie wordt gestart.")

# ───────── Combineer data ─────────
combined = defaultdict(lambda: {
    "first_name": "",
    "last_name": "",
    "linkedin_url": "",
    "liked_posts": set()
})

def add_rows(df: pd.DataFrame):
    """Voeg personen + likes toe aan combined zonder data te verliezen."""
    for _, row in df.iterrows():
        key = row["linkedin_url"]

        # Nieuw record indien nodig
        if key not in combined:
            combined[key] = {
                "first_name": row.get("first_name", ""),
                "last_name":  row.get("last_name", ""),
                "linkedin_url": key,
                "liked_posts": set(row.get("liked_posts", []))
            }
        else:
            # Houd bestaande likes en voeg nieuwe toe (set voorkomt dubbels)
            combined[key]["liked_posts"].update(row.get("liked_posts", []))

            # Werk namen bij als nieuwe niet leeg zijn
            if row.get("first_name"):
                combined[key]["first_name"] = row["first_name"]
            if row.get("last_name"):
                combined[key]["last_name"]  = row["last_name"]

# Eerst historie, dan nieuwe maand
add_rows(df_hist)
add_rows(df_new)

# ───────── Maak eind-DataFrame ─────────
rows = []
for p in combined.values():
    urls = list(p["liked_posts"])
    rows.append({
        "first_name" : p["first_name"],
        "last_name"  : p["last_name"],
        "linkedin_url": p["linkedin_url"],
        "liked_posts": urls,
        "total_likes": len(urls)
    })

df_final = (pd.DataFrame(rows)
            .sort_values("total_likes", ascending=False))

# liked_posts naar string voor CSV-opslag
df_final["liked_posts"] = df_final["liked_posts"].apply(lambda x: str(x))

# ───────── Opslaan ─────────
HISTORY_DIR.mkdir(exist_ok=True)
new_n    = hist_n + 1
out_file = HISTORY_DIR / f"linkedin_likes_history_{new_n}.csv"
df_final.to_csv(out_file, index=False)

print(f"✅ Gecombineerde dataset opgeslagen → {out_file.name}")

