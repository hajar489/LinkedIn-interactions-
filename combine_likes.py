#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import re
import json
import ast
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import pandas as pd

# ───────── Config ─────────
ROOT_DIR      = Path(__file__).parent
MONTHLY_DIR   = ROOT_DIR / "monthly_data"
HISTORY_DIR   = ROOT_DIR / "history"

# Regex voor bestandsnamen
MONTH_RE   = re.compile(r"LinkedIn_interactions_(\d{2})[-_](\d{2})[-_](\d{4})\.json", re.I)
HIST_RE    = re.compile(r"linkedin_likes_history_(\d+)\.csv", re.I)

# ───────── Hulpfuncties ─────────
def str_to_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, float) and pd.isna(x):
        return []
    try:
        return ast.literal_eval(x)
    except Exception:
        return []

def find_latest_month_file():
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

def find_latest_history_file():
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
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    personen_dict = defaultdict(lambda: {
        "first_name": "",
        "last_name": "",
        "linkedin_url": "",
        "liked_posts": set()
    })

    for entry in data:
        post_url = entry.get("socialContent", {}).get("shareUrl", "")
        for reaction in entry.get("reactionElements", []):
            mini_profile = reaction.get("image", {}).get("attributes", [{}])[0].get("miniProfile", {})
            first_name = mini_profile.get("firstName", "")
            last_name = mini_profile.get("lastName", "")
            linkedin_id = mini_profile.get("publicIdentifier", "")
            linkedin_url = f"https://www.linkedin.com/in/{linkedin_id}" if linkedin_id else ""

            persoon = personen_dict[linkedin_url]
            persoon["first_name"] = first_name
            persoon["last_name"] = last_name
            persoon["linkedin_url"] = linkedin_url
            persoon["liked_posts"].add(post_url)

    resultaten = []
    for persoon in personen_dict.values():
        resultaten.append({
            "first_name": persoon["first_name"],
            "last_name": persoon["last_name"],
            "linkedin_url": persoon["linkedin_url"],
            "liked_posts": list(persoon["liked_posts"]),
            "total_likes": len(persoon["liked_posts"])
        })

    return pd.DataFrame(resultaten)

# ───────── Start verwerking ─────────
latest_json = find_latest_month_file()
if latest_json is None:
    raise FileNotFoundError("Geen maandelijkse JSON gevonden in 'monthly_data/'")

print(f"📥 JSON inlezen: {latest_json.name}")
df_new = process_json_to_df(latest_json)

# Historie ophalen
hist_fp, hist_n = find_latest_history_file()
if hist_fp:
    df_hist = pd.read_csv(hist_fp)
    df_hist["liked_posts"] = df_hist["liked_posts"].apply(str_to_list)
    print(f"🔄 Historie geladen: {hist_fp.name}")
else:
    df_hist = pd.DataFrame(columns=["first_name", "last_name", "linkedin_url", "liked_posts", "total_likes"])
    print("🆕 Geen bestaand historie-bestand gevonden. Nieuwe historie wordt aangemaakt.")

# ───────── Combineer data ─────────
combined = defaultdict(lambda: {"first_name": "", "last_name": "", "linkedin_url": "", "liked_posts": set()})

def add_rows(df):
    for _, row in df.iterrows():
        key = row["linkedin_url"]
        pers = combined[key]
        pers["first_name"] = row.get("first_name", pers["first_name"])
        pers["last_name"] = row.get("last_name", pers["last_name"])
        pers["linkedin_url"] = key
        pers["liked_posts"].update(row["liked_posts"])

# Voeg eerst de oude historie toe
add_rows(df_hist)

# Voeg daarna de nieuwe maanddata toe
add_rows(df_new)

# Zet de gecombineerde data om naar een lijst van dicts
rows = []
for pers in combined.values():
    liked = list(pers["liked_posts"])  # omzetten naar lijst voor CSV
    rows.append({
        "first_name": pers["first_name"],
        "last_name": pers["last_name"],
        "linkedin_url": pers["linkedin_url"],
        "liked_posts": liked,
        "total_likes": len(liked)
    })

# Zet de lijst om naar een DataFrame en sorteer op meest actieve mensen
df_final = pd.DataFrame(rows).sort_values("total_likes", ascending=False)

# Zet de liked_posts weer om naar string voor CSV-opslag
df_final["liked_posts"] = df_final["liked_posts"].apply(lambda x: str(x))

# Zorg dat de history-map bestaat
HISTORY_DIR.mkdir(exist_ok=True)

# Bepaal nieuw bestandsnummer
new_n = hist_n + 1
out_path = HISTORY_DIR / f"linkedin_likes_history_{new_n}.csv"

# Schrijf de nieuwe gecombineerde dataset weg
df_final.to_csv(out_path, index=False)

print(f"✅ Gecombineerde dataset opgeslagen: {out_path.name}")
