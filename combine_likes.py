#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combineer laatste maand-JSON met bestaande likes-historie.
"""

import json, re, ast, os
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import pandas as pd

# ─── Config ──────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent
MONTHLY_DIR = ROOT / "monthly_data"
HISTORY_DIR = ROOT / "history"
MONTH_RE    = re.compile(r"LinkedIn_interactions_(\d{2})[-_](\d{2})[-_](\d{4})\.json")
HIST_RE     = re.compile(r"linkedin_likes_history_(\d+)\.csv", re.I)

# ─── Hulpfuncties ────────────────────────────────────────────────────────────
def latest_month_file():
    """Geef pad van laatst gedateerd maand-JSON-bestand."""
    latest_dt, latest_fp = None, None
    for fp in MONTHLY_DIR.glob("LinkedIn_interactions_*.json"):
        m = MONTH_RE.match(fp.name)
        if not m: continue
        dd, mm, yyyy = map(int, m.groups())
        dt = datetime(yyyy, mm, dd)
        if latest_dt is None or dt > latest_dt:
            latest_dt, latest_fp = dt, fp
    return latest_fp

def latest_history_file():
    """Geef (pad, n).  n==0 als geen history bestaat."""
    latest_fp, latest_n = None, 0
    for fp in HISTORY_DIR.glob("linkedin_likes_history_*.csv"):
        m = HIST_RE.match(fp.name)
        if m and int(m.group(1)) > latest_n:
            latest_n, latest_fp = int(m.group(1)), fp
    return latest_fp, latest_n

def str_to_list(x):
    if isinstance(x, list):                    return x
    if pd.isna(x):                             return []
    try:                                       return ast.literal_eval(x)
    except Exception:                          return []

def json_to_df(json_path: Path) -> pd.DataFrame:
    """Kopie uit je oude voorbeeld: JSON ⇒ likes-per-persoon DataFrame."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    people = defaultdict(lambda: {
        "first_name": "", "last_name": "",
        "linkedin_url": "", "liked_posts": set()
    })

    for entry in data:
        post_url = entry.get("socialContent", {}).get("shareUrl", "")
        for reaction in entry.get("reactionElements", []):
            mini = reaction.get("image", {}).get("attributes", [{}])[0].get("miniProfile", {})
            url  = f'https://www.linkedin.com/in/{mini.get("publicIdentifier","")}' if mini.get("publicIdentifier") else ""
            p    = people[url]
            p["first_name"]  = mini.get("firstName", "")  or p["first_name"]
            p["last_name"]   = mini.get("lastName", "")   or p["last_name"]
            p["linkedin_url"] = url
            if post_url: p["liked_posts"].add(post_url)

    rows = [{
        "first_name": p["first_name"],
        "last_name":  p["last_name"],
        "linkedin_url": p["linkedin_url"],
        "liked_posts": list(p["liked_posts"]),
        "total_likes": len(p["liked_posts"])
    } for p in people.values()]

    return pd.DataFrame(rows)

# ─── 1) Nieuwste bestanden zoeken ────────────────────────────────────────────
latest_json = latest_month_file()
if latest_json is None:
    raise FileNotFoundError("Geen maand-JSON in monthly_data/")

hist_fp, hist_n = latest_history_file()

print(f"📥 JSON gebruikt : {latest_json.name}")
print("📚 Historie      :", hist_fp.name if hist_fp else "(geen)")

# ─── 2) DataFrames bouwen ────────────────────────────────────────────────────
df_new = json_to_df(latest_json)

if hist_fp:
    df_hist = pd.read_csv(hist_fp)
    df_hist["liked_posts"] = df_hist["liked_posts"].apply(str_to_list)
else:
    df_hist = pd.DataFrame(columns=df_new.columns)

# ─── 3) Mergen zonder dubbels ────────────────────────────────────────────────
combined = defaultdict(lambda: {
    "first_name": "", "last_name": "",
    "linkedin_url": "", "liked_posts": set()
})

def add(df):
    for _, r in df.iterrows():
        url = r["linkedin_url"]
        c   = combined[url]
        c["first_name"]   = r.get("first_name") or c["first_name"]
        c["last_name"]    = r.get("last_name")  or c["last_name"]
        c["linkedin_url"] = url
        c["liked_posts"].update(r.get("liked_posts", []))

add(df_hist)   # eerst oude data
add(df_new)    # dan de nieuwe maand

rows = [{
    "first_name": c["first_name"],
    "last_name":  c["last_name"],
    "linkedin_url": url,
    "liked_posts": list(c["liked_posts"]),
    "total_likes": len(c["liked_posts"])
} for url, c in combined.items()]

df_final = (pd.DataFrame(rows)
            .sort_values("total_likes", ascending=False))

# liked_posts weer als string vóór CSV-export
df_final["liked_posts"] = df_final["liked_posts"].apply(lambda lst: ",".join(lst))

# ─── Wegschrijven ────────────────────────────────────────────────────────────
HISTORY_DIR.mkdir(exist_ok=True)
out_fp = HISTORY_DIR / f"linkedin_likes_history_{hist_n+1}.csv"
df_final.to_csv(out_fp, index=False)
print(f"✅ Geschreven → {out_fp.name}")
