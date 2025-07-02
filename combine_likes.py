#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python3
"""
Voeg de meest recente maandexport toe aan linkedin_likes_history.csv
"""
import os
import re
import ast
from collections import defaultdict
from datetime import datetime
import pandas as pd
from pathlib import Path

# ───────────────────────── 1. Pad-instellingen ─────────────────────────
ROOT_DIR        = Path(__file__).parent            # project-root
MONTHLY_DIR     = ROOT_DIR / "monthly_data"        # map met maand-exports
HISTORY_FILE    = ROOT_DIR / "linkedin_likes_history.csv"

# ───────────────────────── 2. Hulpfuncties ──────────────────────────
DATE_PATTERN = re.compile(
    r"LinkedIn_interactions_(\d{2})[-_](\d{2})[-_](\d{4})\.csv",
    re.IGNORECASE,
)

def find_latest_month_file(folder: Path) -> Path | None:
    """Zoek het bestand met de nieuwste datum in de naam."""
    latest_file = None
    latest_date = None

    for fp in folder.glob("LinkedIn_interactions_*"):
        m = DATE_PATTERN.match(fp.name)
        if not m:
            continue  # bestandsnaam past niet
        day, month, year = map(int, m.groups())
        file_date = datetime(year, month, day)
        if latest_date is None or file_date > latest_date:
            latest_date, latest_file = file_date, fp

    return latest_file

def str_to_list(x):
    """Zet '[]'-string om naar list; laat lists ongemoeid."""
    if isinstance(x, list):
        return x
    if isinstance(x, float) and pd.isna(x):
        return []
    try:
        return ast.literal_eval(x)
    except Exception:
        return []

# ───────────────────────── 3. Inlezen ──────────────────────────
if HISTORY_FILE.exists():
    df_hist = pd.read_csv(HISTORY_FILE)
    df_hist["liked_posts"] = df_hist["liked_posts"].apply(str_to_list)
else:
    df_hist = pd.DataFrame(
        columns=["first_name", "last_name", "linkedin_url", "liked_posts", "total_likes"]
    )

latest_file = find_latest_month_file(MONTHLY_DIR)
if latest_file is None:
    raise FileNotFoundError("Geen maand-export gevonden in ‘monthly_data/’")

df_new = pd.read_csv(latest_file)
df_new["liked_posts"] = df_new["liked_posts"].apply(str_to_list)

print(f"➡ Voeg data toe uit: {latest_file.name}")

# ───────────────────────── 4. Samenvoegen ─────────────────────────
combined = defaultdict(lambda: {"first_name": "", "last_name": "",
                                "linkedin_url": "", "liked_posts": set()})

def add_rows(df: pd.DataFrame):
    for _, row in df.iterrows():
        key = row["linkedin_url"]
        pers = combined[key]
        pers["first_name"] = row.get("first_name", pers["first_name"])
        pers["last_name"]  = row.get("last_name",  pers["last_name"])
        pers["linkedin_url"] = key
        pers["liked_posts"].update(row["liked_posts"])

add_rows(df_hist)
add_rows(df_new)

# ───────────────────────── 5. Terug naar DataFrame ─────────────────────────
final_rows = []
for p in combined.values():
    urls = list(p["liked_posts"])
    final_rows.append(
        {
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "linkedin_url": p["linkedin_url"],
            "liked_posts": urls,
            "total_likes": len(urls),
        }
    )

df_final = pd.DataFrame(final_rows).sort_values("total_likes", ascending=False)

# liked_posts als string opslaan zodat CSV leesbaar blijft
df_final["liked_posts"] = df_final["liked_posts"].apply(lambda x: str(x))

df_final.to_csv(HISTORY_FILE, index=False)

print(f"✅ Bijgewerkt!  {len(df_new)} nieuwe regels verwerkt.")
print(f"📄 Bestand opgeslagen → {HISTORY_FILE}")

