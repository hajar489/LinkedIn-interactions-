#!/bin/bash

echo "▶️  Run update_linkedin_likes.py..."
python update_linkedin_likes.py

echo "📂  Voeg nieuwe CSV toe aan Git..."
git add history/linkedin_likes_history_*.csv

echo "📝  Commit wijzigingen..."
git commit -m "Voeg nieuwe history-bestanden toe"

echo "🚀  Push naar GitHub..."
git push

echo "✅  Alles voltooid!"
