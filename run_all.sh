#!/bin/bash

python update_linkedin_likes.py

git add history/linkedin_likes_history_*.csv
git add -A  # Voeg ook eventuele verwijderde/bijgewerkte bestanden toe

git commit -m "Voeg nieuwe history-bestanden toe"

git pull --rebase

git push

echo "✅  Alles voltooid!"
