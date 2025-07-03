import subprocess

print("▶️  Run update_linkedin_likes.py...")
subprocess.run(["python", "update_linkedin_likes.py"], check=True)

print("📂  Voeg nieuwe CSV toe aan Git...")
subprocess.run(["git", "add", "history/linkedin_likes_history_*.csv"], shell=True, check=True)

print("📝  Commit wijzigingen...")
subprocess.run(["git", "commit", "-m", "Voeg nieuwe history-bestanden toe"], check=True)

print("🚀  Push naar GitHub...")
subprocess.run(["git", "push"], check=True)

print("✅  Alles voltooid!")
