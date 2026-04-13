import os
import sys
import logging
from datetime import datetime

# Ensure the logs directory exists
os.makedirs("logs", exist_ok=True)

# Setup logging configuration
logging.basicConfig(
    filename="logs/scanner.log",
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Import module functions
try:
    from scanner import run_all_scanners
    from scorer import score_all_leads
    from tracker import get_todays_followups, send_daily_digest
except ImportError as e:
    logging.error(f"Import Error: {e}. Make sure all functions exist.")
    sys.exit(1)

def main():
    try:
        # 1. Run Scanners
        summary = run_all_scanners()
        
        # 2. Score Leads
        score_all_leads()
        
        # 3. Get Follow-ups
        todays_followups = get_todays_followups()
        
        # 4. Check if it's the 7 AM run to send the digest
        # Note: Server time zone affects datetime.now().hour
        current_hour = datetime.now().hour
        digest_sent = "No"
        
        if current_hour == 7:
            send_daily_digest()
            digest_sent = "Yes"
            
        # Log Summary
        logging.info("Scanner run complete")
        logging.info(f"- New permit leads: {summary.get('permits_found', 0)}")
        logging.info(f"- New closing leads: {summary.get('closings_found', 0)}")
        logging.info(f"- Duplicates skipped: {summary.get('duplicates_skipped', 0)}")
        logging.info("- All leads re-scored")
        logging.info(f"- Follow-ups due today: {len(todays_followups)}")
        logging.info(f"- Daily digest sent: {digest_sent}")
        
    except Exception as e:
        logging.error(f"Cron execution failed: {e}")

if __name__ == "__main__":
    main()