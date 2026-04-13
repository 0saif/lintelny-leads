#!/bin/bash

# ==========================================================
# LINTEL NY - COMMAND CENTER CRON SETUP
# ==========================================================
#
# INSTRUCTIONS FOR UBUNTU / MAC:
# 1. Open your terminal.
# 2. Open the cron editor by typing: crontab -e
# 3. If prompted, select your preferred editor (nano is easiest).
# 4. Paste the two cron commands below into the file.
# 5. VERY IMPORTANT: Change "/path/to/lintelny-leads" to your actual absolute folder path!
# 6. Save and exit (In nano: press Ctrl+O, Enter, then Ctrl+X).
# 7. Verify it is installed by typing: crontab -l
#
# ==========================================================

# Run scanner every 6 hours (00:00, 06:00, 12:00, 18:00)
0 */6 * * * cd /path/to/lintelny-leads && python3 cron_runner.py >> logs/cron.log 2>&1

# Send daily digest at 7 AM
0 7 * * * cd /path/to/lintelny-leads && python3 -c "from tracker import send_daily_digest; send_daily_digest()" >> logs/cron.log 2>&1