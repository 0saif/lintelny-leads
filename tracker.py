import os
from datetime import datetime, timedelta
import sqlite3
from database import get_connection

def generate_sequence(lead_id):
    conn = get_connection()
    cursor = conn.cursor()
    
    # 9-Touch Follow Up Sequence
    sequence = [
        {"touch": 1, "channel": "email", "delay_days": 1},
        {"touch": 2, "channel": "phone", "delay_days": 2},
        {"touch": 3, "channel": "email", "delay_days": 4},
        {"touch": 4, "channel": "text", "delay_days": 7},
        {"touch": 5, "channel": "phone", "delay_days": 10},
        {"touch": 6, "channel": "email", "delay_days": 14},
        {"touch": 7, "channel": "text", "delay_days": 21},
        {"touch": 8, "channel": "phone", "delay_days": 30},
        {"touch": 9, "channel": "email", "delay_days": 45},
    ]
    
    base_date = datetime.now()
    
    for step in sequence:
        scheduled = (base_date + timedelta(days=step["delay_days"])).strftime("%Y-%m-%d")
        cursor.execute("""
            INSERT INTO follow_ups (lead_id, touch_number, channel, scheduled_date, status)
            VALUES (?, ?, ?, ?, ?)
        """, (lead_id, step["touch"], step["channel"], scheduled, "pending"))
        
    conn.commit()
    conn.close()
    return True

def get_pending_follow_ups():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT f.id, f.touch_number, f.channel, f.scheduled_date, l.name, l.id as lead_id
        FROM follow_ups f
        JOIN leads l ON f.lead_id = l.id 
        WHERE f.status = 'pending' 
        ORDER BY f.scheduled_date ASC
    """
    try:
        cursor.execute(query)
        rows = [dict(row) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        rows = []
    conn.close()
    return rows

def mark_touch_completed(follow_up_id, notes=""):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    cursor.execute("""
        UPDATE follow_ups
        SET status = 'done', completed_date = ?, notes = ?
        WHERE id = ?
    """, (now, notes, follow_up_id))
    conn.commit()
    conn.close()

def get_todays_followups():
    """Returns pending follow-ups that are scheduled for today."""
    today = datetime.now().strftime('%Y-%m-%d')
    all_pending = get_pending_follow_ups()
    return [f for f in all_pending if f.get('scheduled_date', '') == today]

def send_daily_digest():
    """Sends a daily digest email summarising today's pending follow-ups via SendGrid."""
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    from config import EMAIL, COMPANY_NAME

    todays_tasks = get_todays_followups()
    if not todays_tasks:
        print("No follow-ups due today. Daily digest skipped.")
        return False

    task_lines = "".join(
        f"<li>{t.get('channel', '').title()} — {t.get('name', 'Unknown')} "
        f"(Touch #{t.get('touch_number')}, Due: {t.get('scheduled_date')})</li>"
        for t in todays_tasks
    )

    subject = f"[{COMPANY_NAME}] Daily Follow-Up Digest — {datetime.now().strftime('%b %d, %Y')}"
    body = (
        f"<h2>{COMPANY_NAME} Daily Digest</h2>"
        f"<p>You have <strong>{len(todays_tasks)}</strong> follow-up(s) due today:</p>"
        f"<ul>{task_lines}</ul>"
        f"<p>Log into your command center to manage these tasks.</p>"
    )

    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key:
        print("SENDGRID_API_KEY missing. Cannot send daily digest.")
        return False

    try:
        message = Mail(from_email=EMAIL, to_emails=EMAIL, subject=subject, html_content=body)
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        success = response.status_code in (200, 201, 202)
        print(f"Daily digest {'sent' if success else 'failed'} (status: {response.status_code})")
        return success
    except Exception as e:
        print(f"Daily digest send error: {e}")
        return False