import os
from datetime import datetime, timedelta
from database import get_client


def generate_sequence(lead_id):
    sequence = [
        {"touch": 1, "channel": "email", "delay_days": 1},
        {"touch": 2, "channel": "phone", "delay_days": 2},
        {"touch": 3, "channel": "email", "delay_days": 4},
        {"touch": 4, "channel": "text",  "delay_days": 7},
        {"touch": 5, "channel": "phone", "delay_days": 10},
        {"touch": 6, "channel": "email", "delay_days": 14},
        {"touch": 7, "channel": "text",  "delay_days": 21},
        {"touch": 8, "channel": "phone", "delay_days": 30},
        {"touch": 9, "channel": "email", "delay_days": 45},
    ]
    base_date = datetime.now()
    rows = [
        {
            'lead_id':        lead_id,
            'touch_number':   step['touch'],
            'channel':        step['channel'],
            'scheduled_date': (base_date + timedelta(days=step['delay_days'])).strftime('%Y-%m-%d'),
            'status':         'pending',
        }
        for step in sequence
    ]
    get_client().table('follow_ups').insert(rows).execute()
    return True


def get_pending_follow_ups():
    result = (
        get_client()
        .table('follow_ups')
        .select('id, touch_number, channel, scheduled_date, leads(id, name)')
        .eq('status', 'pending')
        .order('scheduled_date')
        .execute()
    )
    tasks = []
    for row in result.data or []:
        lead = row.pop('leads', {}) or {}
        row['lead_id'] = lead.get('id')
        row['name'] = lead.get('name')
        tasks.append(row)
    return tasks


def mark_touch_completed(follow_up_id, notes=''):
    get_client().table('follow_ups').update({
        'status':         'done',
        'completed_date': datetime.now().isoformat(),
        'notes':          notes,
    }).eq('id', follow_up_id).execute()


def get_todays_followups():
    """Returns pending follow-ups scheduled for today."""
    today = datetime.now().strftime('%Y-%m-%d')
    return [f for f in get_pending_follow_ups() if f.get('scheduled_date', '') == today]


def send_daily_digest():
    """Sends a daily digest email summarising today's pending follow-ups via SendGrid."""
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    from config import EMAIL, COMPANY_NAME

    todays_tasks = get_todays_followups()
    if not todays_tasks:
        print("No follow-ups due today. Daily digest skipped.")
        return False

    task_lines = ''.join(
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
