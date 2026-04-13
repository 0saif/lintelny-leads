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
        SELECT f.id, f.touch_number, f.channel, f.scheduled_date, l.name, l.id as lead_id, l.phone 
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