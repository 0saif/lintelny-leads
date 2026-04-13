import sqlite3
from datetime import datetime

DB_NAME = "lintelny.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            name TEXT,
            address TEXT,
            zip TEXT,
            borough_county TEXT,
            property_type TEXT,
            signal_type TEXT,
            signal_date TEXT,
            score INTEGER,
            status TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS follow_ups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            touch_number INTEGER,
            channel TEXT,
            scheduled_date TEXT,
            completed_date TEXT,
            notes TEXT,
            status TEXT,
            FOREIGN KEY(lead_id) REFERENCES leads(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            channel TEXT,
            subject TEXT,
            body TEXT,
            sent_at TEXT,
            opened INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            FOREIGN KEY(lead_id) REFERENCES leads(id)
        )
    """)
    
    conn.commit()
    conn.close()

def insert_lead(lead_data):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    
    cursor.execute("""
        INSERT INTO leads (source, name, address, zip, borough_county, property_type, signal_type, signal_date, score, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        lead_data.get('source', 'manual'),
        lead_data.get('name', 'Unknown'),
        lead_data.get('address', ''),
        lead_data.get('zip', ''),
        lead_data.get('borough_county', ''),
        lead_data.get('property_type', ''),
        lead_data.get('signal_type', ''),
        lead_data.get('signal_date', ''),
        lead_data.get('score', 0),
        lead_data.get('status', 'new'),
        now,
        now
    ))
    lead_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return lead_id

def get_all_leads():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leads ORDER BY score DESC, created_at DESC")
    leads = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return leads

def update_lead_status(lead_id, new_status):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    cursor.execute("UPDATE leads SET status = ?, updated_at = ? WHERE id = ?", (new_status, now, lead_id))
    conn.commit()
    conn.close()
    
def get_lead_by_id(lead_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
    lead = cursor.fetchone()
    conn.close()
    return dict(lead) if lead else None