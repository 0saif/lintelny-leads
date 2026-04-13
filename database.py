import os
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

_client: Client = None


def get_client() -> Client:
    global _client
    if _client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")
        _client = create_client(url, key)
    return _client


def init_db():
    """Validates the Supabase connection on startup. Tables are managed in the Supabase dashboard."""
    get_client()


def insert_lead(lead_data: dict) -> int | None:
    now = datetime.now().isoformat()
    result = get_client().table('leads').insert({
        'source':         lead_data.get('source', 'manual'),
        'name':           lead_data.get('name', 'Unknown'),
        'address':        lead_data.get('address', ''),
        'zip':            lead_data.get('zip', ''),
        'borough_county': lead_data.get('borough_county', ''),
        'property_type':  lead_data.get('property_type', ''),
        'signal_type':    lead_data.get('signal_type', ''),
        'signal_date':    lead_data.get('signal_date', ''),
        'score':          lead_data.get('score', 0),
        'status':         lead_data.get('status', 'new'),
        'created_at':     now,
        'updated_at':     now,
    }).execute()
    return result.data[0]['id'] if result.data else None


def get_all_leads() -> list:
    result = (
        get_client()
        .table('leads')
        .select('*')
        .order('score', desc=True)
        .order('created_at', desc=True)
        .execute()
    )
    return result.data or []


def update_lead_status(lead_id: int, new_status: str):
    get_client().table('leads').update({
        'status':     new_status,
        'updated_at': datetime.now().isoformat(),
    }).eq('id', lead_id).execute()


def get_lead_by_id(lead_id: int) -> dict | None:
    result = get_client().table('leads').select('*').eq('id', lead_id).maybe_single().execute()
    return result.data
