from datetime import datetime
from database import get_client


def score_lead(lead: dict) -> int:
    """
    Evaluates a lead dictionary and returns a score from 0 to 100
    based on Recency, Location, Project Value, and Property Type.
    """
    score = 0

    # ==========================================
    # 1. RECENCY (0-30 points)
    # ==========================================
    signal_date_str = lead.get('signal_date', '')
    days_ago = 999

    if signal_date_str:
        try:
            if 'T' in signal_date_str:
                date_part = signal_date_str.split('T')[0]
                dt = datetime.strptime(date_part, "%Y-%m-%d")
            elif '-' in signal_date_str:
                dt = datetime.strptime(signal_date_str[:10], "%Y-%m-%d")
            else:
                dt = datetime.strptime(signal_date_str[:10], "%m/%d/%Y")
            days_ago = (datetime.now() - dt).days
        except Exception:
            pass

    days_ago = max(0, days_ago)

    if days_ago <= 3:
        score += 30
    elif days_ago <= 7:
        score += 25
    elif days_ago <= 14:
        score += 20
    elif days_ago <= 30:
        score += 10
    else:
        score += 5

    # ==========================================
    # 2. LOCATION TIER (0-25 points)
    # ==========================================
    borough = str(lead.get('borough_county', '')).title().strip()

    if borough in ('Brooklyn', 'Manhattan'):
        score += 25
    elif 'Nassau' in borough:
        score += 20
    elif borough == 'Queens':
        score += 18
    elif 'Suffolk' in borough:
        score += 15
    else:
        score += 5

    # ==========================================
    # 3. PROJECT VALUE SIGNAL (0-25 points)
    # ==========================================
    source = lead.get('source', '')

    if source == 'permit':
        try:
            cost = float(lead.get('estimated_job_costs', 0))
        except (ValueError, TypeError):
            cost = 0

        if cost > 100000:
            score += 25
        elif cost >= 50000:
            score += 20
        elif cost >= 25000:
            score += 15
        else:
            score += 10

    elif source == 'closing':
        try:
            price = float(lead.get('sale_price', 0))
        except (ValueError, TypeError):
            price = 0

        if price > 1000000:
            score += 25
        elif price >= 500000:
            score += 20
        else:
            score += 15

    else:
        score += 10

    # ==========================================
    # 4. PROPERTY TYPE (0-20 points)
    # ==========================================
    prop_type = str(lead.get('property_type', '')).lower()

    if 'brownstone' in prop_type or 'townhouse' in prop_type:
        score += 20
    elif 'co-op' in prop_type or 'coop' in prop_type:
        score += 18
    elif 'condo' in prop_type:
        score += 16
    elif 'single family' in prop_type or 'single-family' in prop_type:
        score += 15
    elif 'multi-family' in prop_type or 'multifamily' in prop_type:
        score += 12
    else:
        score += 8

    return min(score, 100)


def score_all_leads() -> int:
    """
    Fetches all leads, recalculates scores, and bulk-upserts back to Supabase.
    Returns the number of leads updated.
    """
    leads = get_client().table('leads').select('*').execute().data or []
    updates = [{'id': lead['id'], 'score': score_lead(lead)} for lead in leads]
    if updates:
        get_client().table('leads').upsert(updates).execute()
    return len(updates)


def get_priority_leads(limit: int = 20) -> list:
    """Returns the top N new leads sorted by score descending."""
    result = (
        get_client()
        .table('leads')
        .select('*')
        .eq('status', 'new')
        .order('score', desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []
