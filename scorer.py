from datetime import datetime
from database import get_connection

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
    days_ago = 999  # Default to very old if missing/unparseable
    
    if signal_date_str:
        try:
            # Handle both 'YYYY-MM-DD...' and 'MM/DD/YYYY...' formats
            if 'T' in signal_date_str:
                date_part = signal_date_str.split('T')[0]
                dt = datetime.strptime(date_part, "%Y-%m-%d")
            elif '-' in signal_date_str:
                dt = datetime.strptime(signal_date_str[:10], "%Y-%m-%d")
            else:
                dt = datetime.strptime(signal_date_str[:10], "%m/%d/%Y")
                
            days_ago = (datetime.now() - dt).days
        except Exception:
            pass # Keep default 999 if parsing fails

    # Prevent negative days if a future date gets recorded by API error
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
    
    if borough == 'Brooklyn':
        score += 25
    elif borough == 'Manhattan':
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
        # Default to 0 if key is missing or not a number
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
        # Default value points for manual imports
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
    Retrieves all leads from the database, recalculates their scores, 
    and updates the database in bulk. Returns the number of leads updated.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Fetch all leads to score them
    cursor.execute("SELECT * FROM leads")
    leads = [dict(row) for row in cursor.fetchall()]
    
    updates = []
    for lead in leads:
        new_score = score_lead(lead)
        updates.append((new_score, lead['id']))
        
    # Bulk update for performance
    if updates:
        cursor.executemany("UPDATE leads SET score = ? WHERE id = ?", updates)
        conn.commit()
        
    conn.close()
    return len(updates)

def get_priority_leads(limit: int = 20) -> list:
    """
    Returns the top N newest leads, sorted by their score descending.
    Filters exclusively for leads where status='new'.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT * FROM leads 
        WHERE status = 'new' 
        ORDER BY score DESC 
        LIMIT ?
    """
    cursor.execute(query, (limit,))
    leads = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return leads