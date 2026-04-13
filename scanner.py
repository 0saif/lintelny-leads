import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from config import COVERAGE_ZIPS
from database import get_client, insert_lead
from scorer import score_lead

load_dotenv()

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def _rate_limited_get(url, params=None):
    """
    Makes a GET request to the NYC Open Data API, enforcing a max 
    limit of 1 request per second to avoid being blocked.
    """
    time.sleep(1) # Enforce 1 request per second
    
    if params is None:
        params = {}
        
    token = os.getenv("NYC_OPEN_DATA_TOKEN")
    if token:
        params["$$app_token"] = token
        
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def _address_exists_in_db(address):
    """
    Checks if a given address already exists in the leads database.
    """
    if not address:
        return False
        
    result = get_client().table('leads').select('id').ilike('address', address).execute()
    return len(result.data) > 0

def _get_all_coverage_zips():
    """
    Flattens the COVERAGE_ZIPS dictionary into a single list.
    """
    return [str(z) for z_list in COVERAGE_ZIPS.values() for z in z_list]

# ==========================================
# SCANNER 1: DOB PERMIT SCANNER
# ==========================================

def scan_dob_permits():
    """
    Scans the DOB Job Application Filings dataset for renovation permits.
    Date filtering is handled locally to bypass Socrata SoQL text-date errors.
    """
    endpoint = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
    
    # Simplify API query to prevent 400 Bad Request errors
    params = {
        "$where": "job_type IN ('A1', 'A2', 'A3')",
        "$limit": 2000  # Pull a large batch of recent records to filter locally
    }
    
    data = _rate_limited_get(endpoint, params)
    coverage_zips = _get_all_coverage_zips()
    fourteen_days_ago = datetime.now() - timedelta(days=14)
    
    found_count = 0
    skipped_count = 0
    
    for item in data:
        # Local Filter 1: Filing Date
        # Safely extract from Socrata's irregular column names (pre__filing_date)
        filing_date_str = item.get('pre__filing_date', item.get('pre_filing_date', ''))
        try:
            if 'T' in filing_date_str:
                filing_date = datetime.strptime(filing_date_str.split('T')[0], '%Y-%m-%d')
            elif filing_date_str:
                filing_date = datetime.strptime(filing_date_str[:10], '%m/%d/%Y')
            else:
                continue
                
            if filing_date < fourteen_days_ago:
                continue
        except Exception:
            continue # Skip records with unparsable dates
            
        zip_code = str(item.get('zip', ''))
        
        # Local Filter 2: Zip Code
        if zip_code not in coverage_zips:
            continue
            
        # Local Filter 3: Work Type (General Construction, Plumbing, Electrical)
        job_desc = str(item.get('job_description', '')).upper()
        has_gc = item.get('general_construction', '') == 'X'
        has_pl = item.get('plumbing', '') == 'X'
        has_el = item.get('electrical', '') == 'X'
        
        desc_matches = any(kw in job_desc for kw in ["GENERAL CONSTRUCTION", "PLUMBING", "ELECTRICAL"])
        
        if not (has_gc or has_pl or has_el or desc_matches):
            continue
            
        # Extract fields
        house_num = item.get('house__', '')
        street_name = item.get('street_name', '')
        address = f"{house_num} {street_name}".strip()
        borough = item.get('borough', '').title()
        owner_name = f"{item.get('owner_s_first_name', '')} {item.get('owner_s_last_name', '')}".strip()
        
        if not owner_name.strip() or len(owner_name) <= 2:
            owner_name = item.get('owner_s_business_name', 'Unknown Owner')
            
        # Deduplication
        if _address_exists_in_db(address):
            skipped_count += 1
            continue
            
        score = score_lead({
            "source": "permit",
            "borough_county": borough,
            "signal_date": filing_date_str,
            "property_type": "Residential" if item.get('residential', '') == 'YES' else "Commercial"
        })
        
        lead_data = {
            "source": "permit",
            "name": owner_name,
            "address": address,
            "zip": zip_code,
            "borough_county": borough,
            "property_type": "Residential" if item.get('residential', '') == 'YES' else "Commercial",
            "signal_type": "dob_filing",
            "signal_date": filing_date_str,
            "score": score,
            "status": "new"
        }
        
        insert_lead(lead_data)
        found_count += 1
        
    return {"found": found_count, "skipped": skipped_count}

# ==========================================
# SCANNER 2: ACRIS REAL ESTATE SCANNER
# ==========================================

def scan_acris_closings():
    """
    Scans the ACRIS Master dataset for recent property transfers (DEEDs).
    Date/Borough filtering is handled locally to bypass Socrata column mismatches.
    """
    master_endpoint = "https://data.cityofnewyork.us/resource/bnx9-e6tj.json"
    parties_endpoint = "https://data.cityofnewyork.us/resource/636b-3b5g.json"
    
    # Simplify API query: document_id starts with YYYYMMDD, so DESC gives newest
    master_params = {
        "$where": "doc_type = 'DEED'",
        "$limit": 1000,
        "$order": "document_id DESC" 
    }
    
    master_data = _rate_limited_get(master_endpoint, master_params)
    
    thirty_days_ago = datetime.now() - timedelta(days=30)
    found_count = 0
    skipped_count = 0
    borough_map = {"1": "Manhattan", "3": "Brooklyn", "4": "Queens"}
    
    for doc in master_data:
        document_id = doc.get('document_id')
        if not document_id:
            continue
            
        # Local Filter 1: Borough (using the correct API field 'recorded_borough')
        borough_code = str(doc.get('recorded_borough', doc.get('borough', '')))
        if borough_code not in borough_map:
            continue
            
        # Local Filter 2: Date 
        doc_date_str = doc.get('document_date', doc.get('recorded_datetime', ''))
        try:
            if 'T' in doc_date_str:
                doc_date = datetime.strptime(doc_date_str.split('T')[0], '%Y-%m-%d')
            elif doc_date_str:
                doc_date = datetime.strptime(doc_date_str[:10], '%m/%d/%Y')
            else:
                continue
                
            if doc_date < thirty_days_ago:
                continue
        except Exception:
            continue
            
        # Get buyer (party type 2 represents the buyer/grantee in ACRIS DEEDs)
        party_params = {
            "$where": f"document_id = '{document_id}' AND party_type = '2'",
            "$limit": 1
        }
        
        party_data = _rate_limited_get(parties_endpoint, party_params)
        buyer_name = party_data[0].get('name', 'Unknown Buyer') if party_data else "Unknown Buyer"
        
        borough_name = borough_map.get(borough_code, "Unknown")
        block = doc.get('block', '')
        lot = doc.get('lot', '')
        sale_amount = doc.get('document_amt', '0')
        
        address = f"Block {block} Lot {lot}, {borough_name}"
        
        if _address_exists_in_db(address):
            skipped_count += 1
            continue
            
        try:
            sale_amount_float = float(sale_amount)
            if sale_amount_float < 10000 or sale_amount_float > 50000000:
                continue
        except ValueError:
            continue
            
        score = score_lead({
            "source": "closing",
            "borough_county": borough_name,
            "signal_date": doc_date_str,
            "property_type": "Residential"
        })
        
        lead_data = {
            "source": "closing",
            "name": buyer_name,
            "address": address,
            "zip": "", 
            "borough_county": borough_name,
            "property_type": "Residential",
            "signal_type": "property_purchase",
            "signal_date": doc_date_str,
            "score": score + 10,
            "status": "new"
        }
        
        insert_lead(lead_data)
        found_count += 1
        
    return {"found": found_count, "skipped": skipped_count}

# ==========================================
# SCANNER 3: MANUAL LEAD IMPORT
# ==========================================

def import_manual_leads_csv(uploaded_file):
    """
    Accepts a Streamlit UploadedFile (CSV), parses it via Pandas, 
    and inserts rows into the leads database.
    """
    try:
        df = pd.read_csv(uploaded_file)
        
        required_cols = ['name', 'address']
        for col in required_cols:
            if col not in df.columns:
                return False, f"Missing required column: {col}"
                
        imported_count = 0
        skipped_count = 0
        
        for _, row in df.iterrows():
            address = str(row.get('address', '')).strip()
            
            if _address_exists_in_db(address):
                skipped_count += 1
                continue
                
            zip_code = str(row.get('zip', ''))
            borough = str(row.get('borough', ''))
            
            lead_data = {
                "source": "manual",
                "name": str(row.get('name', 'Unknown')),
                "address": address,
                "zip": zip_code,
                "borough_county": borough,
                "property_type": "Unknown",
                "signal_type": "manual_upload",
                "signal_date": datetime.now().isoformat(),
                "score": score_lead({
                    "source": "manual",
                    "borough_county": borough,
                    "signal_date": datetime.now().isoformat()
                }),
                "status": "new"
            }
            
            insert_lead(lead_data)
            imported_count += 1
            
        return True, {"imported": imported_count, "skipped": skipped_count}
        
    except Exception as e:
        return False, str(e)

# ==========================================
# COMMAND CENTER ORCHESTRATOR
# ==========================================

def run_all_scanners():
    """
    Executes Scanner 1 and Scanner 2 sequentially. Catches and logs all 
    API errors to prevent crashes, returning a comprehensive summary dict.
    """
    summary = {
        "permits_found": 0,
        "closings_found": 0,
        "duplicates_skipped": 0,
        "errors": []
    }
    
    # 1. Run DOB Permit Scanner
    try:
        dob_results = scan_dob_permits()
        summary["permits_found"] = dob_results["found"]
        summary["duplicates_skipped"] += dob_results["skipped"]
    except Exception as e:
        summary["errors"].append(f"DOB Scanner Error: {str(e)}")
        
    # 2. Run ACRIS Closing Scanner
    try:
        acris_results = scan_acris_closings()
        summary["closings_found"] = acris_results["found"]
        summary["duplicates_skipped"] += acris_results["skipped"]
    except Exception as e:
        summary["errors"].append(f"ACRIS Scanner Error: {str(e)}")
        
    return summary