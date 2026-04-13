import os
import json
import time
import requests
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

from config import COMPANY_NAME, PHONE, WEBSITE, HIC_LICENSE, SERVICES, EMAIL
from database import get_connection, get_lead_by_id

load_dotenv()

# ==========================================
# HELPER: FALLBACK TEMPLATES
# ==========================================

def _generate_fallback_outreach(lead: dict) -> dict:
    """
    Provides standard template-based outreach if the AI API fails or is unavailable.
    """
    address = lead.get('address', 'your property')
    signal_type = str(lead.get('signal_type', '')).replace('_', ' ')
    
    # Customize opening based on signal
    if 'purchase' in signal_type.lower() or 'closing' in str(lead.get('source', '')).lower():
        opening = f"Congratulations on your recent closing at {address}."
    else:
        opening = f"I noticed a recent {signal_type} permit filed near {address}."

    email_subject = f"Renovation Consultation for {address}"
    email_body = (
        f"{opening} I am reaching out from {COMPANY_NAME}, a local contractor specializing in "
        f"{lead.get('property_type', 'residential')} renovations.\n\n"
        f"We offer a free, no-obligation consultation to review your project scope and feasibility. "
        f"You can also estimate your project costs using our tool at {WEBSITE}/cost-calculator.html.\n\n"
        f"Please let me know if you would like to schedule a walk-through.\n\n"
        f"Regards,\n{COMPANY_NAME} Team\nPhone: {PHONE}\nHIC License: {HIC_LICENSE} | EPA Certified"
    )
    
    text_message = f"Hi, it's {COMPANY_NAME}. Saw your project at {address}. Need a bid? Try our calculator: {WEBSITE}/cost-calculator.html. {PHONE} HIC:{HIC_LICENSE} EPA Cert"
    
    door_hanger_copy = (
        f"PROJECT NOTICE\n\nTo the owner of {address}:\n\n"
        f"{opening} {COMPANY_NAME} is currently operating in your area. "
        f"We specialize in {', '.join(SERVICES[:3])}.\n\n"
        f"Contact us for a free site consultation: {PHONE}\n"
        f"Estimate costs: {WEBSITE}/cost-calculator.html\n\n"
        f"HIC License: {HIC_LICENSE} | EPA Certified"
    )

    return {
        "email": {
            "subject": email_subject[:60], # Enforce length limit
            "body": email_body
        },
        "text_message": text_message[:160], # Enforce length limit
        "door_hanger_copy": door_hanger_copy
    }

# ==========================================
# FUNCTION 1: AI OUTREACH GENERATOR (OPENROUTER)
# ==========================================

def generate_outreach(lead: dict) -> dict:
    """
    Generates personalized, constraint-compliant outreach copy using OpenRouter API.
    Returns a dictionary containing email, text message, and door hanger copy.
    """
    api_key = os.getenv("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")
    if not api_key:
        print("OPENROUTER_API_KEY missing. Falling back to template.")
        return _generate_fallback_outreach(lead)

    address = lead.get('address', 'Unknown Address')
    borough = lead.get('borough_county', 'NYC')
    prop_type = lead.get('property_type', 'Residential')
    signal = str(lead.get('signal_type', '')).replace('_', ' ')
    source = lead.get('source', '')

    system_prompt = (
        "You are a highly professional, knowledgeable, and direct general contractor representing "
        f"{COMPANY_NAME}. Your task is to write outreach copy for a prospective client. You are NOT a pushy salesperson."
    )

    user_prompt = f"""
    Generate outreach copy for a prospect based on this data:
    - Address: {address}
    - Borough: {borough}
    - Property Type: {prop_type}
    - Signal: {signal} (Source: {source})

    Strict Rules for Copy:
    1. Tone must be professional, direct, and knowledgeable.
    2. Reference the specific signal. If source is 'closing' or signal is 'purchase', say something like "Congratulations on your new home". If it's a permit, say something like "I noticed a renovation permit was recently filed near your building".
    3. Offer a free consultation. DO NOT make a hard sales pitch.
    4. You MUST include this exact link: {WEBSITE}/cost-calculator.html
    5. NEVER use more than ONE exclamation mark across all generated copy.
    6. NEVER say "we're the best" or make superlative claims.
    7. ALL messages (email, text, door hanger) MUST include our HIC License ({HIC_LICENSE}) and state that we are "EPA Certified".
    8. ALL messages MUST include our phone number: {PHONE}.
    9. Email subject MUST be under 60 characters.
    10. Email body MUST be under 150 words.
    11. Text message MUST be strictly under 160 characters.

    Output FORMAT: 
    Return strictly valid JSON with the exact following keys. Do not include markdown formatting like ```json.
    {{
        "email_subject": "string",
        "email_body": "string",
        "text_message": "string",
        "door_hanger_copy": "string"
    }}
    """

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": f"https://{WEBSITE}", # OpenRouter ranking requirement
        "X-Title": COMPANY_NAME              # OpenRouter ranking requirement
    }

    payload = {
        "model": "anthropic/claude-3-haiku", # Routing to Claude 3 Haiku via OpenRouter
        "temperature": 0.4,
        "max_tokens": 800,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    }

    try:
        response = requests.post(
            "[https://openrouter.ai/api/v1/chat/completions](https://openrouter.ai/api/v1/chat/completions)",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status() # Raise exception for bad status codes
        
        # Parse the JSON response
        response_data = response.json()
        response_text = response_data['choices'][0]['message']['content'].strip()
        
        # Clean up in case the model included markdown tags despite instructions
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "", 1)
        if response_text.endswith("```"):
            response_text = response_text[::-1].replace("```", "", 1)[::-1]
            
        data = json.loads(response_text.strip())
        
        return {
            "email": {
                "subject": data.get("email_subject", "")[:60],
                "body": data.get("email_body", "")
            },
            "text_message": data.get("text_message", "")[:160],
            "door_hanger_copy": data.get("door_hanger_copy", "")
        }
        
    except Exception as e:
        print(f"OpenRouter API Error: {e}. Falling back to template.")
        return _generate_fallback_outreach(lead)

# ==========================================
# FUNCTION 2: SEND EMAIL VIA SENDGRID
# ==========================================

def send_email(to_email: str, subject: str, body: str, lead_id: int = None) -> bool:
    """
    Sends an email using SendGrid and logs the event to the database if a lead_id is provided.
    """
    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key:
        print("SENDGRID_API_KEY is missing. Operating in simulation mode.")
        return False

    message = Mail(
        from_email=EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=body.replace('\n', '<br>')
    )
    
    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        success = response.status_code in (200, 201, 202)
        
        # Log to the database
        if success and lead_id is not None:
            conn = get_connection()
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                INSERT INTO outreach (lead_id, channel, subject, body, sent_at)
                VALUES (?, ?, ?, ?, ?)
            """, (lead_id, 'email', subject, body, now))
            
            conn.commit()
            conn.close()
            
        return success
        
    except Exception as e:
        print(f"SendGrid API Error: {str(e)}")
        return False

# ==========================================
# FUNCTION 3: BATCH GENERATION
# ==========================================

def generate_batch_outreach(lead_ids: list) -> list:
    """
    Takes a list of lead IDs, retrieves them from the database, and generates 
    outreach for each with a 2-second rate-limiting delay between API calls.
    """
    results = []
    
    for count, lead_id in enumerate(lead_ids):
        lead = get_lead_by_id(lead_id)
        if not lead:
            results.append({"lead_id": lead_id, "error": "Lead not found in database"})
            continue
            
        try:
            outreach_data = generate_outreach(lead)
            results.append({
                "lead_id": lead_id,
                "outreach": outreach_data
            })
        except Exception as e:
            results.append({"lead_id": lead_id, "error": str(e)})
            
        # 2-second delay between API calls to avoid rate limits (unless it's the last item)
        if count < len(lead_ids) - 1:
            time.sleep(2)
            
    return results