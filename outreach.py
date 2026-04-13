import os
import re
import json
import time
import requests
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

from config import COMPANY_NAME, PHONE, WEBSITE, HIC_LICENSE, SERVICES, EMAIL
from database import get_lead_by_id

load_dotenv()

SENDER_NAME = "Saif Anwar"
SENDER_TITLE = "Project Coordinator"


def _build_email_html(body: str) -> str:
    """Wraps a plain-text email body in a branded HTML template."""
    # Style the calculator link as Signal Orange CTA
    styled_body = re.sub(
        r'(https?://)?lintelny\.com/cost-calculator\.html',
        '<a href="https://lintelny.com/cost-calculator.html" '
        'style="color:#E85D2F;font-weight:600;text-decoration:none;">'
        'lintelny.com/cost-calculator.html</a>',
        body
    )

    paragraphs = [p.strip() for p in styled_body.split('\n\n') if p.strip()]
    body_html = ''.join(
        f'<p style="margin:0 0 16px 0;line-height:1.75;color:#171717;font-size:15px;">'
        f'{p.replace(chr(10), "<br>")}</p>'
        for p in paragraphs
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background:#F4F3EE;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#F4F3EE;padding:40px 16px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0"
             style="max-width:600px;width:100%;background:#ffffff;
                    border:1px solid #D5D3CD;border-radius:4px;overflow:hidden;">

        <!-- HEADER -->
        <tr>
          <td style="background:#171717;padding:20px 32px;border-bottom:3px solid #E85D2F;">
            <span style="font-size:20px;font-weight:700;color:#F4F3EE;letter-spacing:3px;">LINTEL NY</span>
            <span style="font-size:11px;color:#9A9690;margin-left:14px;letter-spacing:1px;text-transform:uppercase;">Licensed General Contractors</span>
          </td>
        </tr>

        <!-- BODY -->
        <tr>
          <td style="padding:36px 32px 8px 32px;">
            {body_html}
          </td>
        </tr>

        <!-- SIGNATURE -->
        <tr>
          <td style="padding:0 32px 32px 32px;">
            <table cellpadding="0" cellspacing="0"
                   style="border-top:1px solid #D5D3CD;padding-top:20px;margin-top:8px;width:100%;">
              <tr>
                <td>
                  <p style="margin:0;font-size:15px;font-weight:700;color:#171717;">{SENDER_NAME}</p>
                  <p style="margin:4px 0 0 0;font-size:13px;color:#9A9690;">{SENDER_TITLE} &nbsp;&middot;&nbsp; {COMPANY_NAME}</p>
                  <p style="margin:10px 0 0 0;font-size:13px;color:#171717;">{PHONE}</p>
                  <p style="margin:4px 0 0 0;font-size:12px;color:#9A9690;">
                    HIC License: {HIC_LICENSE}&nbsp;&nbsp;&bull;&nbsp;&nbsp;EPA Certified
                  </p>
                </td>
              </tr>
            </table>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

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

    email_subject = f"Free Consultation — {address}"
    email_body = (
        f"{opening}\n\n"
        f"My name is Saif Anwar, and I'm a Project Coordinator at {COMPANY_NAME}, a licensed general contracting firm "
        f"specializing in {lead.get('property_type', 'residential')} renovations across New York City.\n\n"
        f"We offer a free, no-obligation site consultation to walk through your project scope and provide an honest "
        f"assessment. If you'd like a rough cost estimate before we connect, our online calculator is available at "
        f"{WEBSITE}/cost-calculator.html.\n\n"
        f"Feel free to reply here or give us a call at {PHONE} — no pressure, just a conversation."
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
# FUNCTION 1: AI OUTREACH GENERATOR (OPENAI)
# ==========================================

def generate_outreach(lead: dict) -> dict:
    """
    Generates personalized, constraint-compliant outreach copy using OpenAI API.
    Returns a dictionary containing email, text message, and door hanger copy.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY missing. Falling back to template.")
        return _generate_fallback_outreach(lead)

    address = lead.get('address', 'Unknown Address')
    borough = lead.get('borough_county', 'NYC')
    prop_type = lead.get('property_type', 'Residential')
    signal = str(lead.get('signal_type', '')).replace('_', ' ')
    source = lead.get('source', '')

    system_prompt = (
        f"You are a senior business development representative for {COMPANY_NAME}, a licensed general contracting firm in New York City. "
        "You write concise, professional outreach emails that read like they came from a real person — not a marketing blast. "
        "Your tone is warm but businesslike: confident, specific, and never pushy. You never use filler phrases like 'I hope this message finds you well' or hollow compliments."
    )

    user_prompt = f"""
    Write outreach copy for the following prospect:

    - Address: {address}
    - Borough: {borough}
    - Property Type: {prop_type}
    - Signal: {signal} (Source: {source})

    EMAIL STRUCTURE (follow this exactly):
    1. Opening line: One sentence acknowledging the specific signal. If signal is 'purchase' or source contains 'closing', write "Congratulations on your recent acquisition at [address]." For permits, write "I came across a recent permit filing at [address] and wanted to reach out."
    2. Introduction: One sentence identifying who we are and our relevant expertise (do NOT use "we are the best" or superlatives).
    3. Value paragraph: 2–3 sentences. Mention that we offer a free, no-obligation site consultation. Reference the cost estimator tool at {WEBSITE}/cost-calculator.html naturally within the sentence.
    4. Closing line: A single, low-pressure call to action — invite them to reply or call us at {PHONE} if they'd like to connect.
    NOTE: Do NOT include a signature block. It will be appended automatically.

    RULES:
    - Tone: professional, direct, specific to the signal. No generic filler.
    - NEVER use more than one exclamation mark across all copy combined.
    - NEVER make superlative claims ("best", "top-rated", "industry-leading").
    - Email subject: under 60 characters, specific to the address or signal.
    - Email body: 120–180 words. Well-structured paragraphs, not a wall of text.
    - Text message: strictly under 160 characters. Include {PHONE} and HIC:{HIC_LICENSE}.
    - Door hanger: clear header, 3–4 short lines, include {PHONE}, HIC license, EPA Certified.

    Return strictly valid JSON — no markdown, no code fences:
    {{
        "email_subject": "string",
        "email_body": "string",
        "text_message": "string",
        "door_hanger_copy": "string"
    }}
    """

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "o3",
        "max_completion_tokens": 1000,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status() # Raise exception for bad status codes
        
        # Parse the JSON response
        response_data = response.json()
        response_text = response_data['choices'][0]['message']['content'].strip()
        
        # Clean up in case the model included markdown tags despite instructions
        response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
        response_text = re.sub(r'\n?```\s*$', '', response_text)

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
        print(f"OpenAI API Error: {e}. Falling back to template.")
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
        from_email=(EMAIL, f"{SENDER_NAME} at {COMPANY_NAME}"),
        to_emails=to_email,
        subject=subject,
        html_content=_build_email_html(body)
    )
    
    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        success = response.status_code in (200, 201, 202)
        
        # Log to the database
        if success and lead_id is not None:
            from database import get_client
            get_client().table('outreach').insert({
                'lead_id': lead_id,
                'channel': 'email',
                'subject': subject,
                'body':    body,
                'sent_at': datetime.now().isoformat(),
            }).execute()
            
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