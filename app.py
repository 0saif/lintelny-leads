import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3

from database import init_db, get_connection, get_all_leads, update_lead_status
from scanner import run_all_scanners
from tracker import get_pending_follow_ups, mark_touch_completed
from outreach import generate_outreach, send_email
from scorer import score_all_leads, get_priority_leads
from config import COMPANY_NAME, WEBSITE, PHONE

# ==========================================
# SETUP & INITIALIZATION
# ==========================================

st.set_page_config(page_title=f"{COMPANY_NAME} Command Center", layout="wide", initial_sidebar_state="expanded")

# Initialize database
try:
    init_db()
except Exception as e:
    st.error(f"Database Initialization Error: {e}")

# Initialize session state for scan history
if 'scan_history' not in st.session_state:
    st.session_state['scan_history'] = []

# Fetch data safely
@st.cache_data(ttl=60)
def fetch_leads_df():
    try:
        leads = get_all_leads()
        return pd.DataFrame(leads) if leads else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching leads: {e}")
        return pd.DataFrame()

df_leads = fetch_leads_df()

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def snooze_follow_up(follow_up_id):
    """Snoozes a follow-up task by 1 day."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        cursor.execute("UPDATE follow_ups SET scheduled_date = ? WHERE id = ?", (tomorrow, follow_up_id))
        conn.commit()
        conn.close()
        st.toast("Task snoozed for 1 day!", icon="💤")
    except Exception as e:
        st.error(f"Error snoozing task: {e}")

# ==========================================
# SIDEBAR
# ==========================================

with st.sidebar:
    st.markdown(f"<h2 style='font-family: monospace; color: #E85D2F;'>{COMPANY_NAME.upper()}</h2>", unsafe_allow_html=True)
    st.caption("Lead Command Center")
    st.divider()
    
    st.subheader("Quick Stats")
    if not df_leads.empty:
        # Calculate leads this week
        one_week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        leads_this_week = len(df_leads[df_leads['created_at'] >= one_week_ago])
        
        # Calculate response rate (Contacted or further / Total)
        responded = len(df_leads[df_leads['status'].isin(['consultation', 'estimate', 'signed'])])
        contacted = len(df_leads[df_leads['status'] != 'new'])
        response_rate = (responded / contacted * 100) if contacted > 0 else 0
        
        # Estimate pipeline value (Mock average of $25k per active non-lost lead)
        active_leads = len(df_leads[~df_leads['status'].isin(['lost', 'signed'])])
        pipeline_value = active_leads * 25000
        
        st.metric("Leads This Week", leads_this_week)
        st.metric("Positive Response Rate", f"{response_rate:.1f}%")
        st.metric("Est. Pipeline Value", f"${pipeline_value:,.0f}")
        
        st.divider()
        csv = df_leads.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Export All Leads CSV",
            data=csv,
            file_name=f"{COMPANY_NAME.replace(' ', '_')}_leads_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    else:
        st.info("No data available yet.")

# ==========================================
# HEADER & METRICS
# ==========================================

st.markdown(f"<h1 style='font-family: monospace;'>{COMPANY_NAME.upper()} — Lead Command Center</h1>", unsafe_allow_html=True)
st.markdown("<hr style='border: 1px solid #E85D2F; margin-top: 0px;'>", unsafe_allow_html=True)

if not df_leads.empty:
    today_str = datetime.now().strftime('%Y-%m-%d')
    new_today = len(df_leads[df_leads['created_at'].str.startswith(today_str, na=False)])
    consultations = len(df_leads[df_leads['status'] == 'consultation'])
    signed = len(df_leads[df_leads['status'] == 'signed'])
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Leads", len(df_leads))
    m2.metric("New Today", new_today)
    m3.metric("Consultations Booked", consultations)
    m4.metric("Signed Contracts", signed)
else:
    st.info("Your database is currently empty. Head to the Scanner tab to find leads.")

st.write("") # Spacer

# ==========================================
# TABS SETUP
# ==========================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🎯 Priority Leads", 
    "📡 Scanner", 
    "📅 Follow-Ups Due Today", 
    "✉️ Outreach", 
    "📊 Analytics"
])

# ==========================================
# TAB 1: PRIORITY LEADS
# ==========================================
with tab1:
    st.subheader("Top Priority Leads")
    
    # Refresh scores button
    if st.button("🔄 Recalculate All Scores"):
        with st.spinner("Scoring..."):
            updated = score_all_leads()
            fetch_leads_df.clear()
            st.success(f"Recalculated scores for {updated} leads!")
            st.rerun()

    if not df_leads.empty:
        # Filters
        f1, f2, f3 = st.columns(3)
        boroughs = ["All"] + list(df_leads['borough_county'].dropna().unique())
        signals = ["All"] + list(df_leads['signal_type'].dropna().unique())
        statuses = ["All"] + list(df_leads['status'].dropna().unique())
        
        sel_boro = f1.selectbox("Filter Borough", boroughs)
        sel_sig = f2.selectbox("Filter Signal", signals)
        sel_stat = f3.selectbox("Filter Status", statuses, index=statuses.index("new") if "new" in statuses else 0)
        
        # Apply Filters
        filtered_df = df_leads.copy()
        if sel_boro != "All": filtered_df = filtered_df[filtered_df['borough_county'] == sel_boro]
        if sel_sig != "All": filtered_df = filtered_df[filtered_df['signal_type'] == sel_sig]
        if sel_stat != "All": filtered_df = filtered_df[filtered_df['status'] == sel_stat]
        
        # Sort and limit
        filtered_df = filtered_df.sort_values(by='score', ascending=False).head(20)
        
        st.divider()
        
        # Render Rows
        if filtered_df.empty:
            st.info("No leads match your filters.")
        else:
            # Header Row
            h1, h2, h3, h4, h5 = st.columns([1, 3, 2, 2, 4])
            h1.markdown("**Score**")
            h2.markdown("**Lead / Address**")
            h3.markdown("**Borough**")
            h4.markdown("**Status**")
            h5.markdown("**Quick Actions**")
            st.divider()
            
            for _, row in filtered_df.iterrows():
                c1, c2, c3, c4, c5 = st.columns([1, 3, 2, 2, 4])
                
                # Score with color coding
                score = int(row['score'])
                if score >= 80:
                    c1.success(f"**{score}**")
                elif score >= 60:
                    c1.warning(f"**{score}**")
                else:
                    c1.info(f"**{score}**")
                
                # Details
                c2.markdown(f"**{row['name']}**<br><small>{row['address']}</small>", unsafe_allow_html=True)
                c3.write(row['borough_county'])
                c4.write(str(row['status']).upper())
                
                # Action Buttons
                with c5:
                    btn_col1, btn_col2, btn_col3 = st.columns(3)
                    if btn_col1.button("📞 Contacted", key=f"c_{row['id']}", use_container_width=True):
                        update_lead_status(row['id'], "contacted")
                        fetch_leads_df.clear()
                        st.rerun()
                    if btn_col2.button("📅 Book", key=f"b_{row['id']}", use_container_width=True):
                        update_lead_status(row['id'], "consultation")
                        fetch_leads_df.clear()
                        st.rerun()
                    if btn_col3.button("❌ Lost", key=f"l_{row['id']}", use_container_width=True):
                        update_lead_status(row['id'], "lost")
                        fetch_leads_df.clear()
                        st.rerun()
                st.markdown("<hr style='margin: 0px; padding: 5px; opacity: 0.2;'>", unsafe_allow_html=True)

# ==========================================
# TAB 2: SCANNER
# ==========================================
with tab2:
    st.subheader("Data Intake Scanners")
    st.write("Fetch the latest high-value construction permits and property transfers from NYC Open Data.")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("🚀 Run Scanners Now", type="primary", use_container_width=True):
            with st.spinner("Connecting to NYC Open Data APIs (Enforcing Rate Limits)..."):
                try:
                    summary = run_all_scanners()
                    score_all_leads() # Auto-score new leads
                    
                    # Save to history
                    st.session_state['scan_history'].insert(0, {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "permits": summary.get('permits_found', 0),
                        "closings": summary.get('closings_found', 0),
                        "skipped": summary.get('duplicates_skipped', 0),
                        "status": "Success" if not summary.get('errors') else "Partial Errors"
                    })
                    
                    fetch_leads_df.clear()
                    st.success("Scan Complete!")
                except Exception as e:
                    st.error(f"Critical Scanner Failure: {e}")
                    
        if st.session_state['scan_history']:
            st.caption(f"Last scanned: {st.session_state['scan_history'][0]['timestamp']}")
            
    with col2:
        st.markdown("**Scan History (Last 10 Runs)**")
        if st.session_state['scan_history']:
            history_df = pd.DataFrame(st.session_state['scan_history'][:10])
            st.dataframe(history_df, use_container_width=True, hide_index=True)
        else:
            st.info("No scans executed in this session yet.")

# ==========================================
# TAB 3: FOLLOW-UPS DUE TODAY
# ==========================================
with tab3:
    st.subheader("Action Items: Due Today")
    
    try:
        all_pending = get_pending_follow_ups()
        today = datetime.now().strftime('%Y-%m-%d')
        # Filter for tasks scheduled for today or earlier
        due_tasks = [t for t in all_pending if t.get('scheduled_date', '') <= today]
        
        if not due_tasks:
            st.success("🎉 You're all caught up! No follow-ups due today.")
        else:
            for task in due_tasks:
                with st.container():
                    t1, t2, t3, t4 = st.columns([1, 3, 3, 2])
                    
                    # Icons based on channel
                    chan = str(task.get('channel', '')).lower()
                    icon = "📧" if chan == "email" else "📞" if chan == "phone" else "📱" if chan == "text" else "👋"
                    
                    t1.markdown(f"### {icon} Touch {task.get('touch_number')}")
                    t2.markdown(f"**{task.get('name')}**<br>Lead ID: {task.get('lead_id')}", unsafe_allow_html=True)
                    t3.markdown(f"**Channel:** {chan.title()}<br>**Due:** {task.get('scheduled_date')}", unsafe_allow_html=True)
                    
                    with t4:
                        if st.button("✅ Mark Done", key=f"done_{task['id']}", use_container_width=True):
                            mark_touch_completed(task['id'], notes="Completed from dashboard")
                            st.rerun()
                        if st.button("💤 Snooze 1 Day", key=f"snooze_{task['id']}", use_container_width=True):
                            snooze_follow_up(task['id'])
                            st.rerun()
                st.divider()
    except Exception as e:
        st.error(f"Error loading follow-ups: {e}")

# ==========================================
# TAB 4: OUTREACH
# ==========================================
with tab4:
    st.subheader("Outreach Engine")
    
    if df_leads.empty:
        st.warning("No leads available to generate outreach.")
    else:
        # Lead Selector (Ensure we pull all necessary columns for the AI prompt)
        lead_options = df_leads[['id', 'name', 'address', 'property_type', 'signal_type', 'source', 'borough_county']].to_dict('records')
        lead_dict = {l['id']: f"{l['name']} - {l['address']}" for l in lead_options}
        
        selected_id = st.selectbox("Select Target Lead", options=list(lead_dict.keys()), format_func=lambda x: lead_dict[x])
        target_lead = next((l for l in lead_options if l['id'] == selected_id), None)
        
        if target_lead:
            if st.button("Generate AI Outreach Suite", type="primary", use_container_width=True):
                with st.spinner("Generating personalized email, text, and door hanger via OpenRouter..."):
                    outreach_data = generate_outreach(target_lead)
                    st.session_state['outreach_data'] = outreach_data
            
            # Display generated content if it exists in session state
            if 'outreach_data' in st.session_state:
                data = st.session_state['outreach_data']
                o_col1, o_col2 = st.columns(2)
                
                # --- EMAIL ---
                with o_col1:
                    st.markdown("### 📧 Email Template")
                    email_subject = st.text_input("Subject", value=data.get('email', {}).get('subject', ''))
                    email_body = st.text_area("Email Body", value=data.get('email', {}).get('body', ''), height=250)
                    target_email = st.text_input("Recipient Email Address")
                    
                    if st.button("🚀 Send via SendGrid", type="primary"):
                        if target_email and email_body:
                            with st.spinner("Dispatching..."):
                                success = send_email(target_email, email_subject, email_body, lead_id=target_lead['id'])
                                if success: 
                                    st.success("Email Sent & Logged Successfully!")
                                else: 
                                    st.error("SendGrid Error. Check terminal logs for details.")
                        else:
                            st.warning("Please provide an email address and body.")

                # --- TEXT & DOOR HANGER ---
                with o_col2:
                    st.markdown("### 📱 SMS Text Template")
                    st.code(data.get('text_message', ''), language="text")
                    st.button("📋 Copy SMS to Clipboard", on_click=lambda: st.toast("Highlight the block above and press Ctrl+C to copy!"))
                    
                    st.markdown("### 🚪 Door Hanger Copy")
                    st.text_area("Print Layout", value=data.get('door_hanger_copy', ''), height=150, disabled=True)

# ==========================================
# TAB 5: ANALYTICS
# ==========================================
with tab5:
    st.subheader("Performance Analytics")
    
    if df_leads.empty:
        st.info("Not enough data to generate analytics.")
    else:
        chart_col1, chart_col2 = st.columns(2)
        
        # 1. Bar Chart: Leads by Source
        with chart_col1:
            source_counts = df_leads['source'].value_counts().reset_index()
            source_counts.columns = ['Source', 'Count']
            fig_bar = px.bar(
                source_counts, x='Source', y='Count', 
                title="Leads Generated by Source",
                color_discrete_sequence=["#E85D2F"]
            )
            fig_bar.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_bar, use_container_width=True)
            
        # 2. Funnel Chart: Pipeline Conversions
        with chart_col2:
            stages = ['new', 'contacted', 'consultation', 'estimate', 'signed']
            counts = [len(df_leads[df_leads['status'] == s]) for s in stages]
            
            # Cumulative funnel math (New includes everything further down)
            funnel_vals = [
                sum(counts[0:5]), # New (All leads)
                sum(counts[1:5]), # Contacted or better
                sum(counts[2:5]), # Consultation or better
                sum(counts[3:5]), # Estimate or better
                counts[4]         # Signed
            ]
            
            fig_funnel = go.Figure(go.Funnel(
                y=[s.title() for s in stages],
                x=funnel_vals,
                marker={"color": ["#171717", "#5E5E5E", "#A4A4A4", "#E85D2F", "#D04A20"]}
            ))
            fig_funnel.update_layout(title="Sales Pipeline Funnel", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_funnel, use_container_width=True)

        st.divider()

        # 3. Line Chart: Leads per Week
        st.markdown("### Lead Velocity (Last 30 Days)")
        try:
            # Convert created_at to datetime and extract date
            df_leads['date'] = pd.to_datetime(df_leads['created_at'], errors='coerce').dt.date
            recent_leads = df_leads[df_leads['date'] >= (datetime.now().date() - timedelta(days=30))]
            
            daily_counts = recent_leads.groupby('date').size().reset_index(name='count')
            
            if not daily_counts.empty:
                fig_line = px.line(
                    daily_counts, x='date', y='count', 
                    markers=True, 
                    line_shape="spline",
                    color_discrete_sequence=["#E85D2F"]
                )
                fig_line.update_layout(
                    xaxis_title="Date", yaxis_title="New Leads",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("No leads recorded in the last 30 days to plot velocity.")
        except Exception as e:
            st.error(f"Error rendering velocity chart: {e}")