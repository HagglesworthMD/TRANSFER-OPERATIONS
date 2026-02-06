import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import re
from pathlib import Path
import time
import json
import sys
import site
import importlib.util
AUTOR_IMPORT_ERROR = None
try:
    from streamlit_autorefresh import st_autorefresh
    AUTOR_AVAILABLE = True
except Exception as e:
    AUTOR_AVAILABLE = False
    AUTOR_IMPORT_ERROR = str(e)

# ==================== BUILD STAMP ====================
SCRIPT_PATH = Path(__file__).resolve()
try:
    DASHBOARD_BUILD_ID = str(SCRIPT_PATH.stat().st_mtime_ns)
except Exception:
    DASHBOARD_BUILD_ID = "unknown"

import dashboard_core

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="SAMI Transfer Operations Center",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==================== AUTO-REFRESH STATE ====================
# Auto-refresh state (controls rendered in main content after header)
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True
if 'refresh_interval' not in st.session_state:
    st.session_state.refresh_interval = 30

def log_refresh(message):
    print(f"[DASH_REFRESH] {message}")


# ==================== THEME TOGGLE ====================
if 'theme' not in st.session_state:
    st.session_state.theme = 'dark'  # Default to dark mode

# ==================== CUSTOM CSS ====================
# Theme colors
if st.session_state.theme == 'dark':
    bg_gradient = "linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%)"
    text_color = "#ffffff"
    card_bg = "rgba(255, 255, 255, 0.05)"
    border_color = "rgba(255, 255, 255, 0.1)"
    hover_bg = "rgba(255, 255, 255, 0.08)"
    chart_text_color = "white"
    chart_grid_color = "rgba(255,255,255,0.1)"
else:  # light mode
    bg_gradient = "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)"
    text_color = "#1a202c"
    card_bg = "rgba(255, 255, 255, 0.9)"
    border_color = "rgba(0, 0, 0, 0.1)"
    hover_bg = "rgba(0, 0, 0, 0.05)"
    chart_text_color = "#1a202c"
    chart_grid_color = "rgba(0,0,0,0.1)"

st.markdown(f"""
<style>
    /* ========== GLOBAL TEXT FIXES ========== */
    /* Force all text to be readable */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {{
        color: {text_color} !important;
    }}

    /* Checkbox labels - critical fix */
    .stCheckbox label, .stCheckbox span, .stCheckbox p,
    [data-testid="stCheckbox"] label, [data-testid="stCheckbox"] span,
    [data-baseweb="checkbox"] + div {{
        color: {text_color} !important;
        font-weight: 500 !important;
    }}

    /* All form labels */
    label, .stTextInput label, .stNumberInput label, .stDateInput label {{
        color: {text_color} !important;
        font-weight: 500 !important;
    }}

    /* Paragraphs and captions */
    p, .stCaption, [data-testid="stCaptionContainer"] p {{
        color: {'#cbd5e1' if st.session_state.theme == 'dark' else '#64748b'} !important;
    }}

    div[data-testid="stTextInput"] {{
        margin-bottom: 1.1rem;
    }}

    /* Global Styles */
    * {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}

    /* Main Background */
    .stApp {{
        background: {bg_gradient};
    }}

    /* Header Styling */
    h1, h2, h3 {{
        color: {text_color} !important;
        font-weight: 700 !important;
        letter-spacing: -0.5px;
    }}
    
    /* Fix button visibility */
    .stButton > button {{
        background: {'rgba(102, 126, 234, 0.2)' if st.session_state.theme == 'dark' else 'rgba(102, 126, 234, 0.8)'} !important;
        color: {text_color} !important;
        border: 1px solid {'rgba(255, 255, 255, 0.2)' if st.session_state.theme == 'dark' else 'rgba(102, 126, 234, 0.3)'} !important;
        border-radius: 8px !important;
        padding: 0.5rem 1rem !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
    }}
    
    .stButton > button:hover {{
        background: {'rgba(102, 126, 234, 0.4)' if st.session_state.theme == 'dark' else 'rgba(102, 126, 234, 1.0)'} !important;
        border-color: {'rgba(255, 255, 255, 0.4)' if st.session_state.theme == 'dark' else 'rgba(102, 126, 234, 0.8)'} !important;
        transform: translateY(-2px);
    }}
    
    /* Download button fix */
    .stDownloadButton > button {{
        background: {'rgba(16, 185, 129, 0.2)' if st.session_state.theme == 'dark' else 'rgba(16, 185, 129, 0.8)'} !important;
        color: {text_color} !important;
        border: 1px solid {'rgba(16, 185, 129, 0.3)' if st.session_state.theme == 'dark' else 'rgba(16, 185, 129, 0.5)'} !important;
        border-radius: 8px !important;
        padding: 0.5rem 1rem !important;
        font-weight: 600 !important;
    }}
    
    .stDownloadButton > button:hover {{
        background: {'rgba(16, 185, 129, 0.4)' if st.session_state.theme == 'dark' else 'rgba(16, 185, 129, 1.0)'} !important;
    }}
    
    /* Metric Cards */
    [data-testid="stMetricValue"] {{
        font-size: 2.2rem !important;
        font-weight: 700 !important;
        color: {'#ffffff' if st.session_state.theme == 'dark' else '#1e293b'} !important;
    }}

    [data-testid="stMetricLabel"] {{
        font-size: 0.75rem !important;
        color: {'#e2e8f0' if st.session_state.theme == 'dark' else '#475569'} !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 600 !important;
    }}

    /* Caption text - ensure readable */
    .stCaption, [data-testid="stCaptionContainer"] {{
        color: {'#cbd5e1' if st.session_state.theme == 'dark' else '#64748b'} !important;
    }}

    /* Expander headers - force visibility */
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary span,
    [data-testid="stExpander"] summary p,
    .streamlit-expanderHeader,
    details summary {{
        color: {text_color} !important;
        font-weight: 600 !important;
        background: transparent !important;
    }}

    [data-testid="stExpander"] summary p {{
        color: {text_color} !important;
    }}

    /* DataFrame/Table styling */
    [data-testid="stDataFrame"] th {{
        background: {'rgba(99, 102, 241, 0.2)' if st.session_state.theme == 'dark' else 'rgba(99, 102, 241, 0.1)'} !important;
        color: {'#f1f5f9' if st.session_state.theme == 'dark' else '#1e293b'} !important;
        font-weight: 600 !important;
    }}

    [data-testid="stDataFrame"] td {{
        color: {'#e2e8f0' if st.session_state.theme == 'dark' else '#374151'} !important;
    }}

    /* DataFrame export menu visibility */
    [data-testid="stDataFrame"] [data-testid="stToolbar"] button {{
        color: {text_color} !important;
    }}
    [data-testid="stDataFrame"] [data-testid="stToolbar"] svg {{
        fill: {text_color} !important;
    }}
    [data-testid="stElementToolbarButtonIcon"] {{
        fill: {'#111827' if st.session_state.theme == 'dark' else text_color} !important;
        color: {'#111827' if st.session_state.theme == 'dark' else text_color} !important;
    }}
    [data-testid="stElementToolbar"] button {{
        color: {'#111827' if st.session_state.theme == 'dark' else text_color} !important;
    }}
    [data-testid="stElementToolbar"] button svg {{
        fill: {'#111827' if st.session_state.theme == 'dark' else text_color} !important;
    }}
    [data-testid="stElementToolbarButton"] {{
        color: {'#111827' if st.session_state.theme == 'dark' else text_color} !important;
    }}
    [data-baseweb="popover"] {{
        background: {'#111827' if st.session_state.theme == 'dark' else '#ffffff'} !important;
        color: {text_color} !important;
    }}
    [data-baseweb="menu"] {{
        background: {'#111827' if st.session_state.theme == 'dark' else '#ffffff'} !important;
        color: {text_color} !important;
    }}
    [data-baseweb="menu"] * {{
        color: {text_color} !important;
    }}
    [data-baseweb="popover"] [role="dialog"],
    [data-baseweb="popover"] [role="menu"] {{
        background: {'#111827' if st.session_state.theme == 'dark' else '#ffffff'} !important;
        color: {text_color} !important;
        border: 1px solid {border_color} !important;
    }}
    [data-baseweb="popover"] [role="menuitem"] {{
        color: {text_color} !important;
    }}
    [data-baseweb="popover"] input {{
        color: {text_color} !important;
        background: {'#0f172a' if st.session_state.theme == 'dark' else '#ffffff'} !important;
    }}
    [data-baseweb="popover"] svg {{
        fill: {text_color} !important;
    }}

    /* Plotly text/legend readability */
    .stPlotlyChart text {{
        fill: {text_color} !important;
    }}
    .stPlotlyChart .legend text {{
        fill: {text_color} !important;
    }}
    
    /* Glass Cards */
    .glass-card {{
        background: {card_bg};
        backdrop-filter: blur(10px);
        border-radius: 16px;
        border: 1px solid {border_color};
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
    }}
    
    /* Activity Feed */
    .activity-item {{
        background: {card_bg};
        border-left: 3px solid #667eea;
        padding: 0.8rem;
        margin: 0.5rem 0;
        border-radius: 8px;
        transition: all 0.3s ease;
    }}
    
    .activity-item:hover {{
        background: {hover_bg};
        transform: translateX(5px);
    }}
    
    /* Pulse Animation for Live Indicator */
    @keyframes pulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.5; }}
    }}
    
    .live-indicator {{
        display: inline-block;
        width: 10px;
        height: 10px;
        background: #10b981;
        border-radius: 50%;
        margin-right: 8px;
        animation: pulse 2s infinite;
    }}
    
    /* DataFrame Styling */
    [data-testid="stDataFrame"] {{
        background: {card_bg};
        border-radius: 12px;
        padding: 1rem;
    }}
    
    /* Expander styling */
    .streamlit-expanderHeader {{
        background: {card_bg} !important;
        color: {text_color} !important;
        border-radius: 8px !important;
    }}
    
    /* Hide Streamlit Branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    
    /* Stat Number Glow */
    .stat-glow {{
        text-shadow: 0 0 20px rgba(102, 126, 234, 0.5);
    }}
</style>
""", unsafe_allow_html=True)

# ==================== DATA LOADING ====================
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR

LOG_FILE = DATA_ROOT / "daily_stats.csv"
STATE_FILE = DATA_ROOT / "roster_state.json"
STAFF_FILE = DATA_ROOT / "staff.txt"
LEDGER_FILE = DATA_ROOT / "processed_ledger.json"
WATCHDOG_FILE = DATA_ROOT / "urgent_watchdog.json"
OVERRIDES_FILE = DATA_ROOT / "settings_overrides.json"

def get_file_info(path):
    """Return file stats for display."""
    path_str = str(path)
    if not os.path.exists(path_str):
        return {"path": path_str, "exists": False, "size": None, "mtime": None}
    stat = os.stat(path_str)
    return {
        "path": path_str,
        "exists": True,
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    }

def get_last_heartbeat(log_path):
    try:
        mtime = os.path.getmtime(str(log_path))
        return datetime.fromtimestamp(mtime)
    except FileNotFoundError:
        return None
    except Exception:
        return None

def make_arrow_safe_df(df):
    if not hasattr(df, "columns"):
        return df
    safe = df.copy()
    for col in safe.columns:
        if safe[col].dtype == "object":
            safe[col] = safe[col].apply(lambda v: str(v) if isinstance(v, (Path, os.PathLike)) else v)
    return safe

def normalize_sender(raw):
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "Unknown Sender"
    value = str(raw).strip()
    if not value:
        return "Unknown Sender"
    lower = value.lower()
    if "@" in value and not lower.startswith("/o="):
        return value
    if "/cn=" in lower:
        cn = lower.split("/cn=")[-1]
        cn = cn.split("/")[-1]
        if "-" in cn:
            cn = cn.split("-")[-1]
        if any(ch.isalpha() for ch in cn):
            return cn.replace(".", " ").title()
        return "Unknown Sender"
    return value

def write_json_atomic(path, data):
    """Write JSON atomically."""
    try:
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, path)
        return True, None
    except Exception as e:
        return False, str(e)

@st.cache_data(ttl=5)  # Cache for 5 seconds for real-time feel
def load_data(snapshot):
    """Load and process all data sources"""
    log_refresh("load_data cache miss")
    try:
        # Load CSV with flexible date parsing
        df = dashboard_core.load_daily_stats_csv(str(LOG_FILE))
        df_err = df.attrs.get("error")
        if df_err:
            st.warning("Data unavailable")
            st.caption("Check the service is running and data files exist.")
        
        # Normalize date format - handle mixed formats
        def parse_date(date_str):
            """Parse date from multiple formats"""
            if pd.isna(date_str):
                return None
            
            date_str = str(date_str).strip()
            
            # Try YYYY-MM-DD format first
            try:
                return pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%d')
            except:
                pass
            
            # Try DD/MM/YYYY format
            try:
                return pd.to_datetime(date_str, format='%d/%m/%Y').strftime('%Y-%m-%d')
            except:
                pass
            
            # Fallback to pandas auto-parse with dayfirst
            try:
                return pd.to_datetime(date_str, dayfirst=True).strftime('%Y-%m-%d')
            except:
                return None
        
        df['Date'] = df['Date'].apply(parse_date)
        
        # Remove rows where date parsing failed
        df = df[df['Date'].notna()].copy()
        
        # Normalize assignees to lowercase to prevent duplicates
        if 'Assigned To' in df.columns:
            df['Assigned To'] = df['Assigned To'].fillna("").astype(str).str.lower()
        
        # Add datetime column for time-based analysis
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
        
        # Load roster state
        roster_state = snapshot["data"].get("roster_state.json") if snapshot else None
        roster_err = snapshot["errors"].get("roster_state.json") if snapshot else "missing"
        if roster_err:
            st.warning("Data unavailable")
            st.caption("Check the service is running and data files exist.")
        
        # Load staff list and normalize to lowercase
        with open(STAFF_FILE, 'r') as f:
            staff_list = []
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                staff_list.append(stripped.lower())
            seen = set()
            staff_list = [email for email in staff_list if not (email in seen or seen.add(email))]
        
        return df, roster_state, staff_list
    except FileNotFoundError:
        return None, None, None
    except Exception:
        st.warning("Data unavailable")
        st.caption("Check the service is running and data files exist.")
        return None, None, None

# ==================== HEADER ====================
last_heartbeat = get_last_heartbeat(LOG_FILE)
heartbeat_label = (
    f"Last heartbeat: {last_heartbeat.strftime('%d %b %Y, %H:%M:%S')}"
    if last_heartbeat
    else "Last heartbeat: unavailable"
)
col1, col2, col3, col4 = st.columns([3, 1, 0.4, 0.4])
with col1:
    st.markdown("<h1>SAMI Transfer Operations Center</h1>", unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div style='text-align: right; padding-top: 1rem;'>
        <span style='color: #cbd5e1; font-size: 0.75rem;'>{heartbeat_label}</span>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown("<div style='padding-top: 1.5rem;'></div>", unsafe_allow_html=True)
    # Theme toggle button
    theme_icon = "🌙" if st.session_state.theme == 'dark' else "☀️"
    if st.button(theme_icon, help="Toggle Dark/Light Mode", use_container_width=True):
        st.session_state.theme = 'light' if st.session_state.theme == 'dark' else 'dark'
        st.rerun()
with col4:
    st.markdown("<div style='padding-top: 1.5rem;'></div>", unsafe_allow_html=True)
    if st.button("🔄", help="Refresh Now", use_container_width=True):
        st.rerun()

# ==================== AUTO-REFRESH CONTROLS ====================
with st.container():
    ar_col1, ar_col2, ar_col3 = st.columns([1, 1, 4])
    with ar_col1:
        auto_refresh = st.checkbox("Auto-refresh", value=st.session_state.auto_refresh, key="auto_refresh_cb")
        st.session_state.auto_refresh = auto_refresh
    with ar_col2:
        refresh_interval = st.number_input(
            "Interval (sec)",
            min_value=30,
            max_value=300,
            value=st.session_state.refresh_interval,
            step=5,
            key="refresh_interval_input"
        )
        st.session_state.refresh_interval = refresh_interval

if auto_refresh and AUTOR_AVAILABLE:
    try:
        mtime = os.path.getmtime(str(LOG_FILE))
        age_seconds = int((datetime.now() - datetime.fromtimestamp(mtime)).total_seconds())
    except Exception:
        age_seconds = None
    log_refresh(
        f"enabled={auto_refresh} interval_sec={int(refresh_interval)} interval_ms={int(refresh_interval) * 1000} "
        f"log_age_sec={age_seconds}"
    )
    st_autorefresh(interval=int(refresh_interval) * 1000, key="auto_refresh")
else:
    log_refresh(
        f"enabled={auto_refresh} autorefresh_available={AUTOR_AVAILABLE} import_error={AUTOR_IMPORT_ERROR}"
    )
    if auto_refresh:
        spec = importlib.util.find_spec("streamlit_autorefresh")
        log_refresh(
            f"python={sys.executable} user_site={site.getusersitepackages()} spec_found={spec is not None}"
        )

# ==================== LOAD DATA ====================
snapshot = dashboard_core.load_state_snapshot(str(DATA_ROOT))
df, roster_state, staff_list = load_data(snapshot)

# Handle missing data gracefully - don't stop, show what we have
if df is None:
    # Create empty dataframe with expected columns
    df = pd.DataFrame(columns=['Date', 'Time', 'Subject', 'Assigned To', 'Sender', 'Risk Level'])
    st.info("**No data yet.** Start the service to see live metrics.")

if roster_state is None:
    roster_state = {"current_index": 0, "total_processed": 0}

if staff_list is None:
    staff_list = []

# ==================== MANAGER SUMMARY ====================
st.markdown("---")
st.markdown("## Manager Summary")
today = datetime.now().strftime('%Y-%m-%d')
df_today = df[df['Date'] == today].copy() if len(df) > 0 else df.copy()
heartbeat_mask = pd.Series(False, index=df_today.index)
for col in ["Risk Level", "Subject", "Assigned To", "Sender"]:
    if col in df_today.columns:
        heartbeat_mask |= df_today[col].astype(str).str.contains("heartbeat", case=False, na=False)
df_today_kpi = df_today[~heartbeat_mask].copy()
total_today = len(df_today_kpi)
assigned_today = len(df_today_kpi[df_today_kpi['Assigned To'] != 'completed'])
completions_today = dashboard_core.compute_completions_today(df_today_kpi, datetime.now())
configured_staff = len(staff_list)
next_idx = roster_state.get('current_index', 0) % len(staff_list) if staff_list else 0
next_staff = staff_list[next_idx] if staff_list else 'N/A'
last_update = 'N/A'
if 'DateTime' in df.columns and len(df) > 0:
    try:
        last_dt = df['DateTime'].max()
        if pd.notna(last_dt):
            last_update = str(last_dt)
    except Exception:
        pass
status_label = 'OK' if total_today > 0 else 'Attention'
status_color = '#10b981' if total_today > 0 else '#f59e0b'
st.markdown(f"**Status:** :green[{status_label}]" if total_today > 0 else f"**Status:** :orange[{status_label}]")
col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
with col_m1:
    st.metric('Processed today', total_today)
with col_m2:
    st.metric('Assigned today', assigned_today)
with col_m3:
    st.metric('Completions today', completions_today)
with col_m4:
    st.metric('Configured staff', configured_staff)
    if st.button("Edit staff list (staff.txt)", use_container_width=True):
        try:
            if not os.path.exists(str(STAFF_FILE)):
                raise FileNotFoundError(f"Missing file: {STAFF_FILE}")
            if not hasattr(os, "startfile"):
                raise OSError("Opening files is not supported on this platform.")
            os.startfile(str(STAFF_FILE.resolve()))
            st.success("Opened staff.txt in your default editor.")
        except Exception as e:
            st.error(f"Unable to open staff.txt: {e}")
    st.caption("Opens staff.txt. One email per line. Save; service reads on next tick.")
with col_m5:
    st.metric('Next assignment', next_staff.split('@')[0] if next_staff != 'N/A' else 'N/A')
st.caption(f"Last update: {last_update}")
# ==================== OPERATIONS SNAPSHOT ====================
st.markdown("---")
st.markdown("### Operations Snapshot")

with st.expander("Workload Distribution", expanded=False):
    # Date filter controls
    filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])
    with filter_col1:
        # Get available date range from data
        today_date = datetime.now().date()
        if len(df) > 0 and 'Date' in df.columns:
            available_dates = pd.to_datetime(df['Date'].dropna().unique())
            min_date = available_dates.min().date() if len(available_dates) > 0 else today_date
            max_date = available_dates.max().date() if len(available_dates) > 0 else today_date
        else:
            min_date = today_date
            max_date = today_date

        # Ensure today is within range so defaults work even without today's data
        min_date = min(min_date, today_date)
        max_date = max(max_date, today_date)
        default_date = today_date

        filter_start = st.date_input(
            "From",
            value=default_date,
            min_value=min_date,
            max_value=max_date,
            key="filter_start"
        )
    with filter_col2:
        filter_end = st.date_input(
            "To",
            value=default_date,
            min_value=min_date,
            max_value=max_date,
            key="filter_end"
        )
    with filter_col3:
        st.caption("Filter data by date range. Defaults to today.")

    # Apply date filter
    filter_start_str = filter_start.strftime('%Y-%m-%d')
    filter_end_str = filter_end.strftime('%Y-%m-%d')
    if len(df) > 0:
        df_filtered = df[(df['Date'] >= filter_start_str) & (df['Date'] <= filter_end_str)].copy()
    else:
        df_filtered = df.copy()

    heartbeat_mask = pd.Series(False, index=df_filtered.index)
    for col in ["Risk Level", "Subject", "Assigned To", "Sender"]:
        if col in df_filtered.columns:
            heartbeat_mask |= df_filtered[col].astype(str).str.contains("heartbeat", case=False, na=False)
    df_filtered = df_filtered[~heartbeat_mask].copy()

    # Display filter summary
    filter_label = "today" if filter_start_str == filter_end_str == today else f"{filter_start_str} to {filter_end_str}"
    st.caption(f"Showing data for: **{filter_label}** ({len(df_filtered)} records)")

    if len(df_filtered) == 0:
        st.info("No activity for selected period.")
    else:
        # Filter out 'completed' and 'bot' entries
        staff_only = df_filtered[(df_filtered['Assigned To'] != 'completed') & (~df_filtered['Assigned To'].str.lower().str.contains('bot', na=False)) & (~df_filtered['Assigned To'].str.lower().str.contains('system_notification', na=False)) & (~df_filtered['Assigned To'].str.lower().str.contains('hib', na=False)) & (~df_filtered['Assigned To'].str.lower().str.contains('hold', na=False)) & (~df_filtered['Assigned To'].str.lower().str.contains('manager_review', na=False))]
        workload = staff_only['Assigned To'].value_counts()
        if workload.empty:
            st.info("No staff assignments for selected period.")
        else:
            workload_df = workload.reset_index()
            workload_df.columns = ['Assignee', 'Assignments']
            workload_df['Assignee'] = workload_df['Assignee'].apply(lambda x: x.split('@')[0].title() if isinstance(x, str) else x)
            # Professional color palette for different users
            colors = ['#6366f1', '#8b5cf6', '#ec4899', '#14b8a6', '#f59e0b', '#ef4444', '#3b82f6', '#10b981']
            pulls = [0.06 if i == 0 else 0.02 for i in range(len(workload_df))]
            fig = go.Figure(data=[
                go.Pie(
                    labels=workload_df['Assignee'],
                    values=workload_df['Assignments'],
                    hole=0.4,
                    pull=pulls,
                    marker=dict(
                        colors=colors[:len(workload_df)],
                        line=dict(color='rgba(0,0,0,0.25)', width=1)
                    ),
                    textinfo='label+percent',
                    textposition='outside',
                    textfont=dict(size=12, color=chart_text_color),
                    hovertemplate='%{label}<br>Assignments: %{value}<br>(%{percent})<extra></extra>'
                )
            ])
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color=chart_text_color, size=12),
                margin=dict(l=120, r=120, t=50, b=60),
                showlegend=True,
                legend=dict(orientation='h', yanchor='bottom', y=-0.15, xanchor='center', x=0.5),
                height=420
            )
            st.plotly_chart(fig, use_container_width=True)

with st.expander("Hourly Activity Trend", expanded=False):
    if len(df_filtered) == 0 or 'DateTime' not in df_filtered.columns:
        st.info("No activity for selected period.")
    else:
        hourly = df_filtered.copy()
        hourly['Hour'] = hourly['DateTime'].dt.hour
        hourly_counts = hourly.groupby('Hour').size().reset_index(name='Count')
        fig = go.Figure(data=[
            go.Scatter(
                x=hourly_counts['Hour'],
                y=hourly_counts['Count'],
                mode='lines+markers',
                line=dict(color='#6366f1', width=3),
                marker=dict(size=8, color='#6366f1'),
                fill='tozeroy',
                fillcolor='rgba(99, 102, 241, 0.1)'
            )
        ])
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color=chart_text_color),
            xaxis=dict(showgrid=False, title='Hour of Day', dtick=2),
            yaxis=dict(showgrid=True, gridcolor=chart_grid_color, title='Activity Count'),
            margin=dict(l=40, r=40, t=20, b=40),
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)

with st.expander("Recent Activity", expanded=False):
    if len(df_filtered) == 0:
        st.info("No activity for selected period.")
    else:
        recent = df_filtered.sort_values('DateTime', ascending=False).head(10).copy()
        # Format date nicely for display
        recent['When'] = recent['DateTime'].dt.strftime('%d %b %H:%M')
        recent['Assigned To'] = recent['Assigned To'].apply(lambda x: x.split('@')[0].title() if isinstance(x, str) and '@' in x else (x.title() if isinstance(x, str) else x))
        recent['Subject'] = recent['Subject'].apply(lambda x: (str(x)[:60] + '...') if isinstance(x, str) and len(str(x)) > 60 else x)
        st.dataframe(
            make_arrow_safe_df(recent[['When', 'Subject', 'Assigned To']]),
            column_config={
                "When": st.column_config.TextColumn("When", width="small"),
                "Subject": st.column_config.TextColumn("Subject", width="large"),
                "Assigned To": st.column_config.TextColumn("Assigned To", width="medium"),
            },
            hide_index=True,
            use_container_width=True,
            height=300
        )

with st.expander("External Request Sources", expanded=False):
    if 'Sender' not in df_filtered.columns or len(df_filtered) == 0:
        st.info("No sender data for selected period.")
    else:
        sender_df = df_filtered[(df_filtered['Sender'].notna()) & (df_filtered['Sender'] != 'unknown') & (df_filtered['Assigned To'] != 'completed')].copy()
        if sender_df.empty:
            st.info("No sender data for selected period.")
        else:
            def extract_sender_email(value):
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return None
                match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", str(value))
                if not match:
                    return None
                email = match.group(0).lower()
                local = email.split("@", 1)[0]
                if local in {"system", "noreply", "no-reply", "donotreply", "do-not-reply"}:
                    return None
                return email

            def normalize_sender_label(value):
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return None
                normalized = normalize_sender(value)
                label = str(normalized).strip()
                if not label:
                    return None
                lower = label.lower()
                if lower in {"system", "noreply", "no-reply", "donotreply", "do-not-reply"}:
                    return None
                return label

            sender_df['Sender_Label'] = sender_df['Sender'].apply(extract_sender_email)
            sender_df['Sender_Label'] = sender_df['Sender_Label'].fillna(sender_df['Sender'].apply(normalize_sender_label))
            sender_df = sender_df[sender_df['Sender_Label'].notna()]
            if sender_df.empty:
                st.info("No sender data for selected period.")
                top_df = None
            else:
                top_senders = sender_df['Sender_Label'].value_counts().head(10)
                top_df = top_senders.reset_index()
                top_df.columns = ['Sender', 'Requests']
            if top_df is not None:
                st.dataframe(make_arrow_safe_df(top_df), width="stretch", height=250)

                # ---- Per-Staff KPIs ----
                source_df = df_filtered
                has_event_schema = all(col in source_df.columns for col in ["event_type", "msg_key", "assigned_ts", "completed_ts", "duration_sec", "assigned_to"])
                def _staff_key(raw):
                    if raw is None:
                        return "unknown"
                    s = str(raw).strip()
                    if not s:
                        return "unknown"
                    lower = s.lower()
                    if "@" in lower:
                        return lower
                    return "name:" + lower

                def _staff_label(raw):
                    if raw is None:
                        return "Unknown"
                    s = str(raw).strip()
                    if not s:
                        return "Unknown"
                    lower = s.lower()
                    if "@" in lower:
                        local = lower.split("@", 1)[0]
                        return local.replace(".", " ").replace("_", " ").strip().title()
                    return s.title()

                label_map = pd.Series(dtype="object")
                if has_event_schema:
                    events_df, _dropped = dashboard_core.prepare_event_frame(source_df)
                    now_local = datetime.now()
                    active_df, completed_df, _assigned_latest_df = dashboard_core.build_work_queue_views(events_df, now_local)
                    events_df["staff_key"] = events_df["assigned_to"].apply(_staff_key)
                    if not active_df.empty:
                        active_df["staff_key"] = active_df["assigned_to"].apply(_staff_key)
                    else:
                        active_df["staff_key"] = pd.Series(dtype="object")
                    if not completed_df.empty:
                        completed_df["staff_key"] = completed_df["assigned_to"].apply(_staff_key)
                    else:
                        completed_df["staff_key"] = pd.Series(dtype="object")
                    label_map = (
                        events_df[["staff_key", "assigned_to"]]
                        .dropna()
                        .drop_duplicates(subset=["staff_key"])
                        .set_index("staff_key")["assigned_to"]
                        .apply(_staff_label)
                    )
                    assigned_counts = (
                        events_df[events_df["event_type_norm"] == "ASSIGNED"]
                        .groupby("staff_key")
                        .size()
                        .rename("assigned_count")
                        if not events_df.empty
                        else pd.Series(dtype="int64", name="assigned_count")
                    )
                else:
                    tmp = source_df.copy()
                    if "assigned_to" not in tmp.columns and "Assigned To" in tmp.columns:
                        tmp["assigned_to"] = tmp["Assigned To"]
                    if "assigned_to" not in tmp.columns:
                        tmp["assigned_to"] = ""
                    tmp["assigned_to"] = tmp["assigned_to"].fillna("").astype(str).str.lower()
                    tmp = tmp[tmp["assigned_to"] != ""].copy()
                    tmp["staff_key"] = tmp["assigned_to"].apply(_staff_key)
                    label_map = (
                        tmp[["staff_key", "assigned_to"]]
                        .dropna()
                        .drop_duplicates(subset=["staff_key"])
                        .set_index("staff_key")["assigned_to"]
                        .apply(_staff_label)
                    )
                    active_df = tmp[tmp["assigned_to"] != "completed"].copy()
                    if not active_df.empty:
                        active_df["staff_key"] = active_df["assigned_to"].apply(_staff_key)
                    else:
                        active_df["staff_key"] = pd.Series(dtype="object")
                    completed_df = pd.DataFrame(columns=["staff_key", "duration_sec_num"])
                    assigned_counts = (
                        tmp.groupby("staff_key")
                        .size()
                        .rename("assigned_count")
                        if not tmp.empty
                        else pd.Series(dtype="int64", name="assigned_count")
                    )

                completed_counts = (
                    completed_df.groupby("staff_key")
                    .size()
                    .rename("completed_count")
                    if not completed_df.empty
                    else pd.Series(dtype="int64", name="completed_count")
                )
                active_counts = (
                    active_df.groupby("staff_key")
                    .size()
                    .rename("active_count")
                    if not active_df.empty
                    else pd.Series(dtype="int64", name="active_count")
                )
                duration_stats = (
                    completed_df.groupby("staff_key")["duration_sec_num"].agg(
                        median="median",
                        p90=lambda s: s.quantile(0.9),
                    )
                    if (completed_df is not None and not completed_df.empty and "duration_sec_num" in completed_df.columns)
                    else pd.DataFrame(columns=["median", "p90"])
                )

                staff_keys = sorted(set(
                    list(getattr(assigned_counts, "index", []))
                    + list(getattr(active_counts, "index", []))
                    + list(getattr(completed_counts, "index", []))
                ))
                staff_kpi_df = pd.DataFrame(index=staff_keys)
                staff_kpi_df = staff_kpi_df.join(assigned_counts, how="left")
                staff_kpi_df = staff_kpi_df.join(completed_counts, how="left")
                staff_kpi_df = staff_kpi_df.join(active_counts, how="left")
                staff_kpi_df = staff_kpi_df.join(duration_stats, how="left")
                staff_kpi_df["assigned_to"] = staff_kpi_df.index.map(lambda k: label_map.get(k, "Unknown"))
                staff_kpi_df = staff_kpi_df.fillna(0).reset_index(drop=True)
                if "median" in staff_kpi_df.columns:
                    staff_kpi_df["median_min"] = (staff_kpi_df["median"] / 60.0).round(1)
                if "p90" in staff_kpi_df.columns:
                    staff_kpi_df["p90_min"] = (staff_kpi_df["p90"] / 60.0).round(1)
                staff_kpi_df = staff_kpi_df.sort_values(
                    by=["active_count", "completed_count"], ascending=[False, False]
                )
                # Clean up columns for manager readability
                drop_cols = [c for c in ['median', 'p90'] if c in staff_kpi_df.columns]
                staff_kpi_df = staff_kpi_df.drop(columns=drop_cols)
                rename_map = {
                    'assigned_to': 'Staff Member',
                    'assigned_count': 'Assigned',
                    'completed_count': 'Completed',
                    'active_count': 'Active',
                    'median_min': 'Median (min)',
                    'p90_min': 'P90 (min)',
                }
                staff_kpi_df = staff_kpi_df.rename(columns={k: v for k, v in rename_map.items() if k in staff_kpi_df.columns})
                # Format staff names: strip email domain, title case
                if 'Staff Member' in staff_kpi_df.columns:
                    staff_kpi_df['Staff Member'] = staff_kpi_df['Staff Member'].apply(lambda x: x.split('@')[0].replace('.', ' ').title() if isinstance(x, str) else x)
                st.markdown("#### Per-Staff KPIs")
                st.dataframe(make_arrow_safe_df(staff_kpi_df), width="stretch", height=300)


# ==================== BOT FOLDER TARGETS ====================
st.markdown("---")
st.markdown("### Folder Targets")
current_overrides = snapshot["data"].get("settings_overrides.json") if snapshot else None
overrides_err = snapshot["errors"].get("settings_overrides.json") if snapshot else "missing"
if overrides_err:
    st.warning("Data unavailable")
if not isinstance(current_overrides, dict):
    current_overrides = {}

inbox_override = st.text_input(
    "Inbox folder (system target)",
    help="If empty, the service uses its configured default.",
    value=current_overrides.get("inbox_folder", "")
)
processed_override = st.text_input(
    "Processed folder (system target)",
    help="If empty, the service uses its configured default.",
    value=current_overrides.get("processed_folder", "")
)
apps_cc_override = st.text_input(
    "Apps CC (visibility)",
    help="If empty, the service uses its configured default. Multiple addresses may be separated with semicolons (;).",
    value=current_overrides.get("apps_cc_addr", "")
)
manager_cc_override = st.text_input(
    "Manager CC (visibility)",
    help="If empty, the service uses its configured default. Multiple addresses may be separated with semicolons (;).",
    value=current_overrides.get("manager_cc_addr", "")
)
st.caption("Visibility only. Primary assignment comes from staff.txt.")

def validate_visibility_cc(label, value):
    raw = "" if value is None else str(value)
    if not raw.strip():
        return "", None
    if "\n" in raw or "\r" in raw:
        return None, f"{label} must be a single line."
    parts = [part.strip() for part in raw.split(";")]
    for part in parts:
        if not part:
            return None, f"{label} entries must be non-empty."
        if any(ch.isspace() for ch in part):
            return None, f"{label} entries must not contain whitespace."
        if "@" not in part:
            return None, f"{label} entries must contain @."
    return raw.strip(), None

col_override_1, col_override_2 = st.columns(2)
with col_override_1:
    if st.button("Save overrides", use_container_width=True):
        errors = []
        apps_cc_value, apps_cc_err = validate_visibility_cc("Apps CC (visibility)", apps_cc_override)
        if apps_cc_err:
            errors.append(apps_cc_err)
        manager_cc_value, manager_cc_err = validate_visibility_cc("Manager CC (visibility)", manager_cc_override)
        if manager_cc_err:
            errors.append(manager_cc_err)
        if errors:
            for err in errors:
                st.error(err)
        else:
            new_overrides = {}
            if inbox_override.strip():
                new_overrides["inbox_folder"] = inbox_override.strip()
            if processed_override.strip():
                new_overrides["processed_folder"] = processed_override.strip()
            if apps_cc_value:
                new_overrides["apps_cc_addr"] = apps_cc_value
            if manager_cc_value:
                new_overrides["manager_cc_addr"] = manager_cc_value
            ok, err = write_json_atomic(OVERRIDES_FILE, new_overrides)
            if ok:
                st.success("Overrides saved.")
            else:
                st.error("Unable to save overrides.")
with col_override_2:
    if st.button("Clear overrides", use_container_width=True):
        ok, err = write_json_atomic(OVERRIDES_FILE, {})
        if ok:
            st.success("Overrides cleared.")
        else:
            st.error("Unable to clear overrides.")

# ==================== NOTES ====================
st.markdown("---")
st.markdown("### Notes")
st.caption('This dashboard is read-only. Folder targets control where the service looks for inbox and processed items. Outlook settings are not changed by this dashboard.')
