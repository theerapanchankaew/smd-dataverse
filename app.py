import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta
import uuid
import hashlib
import json
import io
import base64

# ============================================================
# SMD INTELLIGENCE HUB v2.0 - Enterprise Decision Platform
# ============================================================
# Features:
# - RBAC (Role-Based Access Control)
# - Department-Specific Dashboards with Custom Analytics
# - Trend Analysis & Forecasting
# - Automated Insight Generation
# - Advanced Report Generator (Excel/PDF/Word/JSON)
# - Custom Dashboard Builder
# - Real-time Alerts & Notifications
# ============================================================

st.set_page_config(
    page_title="SMD Intelligence Hub",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CUSTOM CSS
# ============================================================
st.markdown("""
<style>
    /* Main Theme */
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
    }
    
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border: 1px solid #475569;
        border-radius: 12px;
        padding: 1.2rem;
        margin: 0.5rem 0;
    }
    
    .metric-card.green { border-left: 4px solid #22c55e; }
    .metric-card.red { border-left: 4px solid #ef4444; }
    .metric-card.amber { border-left: 4px solid #f59e0b; }
    .metric-card.blue { border-left: 4px solid #3b82f6; }
    
    /* Section Headers */
    .section-header {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.5rem;
        font-weight: bold;
        margin: 1rem 0;
    }
    
    /* Insight Cards */
    .insight-card {
        background: rgba(59, 130, 246, 0.1);
        border: 1px solid #3b82f6;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .insight-card.warning {
        background: rgba(245, 158, 11, 0.1);
        border-color: #f59e0b;
    }
    
    .insight-card.danger {
        background: rgba(239, 68, 68, 0.1);
        border-color: #ef4444;
    }
    
    .insight-card.success {
        background: rgba(34, 197, 94, 0.1);
        border-color: #22c55e;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #1e293b;
        border-radius: 10px;
        padding: 0.5rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 8px;
        color: #94a3b8;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #3b82f6 !important;
        color: white !important;
    }
    
    /* Hide Streamlit Branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ============================================================
# DATABASE CONFIGURATION
# ============================================================
DB_PATH = Path("smd_intelligence_hub.db")
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# ============================================================
# DATABASE HELPERS
# ============================================================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def exec_sql(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        conn.commit()
    finally:
        conn.close()

def read_df(sql, params=None):
    conn = get_conn()
    try:
        return pd.read_sql_query(sql, conn, params=params) if params else pd.read_sql_query(sql, conn)
    finally:
        conn.close()

def replace_table(table, df):
    conn = get_conn()
    try:
        df.to_sql(table, conn, if_exists="replace", index=False)
    finally:
        conn.close()

def uid(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"

def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def to_date_id(d):
    return int(d.strftime("%Y%m%d"))

def from_date_id(date_id):
    s = str(date_id)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

# ============================================================
# DATABASE INITIALIZATION
# ============================================================
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    
    # Dimension Tables
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY, date TEXT NOT NULL, 
        month INTEGER, quarter INTEGER, year INTEGER, week INTEGER
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_department (
        dept_id TEXT PRIMARY KEY, dept_name TEXT NOT NULL,
        dept_code TEXT, dept_head_person_id TEXT, description TEXT,
        color TEXT DEFAULT '#3b82f6'
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_person (
        person_id TEXT PRIMARY KEY, person_name TEXT NOT NULL,
        role TEXT, department TEXT, email TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_kpi (
        kpi_id TEXT PRIMARY KEY, kpi_name TEXT NOT NULL,
        kpi_definition TEXT, dept_id TEXT, calculation_logic TEXT,
        unit TEXT, target_direction TEXT DEFAULT 'higher_is_better'
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_strategy (
        strategy_id TEXT PRIMARY KEY, strategy_name TEXT NOT NULL,
        dept_id TEXT, start_date INTEGER, end_date INTEGER, status TEXT
    )""")
    
    # User & RBAC
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_user (
        username TEXT PRIMARY KEY, password_hash TEXT NOT NULL,
        role TEXT NOT NULL, dept_id TEXT, person_id TEXT,
        is_enabled INTEGER DEFAULT 1
    )""")
    
    # Fact Tables
    cur.execute("""CREATE TABLE IF NOT EXISTS fact_kpi_data (
        record_id TEXT PRIMARY KEY, date_id INTEGER NOT NULL,
        dept_id TEXT NOT NULL, kpi_id TEXT NOT NULL,
        actual_value REAL, target_value REAL,
        created_ts TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS fact_work_item (
        work_id TEXT PRIMARY KEY, dept_id TEXT NOT NULL,
        strategy_id TEXT, kpi_id TEXT, owner_person_id TEXT,
        work_title TEXT NOT NULL, work_type TEXT,
        priority TEXT, status TEXT, progress_percent REAL,
        start_date_id INTEGER, due_date_id INTEGER,
        risk_level TEXT, notes TEXT, created_ts TEXT, updated_ts TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS fact_meeting (
        meeting_id TEXT PRIMARY KEY, date_id INTEGER NOT NULL,
        dept_id TEXT, meeting_title TEXT NOT NULL,
        meeting_type TEXT, organizer_person_id TEXT,
        minutes_text TEXT, created_ts TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS fact_meeting_action (
        action_id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL,
        dept_id TEXT, owner_person_id TEXT,
        action_title TEXT NOT NULL, status TEXT,
        due_date_id INTEGER, progress_percent REAL,
        linked_work_id TEXT, created_ts TEXT, updated_ts TEXT
    )""")
    
    # Dashboard & Config Tables
    cur.execute("""CREATE TABLE IF NOT EXISTS user_dashboard_config (
        config_id TEXT PRIMARY KEY, username TEXT NOT NULL,
        dept_id TEXT, dashboard_name TEXT NOT NULL,
        widgets_json TEXT, layout_json TEXT,
        is_default INTEGER DEFAULT 0, created_ts TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS log_insights (
        insight_id TEXT PRIMARY KEY, dept_id TEXT,
        insight_type TEXT, metric_name TEXT,
        insight_text TEXT NOT NULL, severity TEXT,
        is_read INTEGER DEFAULT 0, created_ts TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS config_alerts (
        alert_id TEXT PRIMARY KEY, dept_id TEXT,
        kpi_id TEXT, condition_type TEXT,
        threshold_value REAL, is_active INTEGER DEFAULT 1
    )""")
    
    conn.commit()
    conn.close()

def ensure_dim_date(start_date, end_date):
    conn = get_conn()
    cur = conn.cursor()
    d = start_date
    while d <= end_date:
        date_id = to_date_id(d)
        cur.execute("""INSERT OR IGNORE INTO dim_date 
            (date_id, date, month, quarter, year, week) VALUES (?, ?, ?, ?, ?, ?)""",
            (date_id, d.isoformat(), d.month, (d.month-1)//3+1, d.year, d.isocalendar()[1]))
        d += timedelta(days=1)
    conn.commit()
    conn.close()

# ============================================================
# SEED DEMO DATA
# ============================================================
def seed_demo_data():
    today = date.today()
    ensure_dim_date(today - timedelta(days=365), today + timedelta(days=90))
    
    # Departments
    replace_table("dim_department", pd.DataFrame([
        {"dept_id": "MDS", "dept_name": "Marketing & Sales", "dept_code": "MDS", 
         "description": "การตลาดและการขาย", "color": "#06b6d4"},
        {"dept_id": "SGS", "dept_name": "Strategy & Planning", "dept_code": "SGS",
         "description": "กลยุทธ์และแผนงาน", "color": "#f59e0b"},
        {"dept_id": "BMS", "dept_name": "Governance & Compliance", "dept_code": "BMS",
         "description": "กำกับดูแลและความสอดคล้อง", "color": "#10b981"},
        {"dept_id": "IT", "dept_name": "IT Operations", "dept_code": "IT",
         "description": "เทคโนโลยีสารสนเทศ", "color": "#8b5cf6"},
    ]))
    
    # Persons
    replace_table("dim_person", pd.DataFrame([
        {"person_id": "P1", "person_name": "ผู้อำนวยการ", "role": "Executive", "department": "Management"},
        {"person_id": "P2", "person_name": "หัวหน้า MDS", "role": "DeptHead", "department": "MDS"},
        {"person_id": "P3", "person_name": "หัวหน้า SGS", "role": "DeptHead", "department": "SGS"},
        {"person_id": "P4", "person_name": "หัวหน้า BMS", "role": "DeptHead", "department": "BMS"},
        {"person_id": "P5", "person_name": "หัวหน้า IT", "role": "DeptHead", "department": "IT"},
        {"person_id": "P6", "person_name": "เจ้าหน้าที่ MDS", "role": "Staff", "department": "MDS"},
        {"person_id": "P7", "person_name": "เจ้าหน้าที่ SGS", "role": "Staff", "department": "SGS"},
    ]))
    
    # KPIs
    replace_table("dim_kpi", pd.DataFrame([
        # MDS KPIs
        {"kpi_id": "MDS_K1", "kpi_name": "Lead Volume", "dept_id": "MDS", "unit": "leads", "target_direction": "higher_is_better"},
        {"kpi_id": "MDS_K2", "kpi_name": "Conversion Rate", "dept_id": "MDS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "MDS_K3", "kpi_name": "Pipeline Value", "dept_id": "MDS", "unit": "MB", "target_direction": "higher_is_better"},
        # SGS KPIs
        {"kpi_id": "SGS_K1", "kpi_name": "Strategy Progress", "dept_id": "SGS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "SGS_K2", "kpi_name": "Risk Score", "dept_id": "SGS", "unit": "score", "target_direction": "lower_is_better"},
        {"kpi_id": "SGS_K3", "kpi_name": "Budget Utilization", "dept_id": "SGS", "unit": "%", "target_direction": "higher_is_better"},
        # BMS KPIs
        {"kpi_id": "BMS_K1", "kpi_name": "Compliance Score", "dept_id": "BMS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "BMS_K2", "kpi_name": "Action Closure Rate", "dept_id": "BMS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "BMS_K3", "kpi_name": "Audit Findings", "dept_id": "BMS", "unit": "items", "target_direction": "lower_is_better"},
        # IT KPIs
        {"kpi_id": "IT_K1", "kpi_name": "System Uptime", "dept_id": "IT", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "IT_K2", "kpi_name": "Incident Count", "dept_id": "IT", "unit": "incidents", "target_direction": "lower_is_better"},
        {"kpi_id": "IT_K3", "kpi_name": "MTTR", "dept_id": "IT", "unit": "hours", "target_direction": "lower_is_better"},
    ]))
    
    # Generate KPI Data (last 90 days)
    import random
    kpi_data = []
    for i in range(90):
        d = today - timedelta(days=89-i)
        date_id = to_date_id(d)
        
        # MDS Data
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "MDS", "kpi_id": "MDS_K1",
                        "actual_value": random.randint(80, 150) + i*0.3, "target_value": 100, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "MDS", "kpi_id": "MDS_K2",
                        "actual_value": random.uniform(15, 30), "target_value": 25, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "MDS", "kpi_id": "MDS_K3",
                        "actual_value": random.uniform(50, 100) + i*0.5, "target_value": 80, "created_ts": datetime.now().isoformat()})
        
        # SGS Data
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "SGS", "kpi_id": "SGS_K1",
                        "actual_value": min(45 + i*0.5 + random.uniform(-5, 5), 100), "target_value": 80, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "SGS", "kpi_id": "SGS_K2",
                        "actual_value": max(70 - i*0.2 + random.uniform(-5, 5), 20), "target_value": 40, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "SGS", "kpi_id": "SGS_K3",
                        "actual_value": min(30 + i*0.4 + random.uniform(-3, 3), 95), "target_value": 75, "created_ts": datetime.now().isoformat()})
        
        # BMS Data
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "BMS", "kpi_id": "BMS_K1",
                        "actual_value": min(85 + i*0.1 + random.uniform(-2, 2), 99), "target_value": 95, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "BMS", "kpi_id": "BMS_K2",
                        "actual_value": random.uniform(70, 95), "target_value": 90, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "BMS", "kpi_id": "BMS_K3",
                        "actual_value": max(15 - i*0.1 + random.uniform(-2, 2), 2), "target_value": 5, "created_ts": datetime.now().isoformat()})
        
        # IT Data
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "IT", "kpi_id": "IT_K1",
                        "actual_value": random.uniform(99.0, 99.99), "target_value": 99.5, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "IT", "kpi_id": "IT_K2",
                        "actual_value": random.randint(0, 5), "target_value": 2, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "IT", "kpi_id": "IT_K3",
                        "actual_value": random.uniform(0.5, 4), "target_value": 2, "created_ts": datetime.now().isoformat()})
    
    replace_table("fact_kpi_data", pd.DataFrame(kpi_data))
    
    # Work Items
    work_items = [
        {"work_id": uid("W"), "dept_id": "MDS", "work_title": "พัฒนาแคมเปญ Q1", "work_type": "Project",
         "priority": "High", "status": "In Progress", "progress_percent": 65, "risk_level": "Medium",
         "start_date_id": to_date_id(today-timedelta(days=30)), "due_date_id": to_date_id(today+timedelta(days=15))},
        {"work_id": uid("W"), "dept_id": "MDS", "work_title": "Lead Generation Automation", "work_type": "Project",
         "priority": "High", "status": "In Progress", "progress_percent": 40, "risk_level": "Low",
         "start_date_id": to_date_id(today-timedelta(days=14)), "due_date_id": to_date_id(today+timedelta(days=30))},
        {"work_id": uid("W"), "dept_id": "SGS", "work_title": "ทบทวนแผนยุทธศาสตร์ 2025", "work_type": "Project",
         "priority": "Critical", "status": "At Risk", "progress_percent": 30, "risk_level": "High",
         "start_date_id": to_date_id(today-timedelta(days=45)), "due_date_id": to_date_id(today+timedelta(days=5))},
        {"work_id": uid("W"), "dept_id": "BMS", "work_title": "ปิด Audit Findings", "work_type": "Improvement",
         "priority": "High", "status": "In Progress", "progress_percent": 75, "risk_level": "Medium",
         "start_date_id": to_date_id(today-timedelta(days=20)), "due_date_id": to_date_id(today+timedelta(days=10))},
        {"work_id": uid("W"), "dept_id": "IT", "work_title": "Database Migration", "work_type": "Project",
         "priority": "High", "status": "Planned", "progress_percent": 10, "risk_level": "High",
         "start_date_id": to_date_id(today), "due_date_id": to_date_id(today+timedelta(days=60))},
    ]
    for w in work_items:
        w["created_ts"] = datetime.now().isoformat()
        w["updated_ts"] = datetime.now().isoformat()
    replace_table("fact_work_item", pd.DataFrame(work_items))
    
    # Users
    replace_table("dim_user", pd.DataFrame([
        {"username": "admin", "password_hash": sha256("demo123"), "role": "Admin", "dept_id": None, "person_id": "P1", "is_enabled": 1},
        {"username": "executive", "password_hash": sha256("demo123"), "role": "Executive", "dept_id": None, "person_id": "P1", "is_enabled": 1},
        {"username": "mds_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "MDS", "person_id": "P2", "is_enabled": 1},
        {"username": "sgs_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "SGS", "person_id": "P3", "is_enabled": 1},
        {"username": "bms_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "BMS", "person_id": "P4", "is_enabled": 1},
        {"username": "it_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "IT", "person_id": "P5", "is_enabled": 1},
        {"username": "mds_staff", "password_hash": sha256("demo123"), "role": "Staff", "dept_id": "MDS", "person_id": "P6", "is_enabled": 1},
    ]))

# ============================================================
# AUTH HELPERS
# ============================================================
def login(username, password):
    df = read_df("SELECT * FROM dim_user WHERE username = ? AND is_enabled = 1", params=(username,))
    if df.empty:
        return False
    row = df.iloc[0].to_dict()
    if row["password_hash"] != sha256(password):
        return False
    st.session_state["auth"] = {
        "logged_in": True,
        "username": row["username"],
        "role": row["role"],
        "dept_id": row.get("dept_id"),
        "person_id": row.get("person_id"),
    }
    return True

def logout():
    st.session_state["auth"] = {"logged_in": False}

def is_logged_in():
    return st.session_state.get("auth", {}).get("logged_in", False)

def current_user():
    return st.session_state.get("auth", {})

def get_user_dept_scope():
    role = current_user().get("role")
    if role in ["Admin", "Executive"]:
        return None  # Can see all
    return current_user().get("dept_id")

# ============================================================
# ANALYTICS FUNCTIONS
# ============================================================
def calculate_trend(df, value_col, periods=7):
    """Calculate trend direction and percentage change"""
    if len(df) < 2:
        return "stable", 0
    
    recent = df.tail(periods)[value_col].mean()
    previous = df.head(len(df)-periods)[value_col].mean() if len(df) > periods else df[value_col].iloc[0]
    
    if previous == 0:
        return "new", 0
    
    change = ((recent - previous) / previous) * 100
    
    if change > 5:
        return "up", change
    elif change < -5:
        return "down", change
    else:
        return "stable", change

def generate_insights(dept_id=None):
    """Generate automated insights based on data patterns"""
    insights = []
    today_id = to_date_id(date.today())
    week_ago_id = to_date_id(date.today() - timedelta(days=7))
    
    # Get latest KPI data
    where_clause = f"AND dept_id = '{dept_id}'" if dept_id else ""
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, d.target_direction
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        WHERE k.date_id >= {week_ago_id} {where_clause}
        ORDER BY k.date_id DESC
    """)
    
    if kpi_df.empty:
        return insights
    
    # Analyze each KPI
    for kpi_id in kpi_df['kpi_id'].unique():
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id].copy()
        kpi_name = kpi_data.iloc[0]['kpi_name']
        target_direction = kpi_data.iloc[0].get('target_direction', 'higher_is_better')
        
        latest = kpi_data.iloc[0]
        actual = latest['actual_value']
        target = latest['target_value']
        
        # Check against target
        if target and actual:
            if target_direction == 'higher_is_better':
                if actual >= target:
                    insights.append({
                        "type": "success",
                        "icon": "✅",
                        "text": f"{kpi_name}: บรรลุเป้าหมาย ({actual:.1f} vs {target:.1f})",
                        "severity": "low"
                    })
                elif actual < target * 0.8:
                    insights.append({
                        "type": "danger",
                        "icon": "🚨",
                        "text": f"{kpi_name}: ต่ำกว่าเป้าหมายมาก ({actual:.1f} vs {target:.1f})",
                        "severity": "high"
                    })
            else:  # lower is better
                if actual <= target:
                    insights.append({
                        "type": "success",
                        "icon": "✅",
                        "text": f"{kpi_name}: บรรลุเป้าหมาย ({actual:.1f} vs {target:.1f})",
                        "severity": "low"
                    })
                elif actual > target * 1.5:
                    insights.append({
                        "type": "danger",
                        "icon": "🚨",
                        "text": f"{kpi_name}: สูงกว่าเป้าหมายมาก ({actual:.1f} vs {target:.1f})",
                        "severity": "high"
                    })
        
        # Check trend
        if len(kpi_data) >= 3:
            trend, change = calculate_trend(kpi_data, 'actual_value', 3)
            if trend == "up" and abs(change) > 10:
                insights.append({
                    "type": "info",
                    "icon": "📈",
                    "text": f"{kpi_name}: เพิ่มขึ้น {abs(change):.1f}% ในสัปดาห์ที่ผ่านมา",
                    "severity": "medium"
                })
            elif trend == "down" and abs(change) > 10:
                insights.append({
                    "type": "warning",
                    "icon": "📉",
                    "text": f"{kpi_name}: ลดลง {abs(change):.1f}% ในสัปดาห์ที่ผ่านมา",
                    "severity": "medium"
                })
    
    # Check overdue work items
    work_df = read_df(f"""
        SELECT * FROM fact_work_item 
        WHERE due_date_id < {today_id} AND status NOT IN ('Done', 'Cancelled')
        {where_clause.replace('dept_id', 'fact_work_item.dept_id') if where_clause else ''}
    """)
    
    if len(work_df) > 0:
        insights.append({
            "type": "danger",
            "icon": "⏰",
            "text": f"มี {len(work_df)} งานที่เกินกำหนด - ต้องดำเนินการเร่งด่วน",
            "severity": "high"
        })
    
    # Check high-risk items
    risk_df = read_df(f"""
        SELECT * FROM fact_work_item 
        WHERE risk_level = 'High' AND status NOT IN ('Done', 'Cancelled')
        {where_clause.replace('dept_id', 'fact_work_item.dept_id') if where_clause else ''}
    """)
    
    if len(risk_df) > 0:
        insights.append({
            "type": "warning",
            "icon": "⚠️",
            "text": f"มี {len(risk_df)} งานที่มีความเสี่ยงสูง - ต้องติดตามใกล้ชิด",
            "severity": "medium"
        })
    
    return insights

def forecast_simple(series, periods=7):
    """Simple moving average forecast"""
    if len(series) < 3:
        return [series.mean()] * periods
    
    ma = series.tail(7).mean()
    trend = (series.tail(3).mean() - series.head(3).mean()) / len(series) * 3
    
    forecasts = []
    for i in range(periods):
        forecasts.append(ma + trend * (i + 1))
    return forecasts

# ============================================================
# EXPORT FUNCTIONS
# ============================================================
def export_to_excel(df, filename):
    """Export DataFrame to Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Data']
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))
    
    b64 = base64.b64encode(output.getvalue()).decode()
    return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}.xlsx">📥 Download Excel</a>'

def export_to_csv(df, filename):
    """Export DataFrame to CSV"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">📥 Download CSV</a>'

def export_to_json(df, filename):
    """Export DataFrame to JSON"""
    json_str = df.to_json(orient='records', date_format='iso', indent=2)
    b64 = base64.b64encode(json_str.encode()).decode()
    return f'<a href="data:file/json;base64,{b64}" download="{filename}.json">📥 Download JSON</a>'

# ============================================================
# VISUALIZATION FUNCTIONS
# ============================================================
def create_kpi_card(title, value, target, unit="", trend_direction=None, trend_value=None):
    """Create a KPI metric card"""
    # Determine status color
    if target:
        ratio = value / target if target != 0 else 1
        if ratio >= 1:
            status_class = "green"
            status_icon = "✅"
        elif ratio >= 0.8:
            status_class = "amber"
            status_icon = "⚠️"
        else:
            status_class = "red"
            status_icon = "❌"
    else:
        status_class = "blue"
        status_icon = "📊"
    
    # Trend indicator
    trend_html = ""
    if trend_direction:
        if trend_direction == "up":
            trend_html = f'<span style="color: #22c55e;">↑ {abs(trend_value):.1f}%</span>'
        elif trend_direction == "down":
            trend_html = f'<span style="color: #ef4444;">↓ {abs(trend_value):.1f}%</span>'
        else:
            trend_html = f'<span style="color: #f59e0b;">→ {abs(trend_value):.1f}%</span>'
    
    return f"""
    <div class="metric-card {status_class}">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="color: #94a3b8; font-size: 0.85rem;">{title}</span>
            <span>{status_icon}</span>
        </div>
        <div style="font-size: 2rem; font-weight: bold; color: white; margin: 0.5rem 0;">
            {value:,.1f} <span style="font-size: 1rem; color: #64748b;">{unit}</span>
        </div>
        <div style="display: flex; justify-content: space-between; color: #64748b; font-size: 0.8rem;">
            <span>Target: {target:,.1f}</span>
            {trend_html}
        </div>
    </div>
    """

def create_trend_chart(df, x_col, y_col, title, color="#3b82f6"):
    """Create a trend line chart"""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[x_col], y=df[y_col],
        mode='lines+markers',
        name='Actual',
        line=dict(color=color, width=2),
        marker=dict(size=6)
    ))
    
    # Add target line if available
    if 'target_value' in df.columns:
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df['target_value'],
            mode='lines',
            name='Target',
            line=dict(color='#ef4444', width=2, dash='dash')
        ))
    
    fig.update_layout(
        title=title,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='#94a3b8',
        xaxis=dict(showgrid=True, gridcolor='rgba(71, 85, 105, 0.3)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(71, 85, 105, 0.3)'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
        height=350,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    return fig

def create_gauge_chart(value, target, title, max_val=None):
    """Create a gauge chart"""
    if max_val is None:
        max_val = max(value, target) * 1.2
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        delta={'reference': target, 'relative': True, 'valueformat': '.1%'},
        title={'text': title, 'font': {'color': '#94a3b8'}},
        gauge={
            'axis': {'range': [0, max_val], 'tickcolor': '#64748b'},
            'bar': {'color': '#3b82f6'},
            'bgcolor': '#1e293b',
            'bordercolor': '#475569',
            'steps': [
                {'range': [0, target * 0.8], 'color': 'rgba(239, 68, 68, 0.3)'},
                {'range': [target * 0.8, target], 'color': 'rgba(245, 158, 11, 0.3)'},
                {'range': [target, max_val], 'color': 'rgba(34, 197, 94, 0.3)'}
            ],
            'threshold': {
                'line': {'color': '#ef4444', 'width': 4},
                'thickness': 0.75,
                'value': target
            }
        }
    ))
    
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='#94a3b8',
        height=250,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    return fig

# ============================================================
# DEPARTMENT DASHBOARD FUNCTIONS
# ============================================================
def render_mds_dashboard():
    """Render MDS (Marketing & Sales) Dashboard"""
    st.markdown("## 💼 MDS - Marketing & Sales Dashboard")
    
    # Load data
    today_id = to_date_id(date.today())
    thirty_days_ago = to_date_id(date.today() - timedelta(days=30))
    
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, dim.date
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        JOIN dim_date dim ON k.date_id = dim.date_id
        WHERE k.dept_id = 'MDS' AND k.date_id >= {thirty_days_ago}
        ORDER BY k.date_id
    """)
    
    # Get latest values
    latest_df = kpi_df.groupby('kpi_id').last().reset_index()
    
    # KPI Cards
    col1, col2, col3 = st.columns(3)
    
    for i, (col, kpi_id) in enumerate(zip([col1, col2, col3], ['MDS_K1', 'MDS_K2', 'MDS_K3'])):
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id]
        if not kpi_data.empty:
            latest = kpi_data.iloc[-1]
            trend, change = calculate_trend(kpi_data, 'actual_value')
            
            with col:
                st.markdown(create_kpi_card(
                    latest['kpi_name'],
                    latest['actual_value'],
                    latest['target_value'],
                    unit=['leads', '%', 'MB'][i],
                    trend_direction=trend,
                    trend_value=change
                ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts Row
    col1, col2 = st.columns(2)
    
    with col1:
        # Lead Volume Trend
        lead_data = kpi_df[kpi_df['kpi_id'] == 'MDS_K1'].copy()
        if not lead_data.empty:
            fig = create_trend_chart(lead_data, 'date', 'actual_value', 'Lead Volume Trend (30 Days)', '#06b6d4')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Conversion Rate Trend
        conv_data = kpi_df[kpi_df['kpi_id'] == 'MDS_K2'].copy()
        if not conv_data.empty:
            fig = create_trend_chart(conv_data, 'date', 'actual_value', 'Conversion Rate Trend (%)', '#f59e0b')
            st.plotly_chart(fig, use_container_width=True)
    
    # Pipeline Value Gauge
    col1, col2 = st.columns(2)
    
    with col1:
        pipeline_data = kpi_df[kpi_df['kpi_id'] == 'MDS_K3']
        if not pipeline_data.empty:
            latest = pipeline_data.iloc[-1]
            fig = create_gauge_chart(latest['actual_value'], latest['target_value'], 'Pipeline Value (MB)', max_val=150)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Work Items Status
        work_df = read_df("SELECT status, COUNT(*) as count FROM fact_work_item WHERE dept_id = 'MDS' GROUP BY status")
        if not work_df.empty:
            fig = px.pie(work_df, values='count', names='status', title='Work Items by Status',
                        color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8', height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    # Insights
    st.markdown("### 💡 AI Insights")
    insights = generate_insights('MDS')
    if insights:
        for insight in insights[:5]:
            st.markdown(f"""
            <div class="insight-card {insight['type']}">
                {insight['icon']} {insight['text']}
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("ไม่มี insights ใหม่ในขณะนี้")

def render_sgs_dashboard():
    """Render SGS (Strategy & Planning) Dashboard"""
    st.markdown("## 🧭 SGS - Strategy & Planning Dashboard")
    
    today_id = to_date_id(date.today())
    thirty_days_ago = to_date_id(date.today() - timedelta(days=30))
    
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, dim.date
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        JOIN dim_date dim ON k.date_id = dim.date_id
        WHERE k.dept_id = 'SGS' AND k.date_id >= {thirty_days_ago}
        ORDER BY k.date_id
    """)
    
    # KPI Cards
    col1, col2, col3 = st.columns(3)
    
    for i, (col, kpi_id, unit) in enumerate(zip([col1, col2, col3], 
                                                 ['SGS_K1', 'SGS_K2', 'SGS_K3'],
                                                 ['%', 'score', '%'])):
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id]
        if not kpi_data.empty:
            latest = kpi_data.iloc[-1]
            trend, change = calculate_trend(kpi_data, 'actual_value')
            
            with col:
                st.markdown(create_kpi_card(
                    latest['kpi_name'],
                    latest['actual_value'],
                    latest['target_value'],
                    unit=unit,
                    trend_direction=trend,
                    trend_value=change
                ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Risk Matrix
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ⚠️ Risk Matrix")
        work_df = read_df("SELECT * FROM fact_work_item WHERE dept_id = 'SGS'")
        
        if not work_df.empty:
            # Create risk matrix
            risk_map = {'Low': 1, 'Medium': 2, 'High': 3}
            priority_map = {'Low': 1, 'Medium': 2, 'High': 3, 'Critical': 4}
            
            work_df['risk_score'] = work_df['risk_level'].map(risk_map).fillna(2)
            work_df['priority_score'] = work_df['priority'].map(priority_map).fillna(2)
            
            fig = px.scatter(work_df, x='risk_score', y='priority_score', 
                           text='work_title', color='status',
                           size_max=40)
            fig.update_traces(textposition='top center', marker=dict(size=20))
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#94a3b8',
                xaxis=dict(title='Risk Level', tickvals=[1,2,3], ticktext=['Low','Medium','High']),
                yaxis=dict(title='Priority', tickvals=[1,2,3,4], ticktext=['Low','Medium','High','Critical']),
                height=350
            )
            
            # Add quadrant colors
            fig.add_shape(type="rect", x0=0.5, y0=0.5, x1=2, y1=2.5, fillcolor="rgba(34, 197, 94, 0.1)", line_width=0)
            fig.add_shape(type="rect", x0=2, y0=2.5, x1=3.5, y1=4.5, fillcolor="rgba(239, 68, 68, 0.1)", line_width=0)
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Strategy Progress
        progress_data = kpi_df[kpi_df['kpi_id'] == 'SGS_K1']
        if not progress_data.empty:
            fig = create_trend_chart(progress_data, 'date', 'actual_value', 'Strategy Progress (%)', '#f59e0b')
            st.plotly_chart(fig, use_container_width=True)
    
    # Budget Utilization
    col1, col2 = st.columns(2)
    
    with col1:
        budget_data = kpi_df[kpi_df['kpi_id'] == 'SGS_K3']
        if not budget_data.empty:
            latest = budget_data.iloc[-1]
            fig = create_gauge_chart(latest['actual_value'], latest['target_value'], 'Budget Utilization (%)', max_val=100)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        risk_data = kpi_df[kpi_df['kpi_id'] == 'SGS_K2']
        if not risk_data.empty:
            fig = create_trend_chart(risk_data, 'date', 'actual_value', 'Risk Score Trend', '#ef4444')
            st.plotly_chart(fig, use_container_width=True)
    
    # Insights
    st.markdown("### 💡 AI Insights")
    insights = generate_insights('SGS')
    for insight in insights[:5]:
        st.markdown(f"""
        <div class="insight-card {insight['type']}">
            {insight['icon']} {insight['text']}
        </div>
        """, unsafe_allow_html=True)

def render_bms_dashboard():
    """Render BMS (Governance & Compliance) Dashboard"""
    st.markdown("## ⚖️ BMS - Governance & Compliance Dashboard")
    
    today_id = to_date_id(date.today())
    thirty_days_ago = to_date_id(date.today() - timedelta(days=30))
    
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, dim.date
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        JOIN dim_date dim ON k.date_id = dim.date_id
        WHERE k.dept_id = 'BMS' AND k.date_id >= {thirty_days_ago}
        ORDER BY k.date_id
    """)
    
    # KPI Cards
    col1, col2, col3 = st.columns(3)
    
    for i, (col, kpi_id, unit) in enumerate(zip([col1, col2, col3], 
                                                 ['BMS_K1', 'BMS_K2', 'BMS_K3'],
                                                 ['%', '%', 'items'])):
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id]
        if not kpi_data.empty:
            latest = kpi_data.iloc[-1]
            trend, change = calculate_trend(kpi_data, 'actual_value')
            
            with col:
                st.markdown(create_kpi_card(
                    latest['kpi_name'],
                    latest['actual_value'],
                    latest['target_value'],
                    unit=unit,
                    trend_direction=trend,
                    trend_value=change
                ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        compliance_data = kpi_df[kpi_df['kpi_id'] == 'BMS_K1']
        if not compliance_data.empty:
            latest = compliance_data.iloc[-1]
            fig = create_gauge_chart(latest['actual_value'], latest['target_value'], 'Compliance Score (%)', max_val=100)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        findings_data = kpi_df[kpi_df['kpi_id'] == 'BMS_K3']
        if not findings_data.empty:
            fig = create_trend_chart(findings_data, 'date', 'actual_value', 'Audit Findings Trend', '#ef4444')
            st.plotly_chart(fig, use_container_width=True)
    
    # Action Items Tracker
    st.markdown("### 📋 Action Items Tracker")
    actions_df = read_df("""
        SELECT * FROM fact_meeting_action 
        WHERE dept_id = 'BMS' OR dept_id IS NULL
        ORDER BY due_date_id
    """)
    
    if not actions_df.empty:
        st.dataframe(actions_df, use_container_width=True)
    else:
        st.info("ไม่มี Action Items")
    
    # Insights
    st.markdown("### 💡 AI Insights")
    insights = generate_insights('BMS')
    for insight in insights[:5]:
        st.markdown(f"""
        <div class="insight-card {insight['type']}">
            {insight['icon']} {insight['text']}
        </div>
        """, unsafe_allow_html=True)

def render_it_dashboard():
    """Render IT Operations Dashboard"""
    st.markdown("## 🖥️ IT Operations Dashboard")
    
    today_id = to_date_id(date.today())
    thirty_days_ago = to_date_id(date.today() - timedelta(days=30))
    
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, dim.date
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        JOIN dim_date dim ON k.date_id = dim.date_id
        WHERE k.dept_id = 'IT' AND k.date_id >= {thirty_days_ago}
        ORDER BY k.date_id
    """)
    
    # KPI Cards
    col1, col2, col3 = st.columns(3)
    
    for i, (col, kpi_id, unit) in enumerate(zip([col1, col2, col3], 
                                                 ['IT_K1', 'IT_K2', 'IT_K3'],
                                                 ['%', 'incidents', 'hours'])):
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id]
        if not kpi_data.empty:
            latest = kpi_data.iloc[-1]
            trend, change = calculate_trend(kpi_data, 'actual_value')
            
            with col:
                st.markdown(create_kpi_card(
                    latest['kpi_name'],
                    latest['actual_value'],
                    latest['target_value'],
                    unit=unit,
                    trend_direction=trend,
                    trend_value=change
                ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        uptime_data = kpi_df[kpi_df['kpi_id'] == 'IT_K1']
        if not uptime_data.empty:
            fig = create_trend_chart(uptime_data, 'date', 'actual_value', 'System Uptime (%)', '#22c55e')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        incident_data = kpi_df[kpi_df['kpi_id'] == 'IT_K2']
        if not incident_data.empty:
            fig = create_trend_chart(incident_data, 'date', 'actual_value', 'Incident Count', '#ef4444')
            st.plotly_chart(fig, use_container_width=True)
    
    # MTTR Gauge
    col1, col2 = st.columns(2)
    
    with col1:
        mttr_data = kpi_df[kpi_df['kpi_id'] == 'IT_K3']
        if not mttr_data.empty:
            latest = mttr_data.iloc[-1]
            fig = create_gauge_chart(latest['actual_value'], latest['target_value'], 'MTTR (Hours)', max_val=8)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Work items
        work_df = read_df("SELECT priority, COUNT(*) as count FROM fact_work_item WHERE dept_id = 'IT' GROUP BY priority")
        if not work_df.empty:
            fig = px.bar(work_df, x='priority', y='count', title='Work Items by Priority',
                        color='priority',
                        color_discrete_map={'Critical': '#ef4444', 'High': '#f59e0b', 'Medium': '#3b82f6', 'Low': '#22c55e'})
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8', height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    # Insights
    st.markdown("### 💡 AI Insights")
    insights = generate_insights('IT')
    for insight in insights[:5]:
        st.markdown(f"""
        <div class="insight-card {insight['type']}">
            {insight['icon']} {insight['text']}
        </div>
        """, unsafe_allow_html=True)

# ============================================================
# EXECUTIVE DASHBOARD
# ============================================================
def render_executive_dashboard():
    """Render Executive Overview Dashboard"""
    st.markdown("## 📊 Executive Dashboard")
    st.markdown("ภาพรวมผลการดำเนินงานทุกแผนก")
    
    # Department Summary
    depts = read_df("SELECT * FROM dim_department ORDER BY dept_name")
    
    cols = st.columns(4)
    for i, (_, dept) in enumerate(depts.iterrows()):
        with cols[i]:
            # Get latest KPI summary for this department
            kpi_summary = read_df(f"""
                SELECT AVG(actual_value / NULLIF(target_value, 0)) as avg_achievement
                FROM fact_kpi_data k
                JOIN dim_kpi d ON k.kpi_id = d.kpi_id
                WHERE k.dept_id = '{dept['dept_id']}'
                AND k.date_id = (SELECT MAX(date_id) FROM fact_kpi_data WHERE dept_id = '{dept['dept_id']}')
            """)
            
            achievement = kpi_summary.iloc[0]['avg_achievement'] * 100 if not kpi_summary.empty and kpi_summary.iloc[0]['avg_achievement'] else 0
            
            status_class = "green" if achievement >= 100 else ("amber" if achievement >= 80 else "red")
            
            st.markdown(f"""
            <div class="metric-card {status_class}">
                <h4 style="color: white; margin: 0;">{dept['dept_name']}</h4>
                <p style="color: #64748b; font-size: 0.8rem;">{dept['description']}</p>
                <div style="font-size: 2rem; font-weight: bold; color: {dept['color']};">
                    {achievement:.0f}%
                </div>
                <small style="color: #64748b;">KPI Achievement</small>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Cross-Department Comparison
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 KPI Achievement by Department")
        
        dept_performance = read_df("""
            SELECT d.dept_name, 
                   AVG(k.actual_value / NULLIF(k.target_value, 0)) * 100 as achievement
            FROM fact_kpi_data k
            JOIN dim_department d ON k.dept_id = d.dept_id
            GROUP BY d.dept_name
        """)
        
        if not dept_performance.empty:
            fig = px.bar(dept_performance, x='dept_name', y='achievement',
                        color='achievement',
                        color_continuous_scale=['#ef4444', '#f59e0b', '#22c55e'],
                        range_color=[0, 120])
            fig.add_hline(y=100, line_dash="dash", line_color="#ef4444", annotation_text="Target (100%)")
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#94a3b8',
                xaxis_title="Department",
                yaxis_title="Achievement (%)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 📋 Work Items Overview")
        
        work_summary = read_df("""
            SELECT d.dept_name, w.status, COUNT(*) as count
            FROM fact_work_item w
            JOIN dim_department d ON w.dept_id = d.dept_id
            GROUP BY d.dept_name, w.status
        """)
        
        if not work_summary.empty:
            fig = px.bar(work_summary, x='dept_name', y='count', color='status',
                        barmode='stack',
                        color_discrete_map={
                            'Done': '#22c55e', 'In Progress': '#3b82f6',
                            'Planned': '#8b5cf6', 'At Risk': '#ef4444'
                        })
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#94a3b8',
                xaxis_title="Department",
                yaxis_title="Work Items",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # All Insights
    st.markdown("### 💡 Organization-Wide Insights")
    insights = generate_insights()
    
    # Sort by severity
    severity_order = {'high': 0, 'medium': 1, 'low': 2}
    insights.sort(key=lambda x: severity_order.get(x.get('severity', 'low'), 3))
    
    for insight in insights[:8]:
        st.markdown(f"""
        <div class="insight-card {insight['type']}">
            {insight['icon']} {insight['text']}
        </div>
        """, unsafe_allow_html=True)

# ============================================================
# REPORT GENERATOR
# ============================================================
def render_report_generator():
    """Render Report Generator Page"""
    st.markdown("## 📝 Report Generator")
    
    tabs = st.tabs(["📊 Quick Report", "📋 Custom Report", "📜 Report History"])
    
    with tabs[0]:
        st.markdown("### Quick Report Generator")
        
        col1, col2 = st.columns(2)
        
        with col1:
            report_type = st.selectbox("Report Type", [
                "Executive Summary",
                "Department Performance",
                "KPI Scorecard",
                "Work Items Status",
                "All Data Export"
            ])
            
            dept_filter = st.selectbox("Department", ["All Departments", "MDS", "SGS", "BMS", "IT"])
        
        with col2:
            date_range = st.date_input("Date Range", 
                                       value=[date.today() - timedelta(days=30), date.today()])
            
            export_format = st.selectbox("Export Format", ["Excel", "CSV", "JSON"])
        
        if st.button("🚀 Generate Report", type="primary"):
            with st.spinner("Generating report..."):
                # Build query based on selections
                dept_clause = f"AND dept_id = '{dept_filter}'" if dept_filter != "All Departments" else ""
                date_from = to_date_id(date_range[0])
                date_to = to_date_id(date_range[1])
                
                if report_type == "KPI Scorecard":
                    report_df = read_df(f"""
                        SELECT d.dept_name, k.kpi_name, 
                               AVG(f.actual_value) as avg_actual,
                               AVG(f.target_value) as avg_target,
                               AVG(f.actual_value / NULLIF(f.target_value, 0)) * 100 as achievement
                        FROM fact_kpi_data f
                        JOIN dim_department d ON f.dept_id = d.dept_id
                        JOIN dim_kpi k ON f.kpi_id = k.kpi_id
                        WHERE f.date_id BETWEEN {date_from} AND {date_to} {dept_clause}
                        GROUP BY d.dept_name, k.kpi_name
                        ORDER BY d.dept_name, k.kpi_name
                    """)
                elif report_type == "Work Items Status":
                    report_df = read_df(f"""
                        SELECT d.dept_name, w.work_title, w.status, w.priority,
                               w.progress_percent, w.risk_level, w.due_date_id
                        FROM fact_work_item w
                        JOIN dim_department d ON w.dept_id = d.dept_id
                        WHERE 1=1 {dept_clause}
                        ORDER BY d.dept_name, w.priority
                    """)
                else:
                    report_df = read_df(f"""
                        SELECT f.*, d.dept_name, k.kpi_name
                        FROM fact_kpi_data f
                        JOIN dim_department d ON f.dept_id = d.dept_id
                        JOIN dim_kpi k ON f.kpi_id = k.kpi_id
                        WHERE f.date_id BETWEEN {date_from} AND {date_to} {dept_clause}
                        ORDER BY f.date_id DESC
                    """)
                
                st.success("✅ Report generated!")
                
                # Preview
                st.markdown("#### Preview")
                st.dataframe(report_df.head(20), use_container_width=True)
                
                # Download
                st.markdown("#### Download")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(export_to_excel(report_df, f'report_{report_type}'), unsafe_allow_html=True)
                with col2:
                    st.markdown(export_to_csv(report_df, f'report_{report_type}'), unsafe_allow_html=True)
                with col3:
                    st.markdown(export_to_json(report_df, f'report_{report_type}'), unsafe_allow_html=True)
    
    with tabs[1]:
        st.markdown("### Custom Report Builder")
        st.info("🔧 Coming soon: Drag-and-drop report builder with custom fields and visualizations")
    
    with tabs[2]:
        st.markdown("### Report History")
        st.info("📜 Report history will be displayed here")

# ============================================================
# DATA IMPORT
# ============================================================
def render_data_import():
    """Render Data Import Page"""
    st.markdown("## 📥 Data Import")
    
    tabs = st.tabs(["📊 Excel/CSV", "📝 Manual Entry", "🔗 API Import"])
    
    with tabs[0]:
        st.markdown("### Import from Excel or CSV")
        
        uploaded_file = st.file_uploader("Choose file", type=['xlsx', 'xls', 'csv'])
        
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.success(f"✅ Loaded {len(df)} rows")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Import options
                col1, col2 = st.columns(2)
                
                with col1:
                    target_table = st.selectbox("Import to", [
                        "fact_kpi_data",
                        "fact_work_item",
                        "dim_department",
                        "dim_person",
                        "dim_kpi"
                    ])
                
                with col2:
                    import_mode = st.radio("Mode", ["Append", "Replace"], horizontal=True)
                
                if st.button("🚀 Import Data", type="primary"):
                    conn = get_conn()
                    try:
                        if import_mode == "Replace":
                            df.to_sql(target_table, conn, if_exists="replace", index=False)
                        else:
                            df.to_sql(target_table, conn, if_exists="append", index=False)
                        st.success(f"✅ Imported {len(df)} rows to {target_table}")
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()
            
            except Exception as e:
                st.error(f"Error reading file: {e}")
    
    with tabs[1]:
        st.markdown("### Manual KPI Entry")
        
        col1, col2 = st.columns(2)
        
        with col1:
            dept = st.selectbox("Department", ["MDS", "SGS", "BMS", "IT"])
            kpis = read_df(f"SELECT * FROM dim_kpi WHERE dept_id = '{dept}'")
            kpi = st.selectbox("KPI", kpis['kpi_name'].tolist() if not kpis.empty else [])
        
        with col2:
            entry_date = st.date_input("Date", value=date.today())
            actual_value = st.number_input("Actual Value", value=0.0)
            target_value = st.number_input("Target Value", value=0.0)
        
        if st.button("💾 Save Entry", type="primary"):
            if kpi:
                kpi_id = kpis.loc[kpis['kpi_name'] == kpi, 'kpi_id'].iloc[0]
                ensure_dim_date(entry_date, entry_date)
                
                exec_sql("""
                    INSERT INTO fact_kpi_data (record_id, date_id, dept_id, kpi_id, actual_value, target_value, created_ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (uid("KPI"), to_date_id(entry_date), dept, kpi_id, actual_value, target_value, datetime.now().isoformat()))
                
                st.success("✅ Entry saved!")
    
    with tabs[2]:
        st.markdown("### API Import")
        st.info("🔗 Configure API connections to automatically import data from external systems")

# ============================================================
# ADMIN PAGE
# ============================================================
def render_admin():
    """Render Admin Page"""
    st.markdown("## ⚙️ System Administration")
    
    tabs = st.tabs(["👥 Users", "📊 Master Data", "🔧 System"])
    
    with tabs[0]:
        st.markdown("### User Management")
        
        users = read_df("SELECT username, role, dept_id, is_enabled FROM dim_user")
        st.dataframe(users, use_container_width=True)
        
        st.markdown("#### Add New User")
        col1, col2 = st.columns(2)
        
        with col1:
            new_user = st.text_input("Username")
            new_pass = st.text_input("Password", type="password")
        
        with col2:
            new_role = st.selectbox("Role", ["Admin", "Executive", "DeptHead", "Staff"])
            new_dept = st.selectbox("Department", [None, "MDS", "SGS", "BMS", "IT"])
        
        if st.button("➕ Add User"):
            if new_user and new_pass:
                exec_sql("""
                    INSERT INTO dim_user (username, password_hash, role, dept_id, is_enabled)
                    VALUES (?, ?, ?, ?, 1)
                """, (new_user, sha256(new_pass), new_role, new_dept))
                st.success(f"✅ User '{new_user}' created!")
                st.rerun()
    
    with tabs[1]:
        st.markdown("### Master Data")
        
        master_table = st.selectbox("Select Table", ["dim_department", "dim_person", "dim_kpi"])
        
        data = read_df(f"SELECT * FROM {master_table}")
        edited_data = st.data_editor(data, use_container_width=True, num_rows="dynamic")
        
        if st.button("💾 Save Changes"):
            replace_table(master_table, edited_data)
            st.success("✅ Changes saved!")
    
    with tabs[2]:
        st.markdown("### System Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🌱 Seed Demo Data", type="secondary"):
                seed_demo_data()
                st.success("✅ Demo data seeded!")
                st.rerun()
        
        with col2:
            if st.button("🗑️ Reset Database", type="secondary"):
                if st.checkbox("I understand this will delete all data"):
                    DB_PATH.unlink(missing_ok=True)
                    st.success("✅ Database reset!")
                    st.rerun()

# ============================================================
# MAIN APPLICATION
# ============================================================
init_db()

if "auth" not in st.session_state:
    st.session_state["auth"] = {"logged_in": False}

# Sidebar
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <div style="font-size: 3rem;">🧠</div>
        <h2 style="margin: 0; background: linear-gradient(90deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            SMD Intelligence Hub
        </h2>
        <p style="color: #64748b; font-size: 0.8rem;">Enterprise Decision Platform v2.0</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Check if first run (no users)
    users_exist = not read_df("SELECT COUNT(*) as n FROM dim_user").iloc[0]['n'] == 0
    
    if not is_logged_in():
        if not users_exist:
            st.warning("ระบบยังไม่มีผู้ใช้ กรุณา Seed Demo Data ก่อน")
            if st.button("🌱 Seed Demo Data", type="primary"):
                seed_demo_data()
                st.success("✅ Demo data seeded! Login: admin/demo123")
                st.rerun()
        else:
            st.markdown("### 🔐 Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.button("Login", type="primary"):
                if login(username, password):
                    st.success("✅ Login successful!")
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials")
    else:
        user = current_user()
        st.success(f"👤 {user['username']} ({user['role']})")
        
        if st.button("🚪 Logout"):
            logout()
            st.rerun()
        
        st.markdown("---")
        
        # Navigation
        pages = ["📊 Executive Dashboard"]
        
        if user['role'] in ['Admin', 'Executive', 'DeptHead', 'Staff']:
            pages.append("💼 MDS Dashboard")
            pages.append("🧭 SGS Dashboard")
            pages.append("⚖️ BMS Dashboard")
            pages.append("🖥️ IT Dashboard")
        
        pages.extend(["📝 Report Generator", "📥 Data Import"])
        
        if user['role'] == 'Admin':
            pages.append("⚙️ Administration")
        
        page = st.radio("Navigation", pages)
        
        st.markdown("---")
        st.markdown(f"**🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}**")

# Main Content
if is_logged_in():
    user = current_user()
    
    if page == "📊 Executive Dashboard":
        render_executive_dashboard()
    elif page == "💼 MDS Dashboard":
        render_mds_dashboard()
    elif page == "🧭 SGS Dashboard":
        render_sgs_dashboard()
    elif page == "⚖️ BMS Dashboard":
        render_bms_dashboard()
    elif page == "🖥️ IT Dashboard":
        render_it_dashboard()
    elif page == "📝 Report Generator":
        render_report_generator()
    elif page == "📥 Data Import":
        render_data_import()
    elif page == "⚙️ Administration":
        if user['role'] == 'Admin':
            render_admin()
        else:
            st.error("Access denied")
else:
    st.markdown("""
    <div style="text-align: center; padding: 4rem 2rem;">
        <h1 style="font-size: 4rem;">🧠</h1>
        <h1 style="background: linear-gradient(90deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            SMD Intelligence Hub
        </h1>
        <p style="color: #94a3b8; font-size: 1.2rem;">Enterprise Decision Intelligence Platform</p>
        <p style="color: #64748b;">กรุณา Login ที่ Sidebar เพื่อเข้าใช้งานระบบ</p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #64748b; font-size: 0.8rem;">
    SMD Intelligence Hub v2.0 | Enterprise Decision Platform | © 2025
</div>
""", unsafe_allow_html=True)
