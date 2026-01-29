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
import numpy as np

# ============================================================
# SMD INTELLIGENCE HUB v2.5 - Enterprise Decision Platform
# ============================================================
# Features:
# - RBAC (Role-Based Access Control)
# - Department-Specific Dashboards with Custom Analytics
# - DATA WORKSPACE - Central Data Management & Analysis
# - Category Grouping & Trend Analysis
# - Automated Insight Generation
# - Advanced Report Generator
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
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
    }
    
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
    .metric-card.purple { border-left: 4px solid #8b5cf6; }
    
    .section-header {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.5rem;
        font-weight: bold;
        margin: 1rem 0;
    }
    
    .insight-card {
        background: rgba(59, 130, 246, 0.1);
        border: 1px solid #3b82f6;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .insight-card.warning { background: rgba(245, 158, 11, 0.1); border-color: #f59e0b; }
    .insight-card.danger { background: rgba(239, 68, 68, 0.1); border-color: #ef4444; }
    .insight-card.success { background: rgba(34, 197, 94, 0.1); border-color: #22c55e; }
    
    .data-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border: 1px solid #475569;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0;
        transition: all 0.3s ease;
    }
    
    .data-card:hover {
        border-color: #3b82f6;
        box-shadow: 0 4px 20px rgba(59, 130, 246, 0.2);
    }
    
    .category-tag {
        display: inline-block;
        background: rgba(139, 92, 246, 0.2);
        border: 1px solid #8b5cf6;
        border-radius: 20px;
        padding: 0.25rem 0.75rem;
        margin: 0.25rem;
        font-size: 0.8rem;
        color: #c4b5fd;
    }
    
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

def append_table(table, df):
    conn = get_conn()
    try:
        df.to_sql(table, conn, if_exists="append", index=False)
    finally:
        conn.close()

def uid(prefix=""):
    return f"{prefix}_{uuid.uuid4().hex[:10]}" if prefix else uuid.uuid4().hex[:12]

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
    
    # ========== DIMENSION TABLES ==========
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY, date TEXT NOT NULL, 
        month INTEGER, quarter INTEGER, year INTEGER, week INTEGER
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_department (
        dept_id TEXT PRIMARY KEY, dept_name TEXT NOT NULL,
        dept_code TEXT, description TEXT, color TEXT DEFAULT '#3b82f6'
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_person (
        person_id TEXT PRIMARY KEY, person_name TEXT NOT NULL,
        role TEXT, department TEXT, email TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_kpi (
        kpi_id TEXT PRIMARY KEY, kpi_name TEXT NOT NULL,
        kpi_definition TEXT, dept_id TEXT, unit TEXT,
        target_direction TEXT DEFAULT 'higher_is_better'
    )""")
    
    # ========== USER & RBAC ==========
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_user (
        username TEXT PRIMARY KEY, password_hash TEXT NOT NULL,
        role TEXT NOT NULL, dept_id TEXT, person_id TEXT,
        is_enabled INTEGER DEFAULT 1
    )""")
    
    # ========== FACT TABLES ==========
    cur.execute("""CREATE TABLE IF NOT EXISTS fact_kpi_data (
        record_id TEXT PRIMARY KEY, date_id INTEGER NOT NULL,
        dept_id TEXT NOT NULL, kpi_id TEXT NOT NULL,
        actual_value REAL, target_value REAL, created_ts TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS fact_work_item (
        work_id TEXT PRIMARY KEY, dept_id TEXT NOT NULL,
        work_title TEXT NOT NULL, work_type TEXT,
        priority TEXT, status TEXT, progress_percent REAL,
        start_date_id INTEGER, due_date_id INTEGER,
        risk_level TEXT, notes TEXT, created_ts TEXT, updated_ts TEXT
    )""")
    
    # ========== DATA WORKSPACE TABLES ==========
    
    # Dataset Catalog - เก็บข้อมูล metadata ของ datasets
    cur.execute("""CREATE TABLE IF NOT EXISTS workspace_datasets (
        dataset_id TEXT PRIMARY KEY,
        dataset_name TEXT NOT NULL,
        description TEXT,
        source_type TEXT,
        dept_id TEXT,
        row_count INTEGER DEFAULT 0,
        column_count INTEGER DEFAULT 0,
        columns_json TEXT,
        tags TEXT,
        created_by TEXT,
        created_ts TEXT,
        updated_ts TEXT
    )""")
    
    # Dataset Records - เก็บข้อมูลจริง (แบบ EAV - Entity-Attribute-Value)
    cur.execute("""CREATE TABLE IF NOT EXISTS workspace_data (
        data_id TEXT PRIMARY KEY,
        dataset_id TEXT NOT NULL,
        row_index INTEGER,
        data_json TEXT NOT NULL,
        created_ts TEXT,
        FOREIGN KEY (dataset_id) REFERENCES workspace_datasets(dataset_id)
    )""")
    
    # Categories - สำหรับ grouping
    cur.execute("""CREATE TABLE IF NOT EXISTS workspace_categories (
        category_id TEXT PRIMARY KEY,
        category_name TEXT NOT NULL,
        category_type TEXT,
        description TEXT,
        dept_id TEXT,
        parent_category_id TEXT,
        color TEXT DEFAULT '#8b5cf6',
        icon TEXT DEFAULT '📁',
        created_ts TEXT
    )""")
    
    # Category Mappings - mapping ระหว่าง data และ categories
    cur.execute("""CREATE TABLE IF NOT EXISTS workspace_category_mappings (
        mapping_id TEXT PRIMARY KEY,
        dataset_id TEXT NOT NULL,
        column_name TEXT NOT NULL,
        category_id TEXT NOT NULL,
        value_mapping_json TEXT,
        created_ts TEXT
    )""")
    
    # Analysis Templates - เก็บ template การวิเคราะห์
    cur.execute("""CREATE TABLE IF NOT EXISTS workspace_analysis_templates (
        template_id TEXT PRIMARY KEY,
        template_name TEXT NOT NULL,
        analysis_type TEXT,
        config_json TEXT,
        created_by TEXT,
        created_ts TEXT
    )""")
    
    # Saved Analysis Results
    cur.execute("""CREATE TABLE IF NOT EXISTS workspace_analysis_results (
        result_id TEXT PRIMARY KEY,
        analysis_name TEXT NOT NULL,
        dataset_id TEXT,
        analysis_type TEXT,
        parameters_json TEXT,
        result_json TEXT,
        created_by TEXT,
        created_ts TEXT
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
    ]))
    
    # KPIs
    replace_table("dim_kpi", pd.DataFrame([
        {"kpi_id": "MDS_K1", "kpi_name": "Lead Volume", "dept_id": "MDS", "unit": "leads", "target_direction": "higher_is_better"},
        {"kpi_id": "MDS_K2", "kpi_name": "Conversion Rate", "dept_id": "MDS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "MDS_K3", "kpi_name": "Pipeline Value", "dept_id": "MDS", "unit": "MB", "target_direction": "higher_is_better"},
        {"kpi_id": "SGS_K1", "kpi_name": "Strategy Progress", "dept_id": "SGS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "SGS_K2", "kpi_name": "Risk Score", "dept_id": "SGS", "unit": "score", "target_direction": "lower_is_better"},
        {"kpi_id": "BMS_K1", "kpi_name": "Compliance Score", "dept_id": "BMS", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "BMS_K2", "kpi_name": "Audit Findings", "dept_id": "BMS", "unit": "items", "target_direction": "lower_is_better"},
        {"kpi_id": "IT_K1", "kpi_name": "System Uptime", "dept_id": "IT", "unit": "%", "target_direction": "higher_is_better"},
        {"kpi_id": "IT_K2", "kpi_name": "Incident Count", "dept_id": "IT", "unit": "incidents", "target_direction": "lower_is_better"},
    ]))
    
    # Generate KPI Data
    import random
    kpi_data = []
    for i in range(90):
        d = today - timedelta(days=89-i)
        date_id = to_date_id(d)
        
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "MDS", "kpi_id": "MDS_K1",
                        "actual_value": random.randint(80, 150) + i*0.3, "target_value": 100, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "MDS", "kpi_id": "MDS_K2",
                        "actual_value": random.uniform(15, 30), "target_value": 25, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "MDS", "kpi_id": "MDS_K3",
                        "actual_value": random.uniform(50, 100) + i*0.5, "target_value": 80, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "SGS", "kpi_id": "SGS_K1",
                        "actual_value": min(45 + i*0.5 + random.uniform(-5, 5), 100), "target_value": 80, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "SGS", "kpi_id": "SGS_K2",
                        "actual_value": max(70 - i*0.2 + random.uniform(-5, 5), 20), "target_value": 40, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "BMS", "kpi_id": "BMS_K1",
                        "actual_value": min(85 + i*0.1 + random.uniform(-2, 2), 99), "target_value": 95, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "BMS", "kpi_id": "BMS_K2",
                        "actual_value": max(15 - i*0.1 + random.uniform(-2, 2), 2), "target_value": 5, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "IT", "kpi_id": "IT_K1",
                        "actual_value": random.uniform(99.0, 99.99), "target_value": 99.5, "created_ts": datetime.now().isoformat()})
        kpi_data.append({"record_id": uid("KPI"), "date_id": date_id, "dept_id": "IT", "kpi_id": "IT_K2",
                        "actual_value": random.randint(0, 5), "target_value": 2, "created_ts": datetime.now().isoformat()})
    
    replace_table("fact_kpi_data", pd.DataFrame(kpi_data))
    
    # Sample Categories for Data Workspace
    categories = [
        {"category_id": "CAT_REGION", "category_name": "Region", "category_type": "Geographic", 
         "description": "ภูมิภาค", "color": "#3b82f6", "icon": "🌍"},
        {"category_id": "CAT_PRODUCT", "category_name": "Product Type", "category_type": "Business",
         "description": "ประเภทผลิตภัณฑ์", "color": "#10b981", "icon": "📦"},
        {"category_id": "CAT_CUSTOMER", "category_name": "Customer Segment", "category_type": "Business",
         "description": "กลุ่มลูกค้า", "color": "#f59e0b", "icon": "👥"},
        {"category_id": "CAT_CHANNEL", "category_name": "Sales Channel", "category_type": "Business",
         "description": "ช่องทางการขาย", "color": "#8b5cf6", "icon": "🛒"},
        {"category_id": "CAT_PERIOD", "category_name": "Time Period", "category_type": "Temporal",
         "description": "ช่วงเวลา", "color": "#ef4444", "icon": "📅"},
    ]
    for cat in categories:
        cat["created_ts"] = datetime.now().isoformat()
    replace_table("workspace_categories", pd.DataFrame(categories))
    
    # Sample Dataset
    dataset_id = "DS_SALES_2024"
    replace_table("workspace_datasets", pd.DataFrame([{
        "dataset_id": dataset_id,
        "dataset_name": "Sales Data 2024",
        "description": "ข้อมูลยอดขายประจำปี 2024",
        "source_type": "Excel Import",
        "dept_id": "MDS",
        "row_count": 100,
        "column_count": 8,
        "columns_json": json.dumps(["date", "region", "product", "customer_segment", "channel", "quantity", "revenue", "cost"]),
        "tags": "sales,revenue,2024",
        "created_by": "admin",
        "created_ts": datetime.now().isoformat(),
        "updated_ts": datetime.now().isoformat()
    }]))
    
    # Generate sample sales data
    regions = ["North", "South", "East", "West", "Central"]
    products = ["Product A", "Product B", "Product C", "Product D"]
    segments = ["Enterprise", "SME", "Retail", "Government"]
    channels = ["Direct", "Online", "Partner", "Retail"]
    
    sales_data = []
    for i in range(100):
        d = today - timedelta(days=random.randint(0, 90))
        row_data = {
            "date": d.isoformat(),
            "region": random.choice(regions),
            "product": random.choice(products),
            "customer_segment": random.choice(segments),
            "channel": random.choice(channels),
            "quantity": random.randint(10, 500),
            "revenue": random.randint(50000, 500000),
            "cost": random.randint(30000, 300000)
        }
        sales_data.append({
            "data_id": uid("DATA"),
            "dataset_id": dataset_id,
            "row_index": i,
            "data_json": json.dumps(row_data),
            "created_ts": datetime.now().isoformat()
        })
    
    replace_table("workspace_data", pd.DataFrame(sales_data))
    
    # Users
    replace_table("dim_user", pd.DataFrame([
        {"username": "admin", "password_hash": sha256("demo123"), "role": "Admin", "dept_id": None, "is_enabled": 1},
        {"username": "executive", "password_hash": sha256("demo123"), "role": "Executive", "dept_id": None, "is_enabled": 1},
        {"username": "mds_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "MDS", "is_enabled": 1},
        {"username": "sgs_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "SGS", "is_enabled": 1},
        {"username": "bms_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "BMS", "is_enabled": 1},
        {"username": "it_head", "password_hash": sha256("demo123"), "role": "DeptHead", "dept_id": "IT", "is_enabled": 1},
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
    }
    return True

def logout():
    st.session_state["auth"] = {"logged_in": False}

def is_logged_in():
    return st.session_state.get("auth", {}).get("logged_in", False)

def current_user():
    return st.session_state.get("auth", {})

# ============================================================
# ANALYTICS FUNCTIONS
# ============================================================
def calculate_trend(series, periods=7):
    if len(series) < 2:
        return "stable", 0
    recent = series.tail(periods).mean()
    previous = series.head(len(series)-periods).mean() if len(series) > periods else series.iloc[0]
    if previous == 0:
        return "new", 0
    change = ((recent - previous) / previous) * 100
    if change > 5:
        return "up", change
    elif change < -5:
        return "down", change
    return "stable", change

def generate_insights(dept_id=None):
    insights = []
    today_id = to_date_id(date.today())
    week_ago_id = to_date_id(date.today() - timedelta(days=7))
    
    where_clause = f"AND k.dept_id = '{dept_id}'" if dept_id else ""
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, d.target_direction
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        WHERE k.date_id >= {week_ago_id} {where_clause}
        ORDER BY k.date_id DESC
    """)
    
    if kpi_df.empty:
        return insights
    
    for kpi_id in kpi_df['kpi_id'].unique():
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id].copy()
        kpi_name = kpi_data.iloc[0]['kpi_name']
        target_direction = kpi_data.iloc[0].get('target_direction', 'higher_is_better')
        
        latest = kpi_data.iloc[0]
        actual = latest['actual_value']
        target = latest['target_value']
        
        if target and actual:
            if target_direction == 'higher_is_better':
                if actual >= target:
                    insights.append({"type": "success", "icon": "✅", 
                                   "text": f"{kpi_name}: บรรลุเป้าหมาย ({actual:.1f} vs {target:.1f})", "severity": "low"})
                elif actual < target * 0.8:
                    insights.append({"type": "danger", "icon": "🚨",
                                   "text": f"{kpi_name}: ต่ำกว่าเป้าหมายมาก ({actual:.1f} vs {target:.1f})", "severity": "high"})
            else:
                if actual <= target:
                    insights.append({"type": "success", "icon": "✅",
                                   "text": f"{kpi_name}: บรรลุเป้าหมาย ({actual:.1f} vs {target:.1f})", "severity": "low"})
                elif actual > target * 1.5:
                    insights.append({"type": "danger", "icon": "🚨",
                                   "text": f"{kpi_name}: สูงกว่าเป้าหมายมาก ({actual:.1f} vs {target:.1f})", "severity": "high"})
    
    return insights

# ============================================================
# EXPORT FUNCTIONS
# ============================================================
def export_to_excel(df, filename):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
        worksheet = writer.sheets['Data']
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))
    b64 = base64.b64encode(output.getvalue()).decode()
    return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}.xlsx">📥 Download Excel</a>'

def export_to_csv(df, filename):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">📥 Download CSV</a>'

def export_to_json(df, filename):
    json_str = df.to_json(orient='records', date_format='iso', indent=2)
    b64 = base64.b64encode(json_str.encode()).decode()
    return f'<a href="data:file/json;base64,{b64}" download="{filename}.json">📥 Download JSON</a>'

# ============================================================
# VISUALIZATION HELPERS
# ============================================================
def create_kpi_card(title, value, target, unit="", trend_direction=None, trend_value=None):
    if target:
        ratio = value / target if target != 0 else 1
        status_class = "green" if ratio >= 1 else ("amber" if ratio >= 0.8 else "red")
        status_icon = "✅" if ratio >= 1 else ("⚠️" if ratio >= 0.8 else "❌")
    else:
        status_class = "blue"
        status_icon = "📊"
    
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
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x_col], y=df[y_col],
        mode='lines+markers',
        name='Actual',
        line=dict(color=color, width=2),
        marker=dict(size=6)
    ))
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
            'steps': [
                {'range': [0, target * 0.8], 'color': 'rgba(239, 68, 68, 0.3)'},
                {'range': [target * 0.8, target], 'color': 'rgba(245, 158, 11, 0.3)'},
                {'range': [target, max_val], 'color': 'rgba(34, 197, 94, 0.3)'}
            ],
            'threshold': {'line': {'color': '#ef4444', 'width': 4}, 'thickness': 0.75, 'value': target}
        }
    ))
    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8', height=250, margin=dict(l=20, r=20, t=50, b=20))
    return fig

# ============================================================
# DATA WORKSPACE - MAIN FEATURE
# ============================================================
def render_data_workspace():
    """Render the Data Workspace - Central hub for data management and analysis"""
    st.markdown("## 🗄️ Data Workspace")
    st.markdown("ศูนย์กลางจัดการข้อมูลและวิเคราะห์เพื่อสารสนเทศ")
    
    tabs = st.tabs([
        "📚 Dataset Catalog",
        "📤 Import Data", 
        "🏷️ Category Manager",
        "🔍 Data Explorer",
        "📊 Trend Analysis",
        "📈 Insights Generator"
    ])
    
    # ========== TAB 1: DATASET CATALOG ==========
    with tabs[0]:
        render_dataset_catalog()
    
    # ========== TAB 2: IMPORT DATA ==========
    with tabs[1]:
        render_data_import_workspace()
    
    # ========== TAB 3: CATEGORY MANAGER ==========
    with tabs[2]:
        render_category_manager()
    
    # ========== TAB 4: DATA EXPLORER ==========
    with tabs[3]:
        render_data_explorer()
    
    # ========== TAB 5: TREND ANALYSIS ==========
    with tabs[4]:
        render_trend_analysis()
    
    # ========== TAB 6: INSIGHTS GENERATOR ==========
    with tabs[5]:
        render_insights_generator()

def render_dataset_catalog():
    """Dataset Catalog - View and manage all datasets"""
    st.markdown("### 📚 Dataset Catalog")
    st.markdown("รายการ Datasets ทั้งหมดในระบบ")
    
    # Load datasets
    datasets = read_df("SELECT * FROM workspace_datasets ORDER BY updated_ts DESC")
    
    if datasets.empty:
        st.info("ยังไม่มี Dataset ในระบบ กรุณา Import ข้อมูลก่อน")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Datasets", len(datasets))
    with col2:
        total_rows = datasets['row_count'].sum()
        st.metric("Total Records", f"{total_rows:,}")
    with col3:
        depts = datasets['dept_id'].nunique()
        st.metric("Departments", depts)
    with col4:
        recent = len(datasets[pd.to_datetime(datasets['updated_ts']) > datetime.now() - timedelta(days=7)])
        st.metric("Updated This Week", recent)
    
    st.markdown("---")
    
    # Dataset Cards
    for _, ds in datasets.iterrows():
        cols = json.loads(ds['columns_json']) if ds['columns_json'] else []
        tags = ds['tags'].split(',') if ds['tags'] else []
        
        st.markdown(f"""
        <div class="data-card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <h3 style="color: white; margin: 0;">📊 {ds['dataset_name']}</h3>
                    <p style="color: #64748b; font-size: 0.9rem;">{ds['description'] or 'No description'}</p>
                </div>
                <div style="text-align: right;">
                    <span style="color: #22c55e; font-size: 0.8rem;">● Active</span>
                </div>
            </div>
            <div style="margin-top: 1rem; display: flex; gap: 2rem; color: #94a3b8; font-size: 0.85rem;">
                <span>📁 {ds['row_count']:,} rows</span>
                <span>📋 {ds['column_count']} columns</span>
                <span>🏢 {ds['dept_id'] or 'All'}</span>
                <span>📅 {ds['updated_ts'][:10] if ds['updated_ts'] else 'N/A'}</span>
            </div>
            <div style="margin-top: 0.5rem;">
                {''.join([f'<span class="category-tag">{tag.strip()}</span>' for tag in tags[:5]])}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Action buttons
        col1, col2, col3, col4 = st.columns([1,1,1,3])
        with col1:
            if st.button("🔍 View", key=f"view_{ds['dataset_id']}"):
                st.session_state['selected_dataset'] = ds['dataset_id']
                st.session_state['workspace_tab'] = 3  # Go to Explorer
        with col2:
            if st.button("📊 Analyze", key=f"analyze_{ds['dataset_id']}"):
                st.session_state['selected_dataset'] = ds['dataset_id']
                st.session_state['workspace_tab'] = 4  # Go to Trend
        with col3:
            if st.button("🗑️ Delete", key=f"delete_{ds['dataset_id']}"):
                exec_sql("DELETE FROM workspace_data WHERE dataset_id = ?", (ds['dataset_id'],))
                exec_sql("DELETE FROM workspace_datasets WHERE dataset_id = ?", (ds['dataset_id'],))
                st.success("Deleted!")
                st.rerun()
        
        st.markdown("---")

def render_data_import_workspace():
    """Import data to workspace"""
    st.markdown("### 📤 Import Data to Workspace")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        uploaded_file = st.file_uploader("Upload Excel or CSV", type=['xlsx', 'xls', 'csv'], key="workspace_upload")
        
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.success(f"✅ Loaded: {len(df)} rows, {len(df.columns)} columns")
                
                # Preview
                st.markdown("#### Preview")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Column info
                st.markdown("#### Column Information")
                col_info = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.astype(str),
                    'Non-Null': df.count().values,
                    'Unique': [df[col].nunique() for col in df.columns]
                })
                st.dataframe(col_info, use_container_width=True)
                
                st.markdown("---")
                
                # Dataset metadata
                st.markdown("#### Dataset Information")
                ds_name = st.text_input("Dataset Name *", value=uploaded_file.name.rsplit('.', 1)[0])
                ds_desc = st.text_area("Description", height=80)
                
                col_a, col_b = st.columns(2)
                with col_a:
                    ds_dept = st.selectbox("Department", ["All", "MDS", "SGS", "BMS", "IT"])
                with col_b:
                    ds_tags = st.text_input("Tags (comma separated)", placeholder="sales, 2024, monthly")
                
                if st.button("💾 Save to Workspace", type="primary"):
                    if ds_name:
                        dataset_id = uid("DS")
                        
                        # Save dataset metadata
                        exec_sql("""
                            INSERT INTO workspace_datasets 
                            (dataset_id, dataset_name, description, source_type, dept_id, 
                             row_count, column_count, columns_json, tags, created_by, created_ts, updated_ts)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (dataset_id, ds_name, ds_desc, "File Upload", 
                              ds_dept if ds_dept != "All" else None,
                              len(df), len(df.columns), json.dumps(df.columns.tolist()),
                              ds_tags, current_user().get('username', 'system'),
                              datetime.now().isoformat(), datetime.now().isoformat()))
                        
                        # Save data records
                        data_records = []
                        for idx, row in df.iterrows():
                            data_records.append({
                                "data_id": uid("DATA"),
                                "dataset_id": dataset_id,
                                "row_index": idx,
                                "data_json": json.dumps(row.to_dict(), default=str),
                                "created_ts": datetime.now().isoformat()
                            })
                        
                        append_table("workspace_data", pd.DataFrame(data_records))
                        
                        st.success(f"✅ Dataset '{ds_name}' saved with {len(df)} records!")
                        st.balloons()
                    else:
                        st.error("Please enter dataset name")
                        
            except Exception as e:
                st.error(f"Error: {e}")
    
    with col2:
        st.markdown("### 📋 Import Guidelines")
        st.info("""
        **Supported Formats:**
        - Excel (.xlsx, .xls)
        - CSV (.csv)
        
        **Best Practices:**
        - Headers in first row
        - Consistent data types per column
        - Date format: YYYY-MM-DD
        - No merged cells
        
        **Recommended Columns:**
        - Date/Time column for trends
        - Category columns for grouping
        - Numeric columns for analysis
        """)
        
        st.markdown("### 🏷️ Suggested Tags")
        st.markdown("""
        - `sales` - ข้อมูลยอดขาย
        - `finance` - ข้อมูลการเงิน
        - `hr` - ข้อมูลบุคลากร
        - `operations` - ข้อมูลปฏิบัติการ
        - `customers` - ข้อมูลลูกค้า
        """)

def render_category_manager():
    """Manage categories for data grouping"""
    st.markdown("### 🏷️ Category Manager")
    st.markdown("จัดการ Categories สำหรับจัดกลุ่มและวิเคราะห์ข้อมูล")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Existing categories
        categories = read_df("SELECT * FROM workspace_categories ORDER BY category_type, category_name")
        
        if not categories.empty:
            st.markdown("#### Existing Categories")
            
            # Group by type
            for cat_type in categories['category_type'].unique():
                st.markdown(f"**{cat_type}**")
                type_cats = categories[categories['category_type'] == cat_type]
                
                for _, cat in type_cats.iterrows():
                    col_a, col_b, col_c = st.columns([3, 1, 1])
                    with col_a:
                        st.markdown(f"""
                        <span style="color: {cat['color']};">{cat['icon']}</span> 
                        **{cat['category_name']}** - {cat['description'] or 'No description'}
                        """, unsafe_allow_html=True)
                    with col_b:
                        if st.button("✏️", key=f"edit_cat_{cat['category_id']}"):
                            st.session_state['edit_category'] = cat['category_id']
                    with col_c:
                        if st.button("🗑️", key=f"del_cat_{cat['category_id']}"):
                            exec_sql("DELETE FROM workspace_categories WHERE category_id = ?", (cat['category_id'],))
                            st.rerun()
                
                st.markdown("---")
        else:
            st.info("ยังไม่มี Categories กรุณาสร้างใหม่")
    
    with col2:
        st.markdown("#### ➕ Create New Category")
        
        with st.form("new_category"):
            cat_name = st.text_input("Category Name *")
            cat_type = st.selectbox("Type", ["Business", "Geographic", "Temporal", "Product", "Customer", "Other"])
            cat_desc = st.text_input("Description")
            cat_color = st.color_picker("Color", "#8b5cf6")
            cat_icon = st.selectbox("Icon", ["📁", "🏷️", "📊", "🌍", "👥", "📦", "💰", "📅", "🎯", "⭐"])
            cat_dept = st.selectbox("Department", [None, "MDS", "SGS", "BMS", "IT"])
            
            if st.form_submit_button("Create Category", type="primary"):
                if cat_name:
                    exec_sql("""
                        INSERT INTO workspace_categories 
                        (category_id, category_name, category_type, description, dept_id, color, icon, created_ts)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (uid("CAT"), cat_name, cat_type, cat_desc, cat_dept, cat_color, cat_icon, datetime.now().isoformat()))
                    st.success(f"✅ Category '{cat_name}' created!")
                    st.rerun()
                else:
                    st.error("Please enter category name")

def render_data_explorer():
    """Explore and analyze data in datasets"""
    st.markdown("### 🔍 Data Explorer")
    
    # Select dataset
    datasets = read_df("SELECT dataset_id, dataset_name FROM workspace_datasets ORDER BY dataset_name")
    
    if datasets.empty:
        st.info("ยังไม่มี Dataset กรุณา Import ข้อมูลก่อน")
        return
    
    selected_ds = st.selectbox("Select Dataset", datasets['dataset_name'].tolist())
    dataset_id = datasets.loc[datasets['dataset_name'] == selected_ds, 'dataset_id'].iloc[0]
    
    # Load data
    data_records = read_df("SELECT data_json FROM workspace_data WHERE dataset_id = ?", (dataset_id,))
    
    if data_records.empty:
        st.warning("Dataset is empty")
        return
    
    # Parse JSON to DataFrame
    df = pd.DataFrame([json.loads(row['data_json']) for _, row in data_records.iterrows()])
    
    st.markdown(f"**Total Records:** {len(df):,} | **Columns:** {len(df.columns)}")
    
    # Filters
    st.markdown("#### 🔧 Filters & Grouping")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Column filter
        filter_col = st.selectbox("Filter Column", ["None"] + df.columns.tolist())
        if filter_col != "None":
            unique_vals = df[filter_col].dropna().unique()
            filter_val = st.multiselect("Filter Values", unique_vals, default=list(unique_vals)[:5])
            if filter_val:
                df = df[df[filter_col].isin(filter_val)]
    
    with col2:
        # Group by
        group_col = st.selectbox("Group By", ["None"] + df.columns.tolist())
    
    with col3:
        # Aggregation
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        agg_col = st.selectbox("Aggregate Column", ["None"] + numeric_cols)
        agg_func = st.selectbox("Function", ["sum", "mean", "count", "min", "max"])
    
    st.markdown("---")
    
    # Display data
    tab1, tab2, tab3 = st.tabs(["📋 Table View", "📊 Chart View", "📈 Statistics"])
    
    with tab1:
        if group_col != "None" and agg_col != "None":
            grouped_df = df.groupby(group_col)[agg_col].agg(agg_func).reset_index()
            grouped_df.columns = [group_col, f'{agg_func}_{agg_col}']
            st.dataframe(grouped_df, use_container_width=True)
            
            # Export
            st.markdown(export_to_excel(grouped_df, f'grouped_{selected_ds}'), unsafe_allow_html=True)
        else:
            st.dataframe(df, use_container_width=True)
            st.markdown(export_to_excel(df, selected_ds), unsafe_allow_html=True)
    
    with tab2:
        if group_col != "None" and agg_col != "None":
            grouped_df = df.groupby(group_col)[agg_col].agg(agg_func).reset_index()
            grouped_df.columns = [group_col, f'{agg_func}_{agg_col}']
            
            chart_type = st.radio("Chart Type", ["Bar", "Pie", "Line"], horizontal=True)
            
            if chart_type == "Bar":
                fig = px.bar(grouped_df, x=group_col, y=f'{agg_func}_{agg_col}',
                            color=group_col, title=f'{agg_func.title()} of {agg_col} by {group_col}')
            elif chart_type == "Pie":
                fig = px.pie(grouped_df, values=f'{agg_func}_{agg_col}', names=group_col,
                            title=f'{agg_func.title()} of {agg_col} by {group_col}')
            else:
                fig = px.line(grouped_df, x=group_col, y=f'{agg_func}_{agg_col}',
                             title=f'{agg_func.title()} of {agg_col} by {group_col}')
            
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Select Group By and Aggregate columns to create chart")
    
    with tab3:
        st.markdown("#### 📈 Statistical Summary")
        
        # Numeric summary
        numeric_df = df.select_dtypes(include=[np.number])
        if not numeric_df.empty:
            st.markdown("**Numeric Columns**")
            st.dataframe(numeric_df.describe(), use_container_width=True)
        
        # Categorical summary
        cat_df = df.select_dtypes(include=['object'])
        if not cat_df.empty:
            st.markdown("**Categorical Columns**")
            cat_summary = pd.DataFrame({
                'Column': cat_df.columns,
                'Unique': [cat_df[col].nunique() for col in cat_df.columns],
                'Top Value': [cat_df[col].mode()[0] if len(cat_df[col].mode()) > 0 else 'N/A' for col in cat_df.columns],
                'Top Freq': [cat_df[col].value_counts().iloc[0] if len(cat_df[col].value_counts()) > 0 else 0 for col in cat_df.columns]
            })
            st.dataframe(cat_summary, use_container_width=True)

def render_trend_analysis():
    """Trend Analysis by Category"""
    st.markdown("### 📊 Trend Analysis by Category")
    st.markdown("วิเคราะห์แนวโน้มข้อมูลตาม Category Grouping")
    
    # Select dataset
    datasets = read_df("SELECT dataset_id, dataset_name FROM workspace_datasets ORDER BY dataset_name")
    
    if datasets.empty:
        st.info("ยังไม่มี Dataset กรุณา Import ข้อมูลก่อน")
        return
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        selected_ds = st.selectbox("Select Dataset", datasets['dataset_name'].tolist(), key="trend_ds")
        dataset_id = datasets.loc[datasets['dataset_name'] == selected_ds, 'dataset_id'].iloc[0]
    
    # Load data
    data_records = read_df("SELECT data_json FROM workspace_data WHERE dataset_id = ?", (dataset_id,))
    
    if data_records.empty:
        st.warning("Dataset is empty")
        return
    
    df = pd.DataFrame([json.loads(row['data_json']) for _, row in data_records.iterrows()])
    
    # Detect date column
    date_cols = []
    for col in df.columns:
        try:
            pd.to_datetime(df[col])
            date_cols.append(col)
        except:
            pass
    
    with col2:
        if date_cols:
            date_col = st.selectbox("Date Column", date_cols)
            df[date_col] = pd.to_datetime(df[date_col])
        else:
            st.warning("No date column detected. Please ensure your data has a date column.")
            return
    
    st.markdown("---")
    
    # Analysis Configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Category to group by
        cat_cols = df.select_dtypes(include=['object']).columns.tolist()
        if date_col in cat_cols:
            cat_cols.remove(date_col)
        group_category = st.selectbox("Group by Category", ["Overall"] + cat_cols)
    
    with col2:
        # Metric to analyze
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        metric_col = st.selectbox("Metric to Analyze", numeric_cols)
    
    with col3:
        # Time granularity
        granularity = st.selectbox("Time Granularity", ["Day", "Week", "Month", "Quarter"])
    
    # Prepare data
    df_sorted = df.sort_values(date_col)
    
    if granularity == "Day":
        df_sorted['period'] = df_sorted[date_col].dt.date
    elif granularity == "Week":
        df_sorted['period'] = df_sorted[date_col].dt.to_period('W').dt.start_time
    elif granularity == "Month":
        df_sorted['period'] = df_sorted[date_col].dt.to_period('M').dt.start_time
    else:
        df_sorted['period'] = df_sorted[date_col].dt.to_period('Q').dt.start_time
    
    # Aggregate
    if group_category == "Overall":
        trend_df = df_sorted.groupby('period')[metric_col].agg(['sum', 'mean', 'count']).reset_index()
        trend_df.columns = ['period', 'total', 'average', 'count']
    else:
        trend_df = df_sorted.groupby(['period', group_category])[metric_col].agg(['sum', 'mean', 'count']).reset_index()
        trend_df.columns = ['period', group_category, 'total', 'average', 'count']
    
    # Display charts
    st.markdown("---")
    
    chart_tabs = st.tabs(["📈 Trend Line", "📊 Comparison", "📋 Data Table", "💡 Insights"])
    
    with chart_tabs[0]:
        st.markdown("#### Trend Over Time")
        
        metric_type = st.radio("Show", ["Total (Sum)", "Average", "Count"], horizontal=True, key="trend_metric")
        y_col = 'total' if 'Total' in metric_type else ('average' if 'Average' in metric_type else 'count')
        
        if group_category == "Overall":
            fig = px.line(trend_df, x='period', y=y_col,
                         title=f'{metric_type} of {metric_col} over Time',
                         markers=True)
        else:
            fig = px.line(trend_df, x='period', y=y_col, color=group_category,
                         title=f'{metric_type} of {metric_col} by {group_category}',
                         markers=True)
        
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#94a3b8',
            xaxis=dict(showgrid=True, gridcolor='rgba(71, 85, 105, 0.3)'),
            yaxis=dict(showgrid=True, gridcolor='rgba(71, 85, 105, 0.3)'),
            height=450
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with chart_tabs[1]:
        st.markdown("#### Category Comparison")
        
        if group_category != "Overall":
            # Comparison bar chart
            comparison_df = df_sorted.groupby(group_category)[metric_col].agg(['sum', 'mean', 'count']).reset_index()
            comparison_df.columns = [group_category, 'Total', 'Average', 'Count']
            
            fig = px.bar(comparison_df, x=group_category, y='Total',
                        color=group_category,
                        title=f'Total {metric_col} by {group_category}')
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig, use_container_width=True)
            
            # Pie chart
            fig2 = px.pie(comparison_df, values='Total', names=group_category,
                         title=f'Distribution of {metric_col} by {group_category}')
            fig2.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Select a category to see comparison")
    
    with chart_tabs[2]:
        st.markdown("#### Trend Data Table")
        st.dataframe(trend_df, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(export_to_excel(trend_df, f'trend_{selected_ds}'), unsafe_allow_html=True)
        with col2:
            st.markdown(export_to_csv(trend_df, f'trend_{selected_ds}'), unsafe_allow_html=True)
        with col3:
            st.markdown(export_to_json(trend_df, f'trend_{selected_ds}'), unsafe_allow_html=True)
    
    with chart_tabs[3]:
        st.markdown("#### 💡 Automated Insights")
        
        # Generate insights
        insights = []
        
        # Overall trend
        if len(trend_df) >= 2:
            if group_category == "Overall":
                first_val = trend_df['total'].iloc[0]
                last_val = trend_df['total'].iloc[-1]
            else:
                first_val = trend_df.groupby('period')['total'].sum().iloc[0]
                last_val = trend_df.groupby('period')['total'].sum().iloc[-1]
            
            if first_val > 0:
                change = ((last_val - first_val) / first_val) * 100
                if change > 10:
                    insights.append({"type": "success", "icon": "📈", 
                                   "text": f"{metric_col} เพิ่มขึ้น {change:.1f}% จากช่วงแรกถึงปัจจุบัน"})
                elif change < -10:
                    insights.append({"type": "danger", "icon": "📉",
                                   "text": f"{metric_col} ลดลง {abs(change):.1f}% จากช่วงแรกถึงปัจจุบัน"})
                else:
                    insights.append({"type": "warning", "icon": "➡️",
                                   "text": f"{metric_col} คงที่ (เปลี่ยนแปลง {change:.1f}%)"})
        
        # Category insights
        if group_category != "Overall":
            cat_totals = df_sorted.groupby(group_category)[metric_col].sum()
            top_cat = cat_totals.idxmax()
            top_val = cat_totals.max()
            total_val = cat_totals.sum()
            
            insights.append({"type": "info", "icon": "🏆",
                           "text": f"{group_category} ที่มี {metric_col} สูงสุดคือ '{top_cat}' ({top_val:,.0f}, คิดเป็น {top_val/total_val*100:.1f}%)"})
            
            bottom_cat = cat_totals.idxmin()
            bottom_val = cat_totals.min()
            insights.append({"type": "warning", "icon": "⚠️",
                           "text": f"{group_category} ที่มี {metric_col} ต่ำสุดคือ '{bottom_cat}' ({bottom_val:,.0f}, คิดเป็น {bottom_val/total_val*100:.1f}%)"})
        
        # Display insights
        for insight in insights:
            st.markdown(f"""
            <div class="insight-card {insight['type']}">
                {insight['icon']} {insight['text']}
            </div>
            """, unsafe_allow_html=True)

def render_insights_generator():
    """Generate insights from data"""
    st.markdown("### 📈 Insights Generator")
    st.markdown("สร้างสารสนเทศจากข้อมูลอัตโนมัติ")
    
    # Select dataset
    datasets = read_df("SELECT dataset_id, dataset_name FROM workspace_datasets ORDER BY dataset_name")
    
    if datasets.empty:
        st.info("ยังไม่มี Dataset กรุณา Import ข้อมูลก่อน")
        return
    
    selected_ds = st.selectbox("Select Dataset", datasets['dataset_name'].tolist(), key="insight_ds")
    dataset_id = datasets.loc[datasets['dataset_name'] == selected_ds, 'dataset_id'].iloc[0]
    
    # Load data
    data_records = read_df("SELECT data_json FROM workspace_data WHERE dataset_id = ?", (dataset_id,))
    
    if data_records.empty:
        st.warning("Dataset is empty")
        return
    
    df = pd.DataFrame([json.loads(row['data_json']) for _, row in data_records.iterrows()])
    
    if st.button("🔮 Generate Insights", type="primary"):
        with st.spinner("Analyzing data..."):
            insights = []
            
            # Basic statistics
            st.markdown("#### 📊 Data Overview")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", f"{len(df):,}")
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                st.metric("Numeric Columns", len(numeric_cols))
            with col4:
                cat_cols = df.select_dtypes(include=['object']).columns
                st.metric("Category Columns", len(cat_cols))
            
            st.markdown("---")
            
            # Numeric insights
            st.markdown("#### 💰 Numeric Analysis")
            numeric_df = df.select_dtypes(include=[np.number])
            
            for col in numeric_df.columns[:5]:  # Top 5 numeric columns
                total = numeric_df[col].sum()
                avg = numeric_df[col].mean()
                max_val = numeric_df[col].max()
                min_val = numeric_df[col].min()
                
                st.markdown(f"""
                <div class="metric-card blue">
                    <h4 style="color: white; margin: 0;">{col}</h4>
                    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-top: 0.5rem;">
                        <div><span style="color: #64748b;">Total</span><br><strong style="color: white;">{total:,.2f}</strong></div>
                        <div><span style="color: #64748b;">Average</span><br><strong style="color: white;">{avg:,.2f}</strong></div>
                        <div><span style="color: #64748b;">Max</span><br><strong style="color: white;">{max_val:,.2f}</strong></div>
                        <div><span style="color: #64748b;">Min</span><br><strong style="color: white;">{min_val:,.2f}</strong></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Categorical insights
            st.markdown("#### 🏷️ Category Distribution")
            cat_df = df.select_dtypes(include=['object'])
            
            cols = st.columns(min(3, len(cat_df.columns)))
            for i, col in enumerate(cat_df.columns[:3]):
                with cols[i]:
                    value_counts = cat_df[col].value_counts().head(5)
                    fig = px.pie(values=value_counts.values, names=value_counts.index, title=col)
                    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8', height=300)
                    st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            # Key findings
            st.markdown("#### 💡 Key Findings")
            
            findings = []
            
            # Find correlations if multiple numeric columns
            if len(numeric_cols) >= 2:
                corr = numeric_df.corr()
                for i in range(len(corr.columns)):
                    for j in range(i+1, len(corr.columns)):
                        if abs(corr.iloc[i, j]) > 0.7:
                            findings.append({
                                "type": "info",
                                "icon": "🔗",
                                "text": f"พบความสัมพันธ์สูงระหว่าง {corr.columns[i]} และ {corr.columns[j]} (r = {corr.iloc[i, j]:.2f})"
                            })
            
            # Find outliers
            for col in numeric_cols[:3]:
                q1 = numeric_df[col].quantile(0.25)
                q3 = numeric_df[col].quantile(0.75)
                iqr = q3 - q1
                outliers = numeric_df[(numeric_df[col] < q1 - 1.5*iqr) | (numeric_df[col] > q3 + 1.5*iqr)]
                if len(outliers) > 0:
                    findings.append({
                        "type": "warning",
                        "icon": "⚠️",
                        "text": f"พบข้อมูลผิดปกติ (Outliers) ใน {col}: {len(outliers)} รายการ ({len(outliers)/len(df)*100:.1f}%)"
                    })
            
            # Missing values
            missing = df.isnull().sum()
            for col in missing[missing > 0].index:
                findings.append({
                    "type": "warning",
                    "icon": "❓",
                    "text": f"คอลัมน์ {col} มีข้อมูลว่าง {missing[col]} รายการ ({missing[col]/len(df)*100:.1f}%)"
                })
            
            if not findings:
                findings.append({
                    "type": "success",
                    "icon": "✅",
                    "text": "ข้อมูลมีคุณภาพดี ไม่พบปัญหาที่ต้องแก้ไข"
                })
            
            for finding in findings:
                st.markdown(f"""
                <div class="insight-card {finding['type']}">
                    {finding['icon']} {finding['text']}
                </div>
                """, unsafe_allow_html=True)

# ============================================================
# DEPARTMENT DASHBOARDS
# ============================================================
def render_dept_dashboard(dept_id, dept_name, color):
    """Generic department dashboard renderer"""
    st.markdown(f"## {dept_name} Dashboard")
    
    today_id = to_date_id(date.today())
    thirty_days_ago = to_date_id(date.today() - timedelta(days=30))
    
    kpi_df = read_df(f"""
        SELECT k.*, d.kpi_name, dim.date
        FROM fact_kpi_data k
        JOIN dim_kpi d ON k.kpi_id = d.kpi_id
        JOIN dim_date dim ON k.date_id = dim.date_id
        WHERE k.dept_id = '{dept_id}' AND k.date_id >= {thirty_days_ago}
        ORDER BY k.date_id
    """)
    
    if kpi_df.empty:
        st.info("No KPI data available")
        return
    
    # KPI Cards
    kpi_ids = kpi_df['kpi_id'].unique()
    cols = st.columns(len(kpi_ids))
    
    for i, kpi_id in enumerate(kpi_ids):
        kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id]
        if not kpi_data.empty:
            latest = kpi_data.iloc[-1]
            trend, change = calculate_trend(kpi_data['actual_value'])
            
            with cols[i]:
                st.markdown(create_kpi_card(
                    latest['kpi_name'],
                    latest['actual_value'],
                    latest['target_value'],
                    trend_direction=trend,
                    trend_value=change
                ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        for kpi_id in kpi_ids[:1]:
            kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_id]
            if not kpi_data.empty:
                fig = create_trend_chart(kpi_data, 'date', 'actual_value', 
                                        f"{kpi_data.iloc[0]['kpi_name']} Trend", color)
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if len(kpi_ids) > 1:
            kpi_data = kpi_df[kpi_df['kpi_id'] == kpi_ids[1]]
            if not kpi_data.empty:
                latest = kpi_data.iloc[-1]
                fig = create_gauge_chart(latest['actual_value'], latest['target_value'], 
                                        kpi_data.iloc[0]['kpi_name'])
                st.plotly_chart(fig, use_container_width=True)
    
    # Insights
    st.markdown("### 💡 Insights")
    insights = generate_insights(dept_id)
    for insight in insights[:5]:
        st.markdown(f"""
        <div class="insight-card {insight['type']}">
            {insight['icon']} {insight['text']}
        </div>
        """, unsafe_allow_html=True)

def render_executive_dashboard():
    """Executive Dashboard"""
    st.markdown("## 📊 Executive Dashboard")
    
    depts = read_df("SELECT * FROM dim_department")
    
    cols = st.columns(4)
    for i, (_, dept) in enumerate(depts.iterrows()):
        with cols[i]:
            kpi_summary = read_df(f"""
                SELECT AVG(actual_value / NULLIF(target_value, 0)) as achievement
                FROM fact_kpi_data WHERE dept_id = '{dept['dept_id']}'
                AND date_id = (SELECT MAX(date_id) FROM fact_kpi_data WHERE dept_id = '{dept['dept_id']}')
            """)
            achievement = kpi_summary.iloc[0]['achievement'] * 100 if not kpi_summary.empty and kpi_summary.iloc[0]['achievement'] else 0
            status = "green" if achievement >= 100 else ("amber" if achievement >= 80 else "red")
            
            st.markdown(f"""
            <div class="metric-card {status}">
                <h4 style="color: white;">{dept['dept_name']}</h4>
                <div style="font-size: 2rem; font-weight: bold; color: {dept['color']};">{achievement:.0f}%</div>
                <small style="color: #64748b;">KPI Achievement</small>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Insights
    st.markdown("### 💡 Organization Insights")
    insights = generate_insights()
    for insight in insights[:6]:
        st.markdown(f"""
        <div class="insight-card {insight['type']}">
            {insight['icon']} {insight['text']}
        </div>
        """, unsafe_allow_html=True)

def render_report_generator():
    """Report Generator"""
    st.markdown("## 📝 Report Generator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        report_type = st.selectbox("Report Type", ["KPI Scorecard", "Department Performance", "All Data"])
        dept_filter = st.selectbox("Department", ["All", "MDS", "SGS", "BMS", "IT"])
    
    with col2:
        date_range = st.date_input("Date Range", [date.today() - timedelta(days=30), date.today()])
    
    if st.button("🚀 Generate", type="primary"):
        dept_clause = f"AND dept_id = '{dept_filter}'" if dept_filter != "All" else ""
        date_from = to_date_id(date_range[0])
        date_to = to_date_id(date_range[1])
        
        report_df = read_df(f"""
            SELECT d.dept_name, k.kpi_name, 
                   AVG(f.actual_value) as avg_actual,
                   AVG(f.target_value) as avg_target
            FROM fact_kpi_data f
            JOIN dim_department d ON f.dept_id = d.dept_id
            JOIN dim_kpi k ON f.kpi_id = k.kpi_id
            WHERE f.date_id BETWEEN {date_from} AND {date_to} {dept_clause}
            GROUP BY d.dept_name, k.kpi_name
        """)
        
        st.success("✅ Generated!")
        st.dataframe(report_df, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(export_to_excel(report_df, 'report'), unsafe_allow_html=True)
        with col2:
            st.markdown(export_to_csv(report_df, 'report'), unsafe_allow_html=True)
        with col3:
            st.markdown(export_to_json(report_df, 'report'), unsafe_allow_html=True)

def render_admin():
    """Admin Page"""
    st.markdown("## ⚙️ Administration")
    
    tabs = st.tabs(["👥 Users", "🌱 System"])
    
    with tabs[0]:
        users = read_df("SELECT username, role, dept_id, is_enabled FROM dim_user")
        st.dataframe(users, use_container_width=True)
        
        st.markdown("#### Add User")
        col1, col2 = st.columns(2)
        with col1:
            new_user = st.text_input("Username")
            new_pass = st.text_input("Password", type="password")
        with col2:
            new_role = st.selectbox("Role", ["Admin", "Executive", "DeptHead", "Staff"])
            new_dept = st.selectbox("Department", [None, "MDS", "SGS", "BMS", "IT"])
        
        if st.button("➕ Add"):
            if new_user and new_pass:
                exec_sql("INSERT INTO dim_user VALUES (?, ?, ?, ?, ?, 1)",
                        (new_user, sha256(new_pass), new_role, new_dept, None))
                st.success("Added!")
                st.rerun()
    
    with tabs[1]:
        if st.button("🌱 Seed Demo Data"):
            seed_demo_data()
            st.success("Done!")
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
        <p style="color: #64748b; font-size: 0.8rem;">Enterprise Decision Platform v2.5</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    users_exist = read_df("SELECT COUNT(*) as n FROM dim_user").iloc[0]['n'] > 0
    
    if not is_logged_in():
        if not users_exist:
            st.warning("No users. Seed demo data first.")
            if st.button("🌱 Seed Demo Data", type="primary"):
                seed_demo_data()
                st.success("Done! Login: admin/demo123")
                st.rerun()
        else:
            st.markdown("### 🔐 Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.button("Login", type="primary"):
                if login(username, password):
                    st.rerun()
                else:
                    st.error("Invalid credentials")
    else:
        user = current_user()
        st.success(f"👤 {user['username']} ({user['role']})")
        
        if st.button("🚪 Logout"):
            logout()
            st.rerun()
        
        st.markdown("---")
        
        # Navigation
        pages = [
            "📊 Executive Dashboard",
            "🗄️ Data Workspace",
            "💼 MDS Dashboard",
            "🧭 SGS Dashboard", 
            "⚖️ BMS Dashboard",
            "🖥️ IT Dashboard",
            "📝 Report Generator"
        ]
        
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
    elif page == "🗄️ Data Workspace":
        render_data_workspace()
    elif page == "💼 MDS Dashboard":
        render_dept_dashboard("MDS", "💼 MDS - Marketing & Sales", "#06b6d4")
    elif page == "🧭 SGS Dashboard":
        render_dept_dashboard("SGS", "🧭 SGS - Strategy & Planning", "#f59e0b")
    elif page == "⚖️ BMS Dashboard":
        render_dept_dashboard("BMS", "⚖️ BMS - Governance", "#10b981")
    elif page == "🖥️ IT Dashboard":
        render_dept_dashboard("IT", "🖥️ IT Operations", "#8b5cf6")
    elif page == "📝 Report Generator":
        render_report_generator()
    elif page == "⚙️ Administration":
        render_admin()
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

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #64748b; font-size: 0.8rem;">
    SMD Intelligence Hub v2.5 | Enterprise Decision Platform | © 2025
</div>
""", unsafe_allow_html=True)
