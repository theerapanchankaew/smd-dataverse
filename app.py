import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta
import uuid
import os
import hashlib

# =========================================================
# Intelligence Hub (MVP++) - RBAC + Meeting → Action Items
# =========================================================

st.set_page_config(
    page_title="Intelligence Hub (MVP++)",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path("intelligence_hub.db")
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def exec_sql(sql: str, params=None) -> None:
    conn = get_conn()
    cur = conn.cursor()
    try:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()

def read_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        if params is None:
            return pd.read_sql_query(sql, conn)
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

def replace_table(table: str, df: pd.DataFrame) -> None:
    conn = get_conn()
    try:
        df.to_sql(table, conn, if_exists="replace", index=False)
    finally:
        conn.close()

def ensure_dim_date(start: date, end: date) -> None:
    conn = get_conn()
    cur = conn.cursor()
    d = start
    while d <= end:
        date_id = int(d.strftime("%Y%m%d"))
        cur.execute("""
            INSERT OR IGNORE INTO dim_date(date_id, date, month, quarter, year)
            VALUES (?, ?, ?, ?, ?)
        """, (date_id, d.isoformat(), d.month, (d.month - 1)//3 + 1, d.year))
        d += timedelta(days=1)
    conn.commit()
    conn.close()

def to_date_id(d: date) -> int:
    return int(d.strftime("%Y%m%d"))

def uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Dimensions
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_date (date_id INTEGER PRIMARY KEY, date TEXT NOT NULL, month INTEGER, quarter INTEGER, year INTEGER)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_strategy (strategy_id TEXT PRIMARY KEY, strategy_name TEXT NOT NULL, strategy_owner TEXT, strategy_level TEXT, start_year INTEGER, end_year INTEGER)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_kpi (kpi_id TEXT PRIMARY KEY, kpi_name TEXT NOT NULL, kpi_definition TEXT, calculation_logic TEXT, kpi_owner TEXT, refresh_frequency TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_person (person_id TEXT PRIMARY KEY, person_name TEXT NOT NULL, role TEXT, department TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_organization (organization_id TEXT PRIMARY KEY, organization_name TEXT NOT NULL, org_type TEXT, sector TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_department (dept_id TEXT PRIMARY KEY, dept_name TEXT NOT NULL, dept_head_person_id TEXT, description TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS dim_work_type (work_type_id TEXT PRIMARY KEY, work_type_name TEXT NOT NULL)""")

    # Users for RBAC
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_user (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        dept_id TEXT,
        person_id TEXT,
        is_enabled INTEGER NOT NULL DEFAULT 1
    )
    """)

    # Facts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_strategic_kpi (
        kpi_fact_id TEXT PRIMARY KEY,
        date_id INTEGER NOT NULL,
        strategy_id TEXT NOT NULL,
        organization_id TEXT NOT NULL,
        kpi_id TEXT NOT NULL,
        actual_value REAL,
        target_value REAL,
        variance_value REAL,
        kpi_status TEXT,
        last_updated_ts TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_dept_work_item (
        work_id TEXT PRIMARY KEY,
        dept_id TEXT NOT NULL,
        organization_id TEXT NOT NULL,
        strategy_id TEXT,
        kpi_id TEXT,
        owner_person_id TEXT,
        work_title TEXT NOT NULL,
        work_type_id TEXT,
        priority TEXT,
        status TEXT,
        start_date_id INTEGER,
        due_date_id INTEGER,
        progress_percent REAL,
        risk_level TEXT,
        decision_needed INTEGER,
        notes TEXT,
        last_updated_ts TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_work_update (
        update_id TEXT PRIMARY KEY,
        work_id TEXT NOT NULL,
        date_id INTEGER NOT NULL,
        progress_percent REAL,
        update_text TEXT,
        blockers TEXT,
        decision_needed INTEGER,
        created_ts TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_work_evidence (
        evidence_id TEXT PRIMARY KEY,
        work_id TEXT NOT NULL,
        date_id INTEGER NOT NULL,
        file_name TEXT NOT NULL,
        stored_path TEXT NOT NULL,
        note TEXT,
        uploaded_ts TEXT
    )
    """)

    # Meetings + Action items
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_meeting (
        meeting_id TEXT PRIMARY KEY,
        meeting_date_id INTEGER NOT NULL,
        meeting_title TEXT NOT NULL,
        meeting_type TEXT,
        organizer_person_id TEXT,
        minutes_text TEXT,
        created_ts TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_meeting_decision (
        decision_id TEXT PRIMARY KEY,
        meeting_id TEXT NOT NULL,
        decision_text TEXT NOT NULL,
        decision_owner_person_id TEXT,
        created_ts TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_meeting_action_item (
        action_id TEXT PRIMARY KEY,
        meeting_id TEXT NOT NULL,
        decision_id TEXT,
        dept_id TEXT,
        owner_person_id TEXT,
        action_title TEXT NOT NULL,
        status TEXT,
        start_date_id INTEGER,
        due_date_id INTEGER,
        progress_percent REAL,
        blockers TEXT,
        decision_needed INTEGER,
        linked_work_id TEXT,
        last_updated_ts TEXT
    )
    """)

    conn.commit()
    conn.close()

# -------------------------
# RBAC helpers
# -------------------------
def is_logged_in() -> bool:
    return bool(st.session_state.get("auth", {}).get("logged_in"))

def current_user():
    return st.session_state.get("auth", {})

def require_login():
    if not is_logged_in():
        st.warning("กรุณาเข้าสู่ระบบก่อน")
        st.stop()

def can_view(page: str) -> bool:
    role = current_user().get("role")
    if role == "Admin":
        return True
    if page in ["Executive View", "Meeting → Action Items"]:
        return role in ["Executive", "Admin"]
    if page == "Department Workspace":
        return role in ["DeptHead", "Staff", "Admin", "Executive"]
    if page in ["Data Import", "Admin / User Management", "Schema / Templates"]:
        return role in ["Admin"]
    return False

def allowed_dept_scope():
    role = current_user().get("role")
    if role in ["Admin", "Executive"]:
        return None
    return current_user().get("dept_id")

TABLE_COLUMNS = {
    "dim_department": ["dept_id","dept_name","dept_head_person_id","description"],
    "dim_work_type": ["work_type_id","work_type_name"],
    "dim_user": ["username","password_hash","role","dept_id","person_id","is_enabled"],
    "dim_person": ["person_id","person_name","role","department"],
    "dim_strategy": ["strategy_id","strategy_name","strategy_owner","strategy_level","start_year","end_year"],
    "dim_kpi": ["kpi_id","kpi_name","kpi_definition","calculation_logic","kpi_owner","refresh_frequency"],
    "dim_organization": ["organization_id","organization_name","org_type","sector"],
    "fact_dept_work_item": ["work_id","dept_id","organization_id","strategy_id","kpi_id","owner_person_id","work_title","work_type_id","priority","status","start_date_id","due_date_id","progress_percent","risk_level","decision_needed","notes","last_updated_ts"],
    "fact_work_update": ["update_id","work_id","date_id","progress_percent","update_text","blockers","decision_needed","created_ts"],
    "fact_work_evidence": ["evidence_id","work_id","date_id","file_name","stored_path","note","uploaded_ts"],
    "fact_meeting": ["meeting_id","meeting_date_id","meeting_title","meeting_type","organizer_person_id","minutes_text","created_ts"],
    "fact_meeting_decision": ["decision_id","meeting_id","decision_text","decision_owner_person_id","created_ts"],
    "fact_meeting_action_item": ["action_id","meeting_id","decision_id","dept_id","owner_person_id","action_title","status","start_date_id","due_date_id","progress_percent","blockers","decision_needed","linked_work_id","last_updated_ts"],
    "fact_strategic_kpi": ["kpi_fact_id","date_id","strategy_id","organization_id","kpi_id","actual_value","target_value","variance_value","kpi_status","last_updated_ts"],
}

def make_template_csv(table: str) -> bytes:
    df = pd.DataFrame(columns=TABLE_COLUMNS[table])
    return df.to_csv(index=False).encode("utf-8")

def seed_demo_data() -> None:
    today = date.today()
    ensure_dim_date(today - timedelta(days=365), today + timedelta(days=90))

    replace_table("dim_organization", pd.DataFrame([{
        "organization_id":"ORG1","organization_name":"MASCI (HQ)","org_type":"Foundation","sector":"Certification"
    }]))
    replace_table("dim_person", pd.DataFrame([
        {"person_id":"P1","person_name":"Director","role":"Director","department":"Management"},
        {"person_id":"P2","person_name":"Head Ops","role":"DeptHead","department":"Operations"},
        {"person_id":"P3","person_name":"Head Strategy","role":"DeptHead","department":"Strategy"},
        {"person_id":"P4","person_name":"Head Governance","role":"DeptHead","department":"Governance"},
        {"person_id":"P5","person_name":"Staff Ops","role":"Staff","department":"Operations"},
    ]))
    replace_table("dim_department", pd.DataFrame([
        {"dept_id":"D1","dept_name":"Operations","dept_head_person_id":"P2","description":"งานปฏิบัติการ/บริการ"},
        {"dept_id":"D2","dept_name":"Strategy","dept_head_person_id":"P3","description":"งานยุทธศาสตร์/แผน"},
        {"dept_id":"D3","dept_name":"Governance","dept_head_person_id":"P4","description":"งานกำกับดูแล/ความเสี่ยง"},
    ]))
    replace_table("dim_work_type", pd.DataFrame([
        {"work_type_id":"WT1","work_type_name":"Routine"},
        {"work_type_id":"WT2","work_type_name":"Project"},
        {"work_type_id":"WT3","work_type_name":"Improvement"},
        {"work_type_id":"WT4","work_type_name":"Risk Mitigation"},
        {"work_type_id":"WT5","work_type_name":"Meeting Action"},
    ]))
    replace_table("dim_strategy", pd.DataFrame([
        {"strategy_id":"S1","strategy_name":"ยกระดับคุณภาพบริการรับรอง","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
        {"strategy_id":"S3","strategy_name":"การบริหารความเสี่ยง/ธรรมาภิบาล","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
    ]))
    replace_table("dim_kpi", pd.DataFrame([
        {"kpi_id":"K1","kpi_name":"On-time Delivery","kpi_definition":"งานส่งมอบตามกำหนด","calculation_logic":"on_time/total","kpi_owner":"Strategy","refresh_frequency":"Weekly"},
        {"kpi_id":"K3","kpi_name":"Compliance Findings","kpi_definition":"ประเด็นไม่สอดคล้อง","calculation_logic":"count(findings)","kpi_owner":"Governance","refresh_frequency":"Monthly"},
    ]))

    replace_table("dim_user", pd.DataFrame([
        {"username":"admin","password_hash":sha256("demo123"),"role":"Admin","dept_id":None,"person_id":"P1","is_enabled":1},
        {"username":"director","password_hash":sha256("demo123"),"role":"Executive","dept_id":None,"person_id":"P1","is_enabled":1},
        {"username":"ops_head","password_hash":sha256("demo123"),"role":"DeptHead","dept_id":"D1","person_id":"P2","is_enabled":1},
        {"username":"ops_staff","password_hash":sha256("demo123"),"role":"Staff","dept_id":"D1","person_id":"P5","is_enabled":1},
        {"username":"gov_head","password_hash":sha256("demo123"),"role":"DeptHead","dept_id":"D3","person_id":"P4","is_enabled":1},
    ]))

    d0 = to_date_id(today)
    replace_table("fact_strategic_kpi", pd.DataFrame([
        {"kpi_fact_id":"F1","date_id":d0,"strategy_id":"S1","organization_id":"ORG1","kpi_id":"K1","actual_value":0.91,"target_value":0.95,"variance_value":-0.04,"kpi_status":"Amber","last_updated_ts":datetime.now().isoformat()},
        {"kpi_fact_id":"F2","date_id":d0,"strategy_id":"S3","organization_id":"ORG1","kpi_id":"K3","actual_value":12,"target_value":8,"variance_value":4,"kpi_status":"Red","last_updated_ts":datetime.now().isoformat()},
    ]))

    meeting_id = "MTG_1"
    exec_sql("""INSERT OR REPLACE INTO fact_meeting VALUES (?, ?, ?, ?, ?, ?, ?)""",
             (meeting_id, d0, "คกก.บริหารประจำเดือน", "Committee", "P1", "สรุปการประชุมฉบับย่อ...", datetime.now().isoformat()))
    decision_id = "DEC_1"
    exec_sql("""INSERT OR REPLACE INTO fact_meeting_decision VALUES (?, ?, ?, ?, ?)""",
             (decision_id, meeting_id, "เร่งลด compliance findings ภายใน 30 วัน", "P1", datetime.now().isoformat()))
    exec_sql("""INSERT OR REPLACE INTO fact_meeting_action_item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
             ("ACT_1", meeting_id, decision_id, "D3", "P4", "จัดทำแผนปิด findings + ติดตามหลักฐาน", "In Progress",
              d0, d0 + 30, 20, "รอข้อมูลจากหลายฝ่าย", 1, None, datetime.now().isoformat()))

def login(username: str, password: str) -> bool:
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

def load_exec_data(as_of_id: int, org_id: str):
    kpi = read_df(f"""
        SELECT f.*, k.kpi_name, s.strategy_name
        FROM fact_strategic_kpi f
        LEFT JOIN dim_kpi k ON f.kpi_id = k.kpi_id
        LEFT JOIN dim_strategy s ON f.strategy_id = s.strategy_id
        WHERE f.date_id = {as_of_id} AND f.organization_id = '{org_id}'
    """)
    dept_work = read_df(f"""
        SELECT w.*, d.dept_name, p.person_name, wt.work_type_name
        FROM fact_dept_work_item w
        LEFT JOIN dim_department d ON w.dept_id = d.dept_id
        LEFT JOIN dim_person p ON w.owner_person_id = p.person_id
        LEFT JOIN dim_work_type wt ON w.work_type_id = wt.work_type_id
        WHERE w.organization_id = '{org_id}'
    """)
    actions = read_df("""
        SELECT a.*, d.dept_name, p.person_name, m.meeting_title
        FROM fact_meeting_action_item a
        LEFT JOIN dim_department d ON a.dept_id = d.dept_id
        LEFT JOIN dim_person p ON a.owner_person_id = p.person_id
        LEFT JOIN fact_meeting m ON a.meeting_id = m.meeting_id
        ORDER BY a.last_updated_ts DESC
    """)
    return kpi, dept_work, actions

# -------------------------
# App start
# -------------------------
init_db()
if "auth" not in st.session_state:
    st.session_state["auth"] = {"logged_in": False}

with st.sidebar:
    st.title("🧠 Intelligence Hub")
    st.caption("MVP++ • RBAC • Meetings • Department Work")
    st.divider()

    if not is_logged_in():
        st.subheader("เข้าสู่ระบบ")
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("Login", type="primary"):
            if login(u.strip(), p):
                st.success("เข้าสู่ระบบสำเร็จ")
                st.rerun()
            else:
                st.error("Username/Password ไม่ถูกต้อง หรือบัญชีถูกปิดใช้งาน")
        st.info("เดโม: กด Seed (admin) แล้วใช้ admin/director/ops_head/ops_staff/gov_head รหัส demo123")
    else:
        st.subheader("ผู้ใช้")
        st.write(f"**{current_user()['username']}**")
        st.write(f"Role: `{current_user()['role']}`")
        if current_user().get("dept_id"):
            st.write(f"Dept: `{current_user()['dept_id']}`")
        if st.button("Logout"):
            logout()
            st.rerun()

    st.divider()
    st.markdown("### As-of & Filters")
    as_of_txt = st.text_input("As-of date_id (YYYYMMDD)", value=date.today().strftime("%Y%m%d"))
    org_id = st.text_input("organization_id", value="ORG1")
    try:
        as_of_id = int(as_of_txt)
    except:
        as_of_id = int(date.today().strftime("%Y%m%d"))

    st.divider()
    pages = ["Executive View", "Department Workspace", "Meeting → Action Items", "Data Import", "Admin / User Management", "Schema / Templates"]
    if is_logged_in():
        visible = [pg for pg in pages if can_view(pg)]
    else:
        visible = ["Schema / Templates"]
    if is_logged_in() and current_user().get("role") == "Admin":
        if st.button("🌱 Seed Demo Data"):
            seed_demo_data()
            st.success("Seed demo data เรียบร้อย")
            st.rerun()
    page = st.radio("ไปที่หน้า", visible, index=0)

# Public schema page
if page == "Schema / Templates" and not is_logged_in():
    st.markdown("## 🧾 Schema / Templates (Public)")
    table = st.selectbox("เลือกตาราง", list(TABLE_COLUMNS.keys()))
    st.download_button(f"Download template: {table}.csv", make_template_csv(table), f"{table}.csv", "text/csv")
    st.stop()

require_login()

# Executive View
if page == "Executive View":
    st.markdown("## 📌 Executive View (1 หน้า)")
    kpi, dept_work, actions = load_exec_data(as_of_id, org_id)
    scope = allowed_dept_scope()

    # KPI
    c1, c2, c3, c4 = st.columns(4)
    if not kpi.empty:
        green = int((kpi["kpi_status"] == "Green").sum())
        amber = int((kpi["kpi_status"] == "Amber").sum())
        red = int((kpi["kpi_status"] == "Red").sum())
        total = int(len(kpi))
    else:
        green = amber = red = total = 0
    c1.metric("KPI (ทั้งหมด)", total); c2.metric("Green", green); c3.metric("Amber", amber); c4.metric("Red", red)

    if total > 0:
        fig = px.pie(pd.DataFrame({"status":["Green","Amber","Red"], "count":[green, amber, red]}), values="count", names="status")
        fig.update_layout(height=260, margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.markdown("### Department Work Snapshot")
    dw = dept_work.copy()
    if scope:
        dw = dw[dw["dept_id"] == scope]
    if dw.empty:
        st.info("ยังไม่มีงาน หรือคุณไม่มีสิทธิ์เห็น")
    else:
        st.dataframe(dw[["dept_name","work_title","status","progress_percent","risk_level","decision_needed","due_date_id","person_name"]], use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("### Meeting Action Items Snapshot")
    act = actions.copy()
    if scope:
        act = act[act["dept_id"] == scope]
    if act.empty:
        st.info("ยังไม่มี action items หรือคุณไม่มีสิทธิ์เห็น")
    else:
        st.dataframe(act[["meeting_title","dept_name","action_title","status","progress_percent","due_date_id","decision_needed","person_name","linked_work_id"]], use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("### Executive Action Panel")
    items = []
    if not kpi.empty:
        for _, r in kpi[kpi["kpi_status"] == "Red"].iterrows():
            items.append(f"⚠️ KPI แดง: **{r['kpi_name']}** ({r['strategy_name']})")
    if not dw.empty:
        need = dw[dw["decision_needed"].fillna(0).astype(int) == 1]
        for _, r in need.head(8).iterrows():
            items.append(f"🧩 ต้องตัดสินใจ: **{r['dept_name']}** — {r['work_title']}")
    if not act.empty:
        need = act[act["decision_needed"].fillna(0).astype(int) == 1]
        for _, r in need.head(8).iterrows():
            items.append(f"🧷 Action ต้องตัดสินใจ: **{r['dept_name']}** — {r['action_title']}")
    if not items:
        st.success("ยังไม่มีประเด็นเร่งด่วน")
    else:
        for it in items[:18]:
            st.write(it)

# Department Workspace
elif page == "Department Workspace":
    st.markdown("## 🧩 Department Workspace")
    scope = allowed_dept_scope()
    depts = read_df("SELECT * FROM dim_department ORDER BY dept_name")
    persons = read_df("SELECT * FROM dim_person ORDER BY person_name")
    wtypes = read_df("SELECT * FROM dim_work_type ORDER BY work_type_name")

    if depts.empty:
        st.warning("ยังไม่มี dim_department — Admin ควร Seed หรือ Import ก่อน")
        st.stop()

    if scope:
        dept_id = scope
        dept_name = depts.loc[depts["dept_id"] == dept_id, "dept_name"].iloc[0]
        st.info(f"สิทธิ์ของคุณจำกัดเฉพาะแผนก: **{dept_name}**")
    else:
        dept_name = st.selectbox("เลือกแผนก", depts["dept_name"].tolist())
        dept_id = depts.loc[depts["dept_name"] == dept_name, "dept_id"].iloc[0]

    with st.expander("➕ เพิ่มงานใหม่", expanded=True):
        title = st.text_input("ชื่องาน/ภารกิจ *")
        owner = st.selectbox("เจ้าภาพ", persons["person_name"].tolist() if not persons.empty else ["-"])
        wtype = st.selectbox("ประเภทงาน", wtypes["work_type_name"].tolist() if not wtypes.empty else ["Routine","Meeting Action"])
        status = st.selectbox("สถานะ", ["Planned","In Progress","At Risk","Done"], index=1)
        priority = st.selectbox("ความสำคัญ", ["High","Medium","Low"], index=0)
        risk = st.selectbox("ระดับความเสี่ยง", ["Low","Medium","High"], index=1)
        decision_needed = st.checkbox("ต้องการการตัดสินใจ", value=False)
        progress = st.slider("% ความคืบหน้า", 0, 100, 0)
        start_d = st.date_input("วันเริ่ม", value=date.today())
        due_d = st.date_input("กำหนดเสร็จ", value=date.today() + timedelta(days=30))
        notes = st.text_area("หมายเหตุ", height=80)

        if st.button("บันทึกงาน", type="primary"):
            if not title.strip():
                st.error("กรุณากรอกชื่องาน")
            else:
                ensure_dim_date(start_d, due_d)
                owner_id = None
                if not persons.empty:
                    owner_id = persons.loc[persons["person_name"] == owner, "person_id"].iloc[0]
                wtype_id = None
                if not wtypes.empty:
                    wtype_id = wtypes.loc[wtypes["work_type_name"] == wtype, "work_type_id"].iloc[0]
                work_id = uid("W")
                exec_sql("""
                    INSERT INTO fact_dept_work_item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    work_id, dept_id, org_id, None, None, owner_id, title, wtype_id, priority, status,
                    to_date_id(start_d), to_date_id(due_d), float(progress), risk, 1 if decision_needed else 0,
                    notes, datetime.now().isoformat()
                ))
                st.success("บันทึกงานเรียบร้อย")
                st.rerun()

    work_df = read_df(f"""
        SELECT w.*, p.person_name, d.dept_name
        FROM fact_dept_work_item w
        LEFT JOIN dim_person p ON w.owner_person_id = p.person_id
        LEFT JOIN dim_department d ON w.dept_id = d.dept_id
        WHERE w.dept_id = '{dept_id}' AND w.organization_id = '{org_id}'
        ORDER BY w.last_updated_ts DESC
    """)
    if work_df.empty:
        st.info("ยังไม่มีงานของแผนกนี้")
        st.stop()

    st.dataframe(work_df[["work_id","dept_name","work_title","status","progress_percent","risk_level","decision_needed","due_date_id","person_name","last_updated_ts"]], use_container_width=True, hide_index=True)

# Meeting module
elif page == "Meeting → Action Items":
    st.markdown("## 🗓️ Meeting → Action Items")
    depts = read_df("SELECT * FROM dim_department ORDER BY dept_name")
    persons = read_df("SELECT * FROM dim_person ORDER BY person_name")

    tab1, tab2, tab3 = st.tabs(["Create Meeting", "Track Actions", "Link Action → Work"])

    with tab1:
        st.subheader("Create Meeting")
        mt = st.text_input("หัวข้อประชุม *")
        mtype = st.selectbox("ประเภท", ["Committee","Department","Ad-hoc"])
        md = st.date_input("วันที่ประชุม", value=date.today())
        org = st.selectbox("ผู้จัด/ประธาน", persons["person_name"].tolist() if not persons.empty else ["-"])
        minutes = st.text_area("Minutes (ย่อ)", height=120)
        if st.button("บันทึก Meeting", type="primary"):
            if not mt.strip():
                st.error("กรุณากรอกหัวข้อประชุม")
            else:
                ensure_dim_date(md, md)
                org_id_p = None
                if not persons.empty:
                    org_id_p = persons.loc[persons["person_name"] == org, "person_id"].iloc[0]
                meeting_id = uid("MTG")
                exec_sql("INSERT INTO fact_meeting VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (meeting_id, to_date_id(md), mt, mtype, org_id_p, minutes, datetime.now().isoformat()))
                st.success(f"สร้าง Meeting: {meeting_id}")
                st.rerun()

        st.divider()
        meetings = read_df("SELECT meeting_id, meeting_title FROM fact_meeting ORDER BY created_ts DESC")
        if meetings.empty:
            st.info("ยังไม่มี meeting")
        else:
            sel_m = st.selectbox("เลือก Meeting", meetings["meeting_id"].tolist())
            dec = st.text_area("Decision *", height=80)
            dec_owner = st.selectbox("Decision owner", persons["person_name"].tolist() if not persons.empty else ["-"], key="dec_owner2")
            if st.button("เพิ่ม Decision"):
                if not dec.strip():
                    st.error("กรุณากรอก Decision")
                else:
                    owner_id = None
                    if not persons.empty:
                        owner_id = persons.loc[persons["person_name"] == dec_owner, "person_id"].iloc[0]
                    decision_id = uid("DEC")
                    exec_sql("INSERT INTO fact_meeting_decision VALUES (?, ?, ?, ?, ?)",
                             (decision_id, sel_m, dec, owner_id, datetime.now().isoformat()))
                    st.success(f"เพิ่ม Decision: {decision_id}")
                    st.rerun()

            decs = read_df("SELECT decision_id, decision_text FROM fact_meeting_decision WHERE meeting_id = ? ORDER BY created_ts DESC", params=(sel_m,))
            if not decs.empty:
                st.markdown("#### Create Action under Decision")
                sel_d = st.selectbox("เลือก Decision", decs["decision_id"].tolist())
                a_title = st.text_input("Action item *", key="a_title2")
                a_dept = st.selectbox("มอบหมายแผนก", depts["dept_name"].tolist() if not depts.empty else ["-"], key="a_dept2")
                a_owner = st.selectbox("เจ้าภาพ", persons["person_name"].tolist() if not persons.empty else ["-"], key="a_owner2")
                a_due = st.date_input("กำหนดเสร็จ", value=date.today() + timedelta(days=30), key="a_due2")
                a_need_dec = st.checkbox("ต้องการการตัดสินใจ", value=False, key="a_need_dec2")
                if st.button("สร้าง Action item", type="primary"):
                    if not a_title.strip():
                        st.error("กรุณากรอก Action item")
                    else:
                        ensure_dim_date(date.today(), a_due)
                        dept_id = depts.loc[depts["dept_name"] == a_dept, "dept_id"].iloc[0] if not depts.empty else None
                        owner_id = persons.loc[persons["person_name"] == a_owner, "person_id"].iloc[0] if not persons.empty else None
                        action_id = uid("ACT")
                        exec_sql("INSERT INTO fact_meeting_action_item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                 (action_id, sel_m, sel_d, dept_id, owner_id, a_title, "In Progress",
                                  to_date_id(date.today()), to_date_id(a_due), 0, "", 1 if a_need_dec else 0, None, datetime.now().isoformat()))
                        st.success(f"สร้าง Action: {action_id}")
                        st.rerun()

    with tab2:
        st.subheader("Track Action Items")
        actions = read_df("""
            SELECT a.*, d.dept_name, p.person_name, m.meeting_title
            FROM fact_meeting_action_item a
            LEFT JOIN dim_department d ON a.dept_id = d.dept_id
            LEFT JOIN dim_person p ON a.owner_person_id = p.person_id
            LEFT JOIN fact_meeting m ON a.meeting_id = m.meeting_id
            ORDER BY a.last_updated_ts DESC
        """)
        if actions.empty:
            st.info("ยังไม่มี actions")
        else:
            st.dataframe(actions[["action_id","meeting_title","dept_name","action_title","status","progress_percent","due_date_id","decision_needed","person_name","linked_work_id"]], use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("Link Action → Work")
        actions = read_df("SELECT * FROM fact_meeting_action_item ORDER BY last_updated_ts DESC")
        if actions.empty:
            st.info("ยังไม่มี actions")
        else:
            sel = st.selectbox("เลือก action_id", actions["action_id"].tolist())
            arow = actions[actions["action_id"] == sel].iloc[0]
            if arow["linked_work_id"]:
                st.success(f"Linked แล้ว: {arow['linked_work_id']}")
            else:
                if st.button("สร้าง Work item จาก Action", type="primary"):
                    if not arow["dept_id"]:
                        st.error("Action นี้ยังไม่ระบุ dept_id")
                    else:
                        work_id = uid("W")
                        exec_sql("INSERT INTO fact_dept_work_item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                 (work_id, arow["dept_id"], org_id, None, None, arow["owner_person_id"],
                                  f"[From Meeting] {arow['action_title']}", None, "High", arow["status"],
                                  arow["start_date_id"], arow["due_date_id"], arow["progress_percent"], "Medium",
                                  arow["decision_needed"], "Generated from meeting action item", datetime.now().isoformat()))
                        exec_sql("UPDATE fact_meeting_action_item SET linked_work_id = ?, last_updated_ts = ? WHERE action_id = ?",
                                 (work_id, datetime.now().isoformat(), sel))
                        st.success(f"สร้างและ link สำเร็จ: {work_id}")
                        st.rerun()

# Admin pages
elif page == "Data Import":
    st.markdown("## ⬆️ Data Import (Admin)")
    table = st.selectbox("เลือกตาราง", list(TABLE_COLUMNS.keys()))
    st.download_button(f"Download template: {table}.csv", make_template_csv(table), f"{table}.csv", "text/csv")
    up = st.file_uploader("Upload CSV", type=["csv"])
    if up is not None and st.button("Import (Replace Table)", type="primary"):
        df = pd.read_csv(up)
        replace_table(table, df)
        st.success(f"Imported -> {table}")
        st.rerun()

elif page == "Admin / User Management":
    st.markdown("## 👤 Admin / User Management")
    users = read_df("SELECT username, role, dept_id, person_id, is_enabled FROM dim_user ORDER BY username")
    st.dataframe(users, use_container_width=True, hide_index=True)

    st.subheader("เพิ่มผู้ใช้ใหม่")
    depts = read_df("SELECT * FROM dim_department ORDER BY dept_name")
    persons = read_df("SELECT * FROM dim_person ORDER BY person_name")
    u = st.text_input("username ใหม่", key="newu")
    pw = st.text_input("password", type="password", key="newpw")
    role = st.selectbox("role", ["Admin","Executive","DeptHead","Staff"], index=3, key="newrole")
    dept_id = st.selectbox("dept_id", ["(none)"] + (depts["dept_id"].tolist() if not depts.empty else []), key="newdept")
    person_id = st.selectbox("person_id", ["(none)"] + (persons["person_id"].tolist() if not persons.empty else []), key="newperson")
    enabled = st.checkbox("is_enabled", value=True, key="newen")
    if st.button("สร้างผู้ใช้", type="primary"):
        if not u.strip() or not pw:
            st.error("กรุณากรอก username และ password")
        else:
            exec_sql("INSERT OR REPLACE INTO dim_user VALUES (?, ?, ?, ?, ?, ?)",
                     (u.strip(), sha256(pw), role,
                      None if dept_id == "(none)" else dept_id,
                      None if person_id == "(none)" else person_id,
                      1 if enabled else 0))
            st.success("สร้าง/อัปเดตผู้ใช้เรียบร้อย")
            st.rerun()

else:
    st.markdown("## 🧾 Schema / Templates")
    for t, cols in TABLE_COLUMNS.items():
        with st.expander(t):
            st.code(", ".join(cols))
    st.markdown("### RBAC")
    st.markdown("- Admin: ทุกหน้า\n- Executive: Executive + Meeting\n- DeptHead/Staff: Department Workspace (เฉพาะแผนกตน)")
