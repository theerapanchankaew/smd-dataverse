import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta
import uuid
import os

# =========================================================
# Intelligence Hub (MVP+) - Department Work Management
# - Adds: departments contribute their responsible work items into the hub
# - Keeps: Executive View (1 page) + CSV Import
# - Storage: SQLite embedded DB (PoC / Streamlit Cloud demo)
# =========================================================

st.set_page_config(
    page_title="Intelligence Hub (MVP+)",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path("intelligence_hub.db")
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# -------------------------
# DB helpers
# -------------------------
def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Dimensions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        month INTEGER,
        quarter INTEGER,
        year INTEGER
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_strategy (
        strategy_id TEXT PRIMARY KEY,
        strategy_name TEXT NOT NULL,
        strategy_owner TEXT,
        strategy_level TEXT,
        start_year INTEGER,
        end_year INTEGER
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_kpi (
        kpi_id TEXT PRIMARY KEY,
        kpi_name TEXT NOT NULL,
        kpi_definition TEXT,
        calculation_logic TEXT,
        kpi_owner TEXT,
        refresh_frequency TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_person (
        person_id TEXT PRIMARY KEY,
        person_name TEXT NOT NULL,
        role TEXT,
        department TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_organization (
        organization_id TEXT PRIMARY KEY,
        organization_name TEXT NOT NULL,
        org_type TEXT,
        sector TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_risk_category (
        risk_category_id TEXT PRIMARY KEY,
        risk_category_name TEXT NOT NULL,
        governance_area TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_source (
        source_id TEXT PRIMARY KEY,
        source_type TEXT,
        source_name TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_topic (
        topic_id TEXT PRIMARY KEY,
        topic_name TEXT NOT NULL
    )
    """)

    # NEW: Department + Work Type
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_department (
        dept_id TEXT PRIMARY KEY,
        dept_name TEXT NOT NULL,
        dept_head_person_id TEXT,
        description TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_work_type (
        work_type_id TEXT PRIMARY KEY,
        work_type_name TEXT NOT NULL
    )
    """)

    # Facts (original)
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
        last_updated_ts TEXT,
        FOREIGN KEY(date_id) REFERENCES dim_date(date_id)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_strategy_project (
        project_fact_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        project_name TEXT NOT NULL,
        owner_person_id TEXT,
        start_date_id INTEGER,
        end_date_id INTEGER,
        progress_percent REAL,
        budget_plan REAL,
        budget_used REAL,
        project_status TEXT,
        risk_level TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_risk_event (
        risk_event_id TEXT PRIMARY KEY,
        date_id INTEGER NOT NULL,
        organization_id TEXT NOT NULL,
        risk_category_id TEXT NOT NULL,
        risk_description TEXT,
        likelihood_score REAL,
        impact_score REAL,
        risk_score REAL,
        mitigation_plan TEXT,
        mitigation_status TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_external_signal (
        signal_id TEXT PRIMARY KEY,
        date_id INTEGER NOT NULL,
        source_id TEXT NOT NULL,
        topic_id TEXT NOT NULL,
        signal_type TEXT,
        relevance_score REAL,
        sentiment_score REAL,
        linked_strategy_id TEXT
    )
    """)

    # NEW: Department Work Items + Updates + Evidence
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

    conn.commit()
    conn.close()

def read_df(sql: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(sql, conn)
    finally:
        conn.close()
    return df

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

# -------------------------
# Templates (for CSV import)
# -------------------------
TABLE_COLUMNS = {
    "dim_strategy": ["strategy_id","strategy_name","strategy_owner","strategy_level","start_year","end_year"],
    "dim_kpi": ["kpi_id","kpi_name","kpi_definition","calculation_logic","kpi_owner","refresh_frequency"],
    "dim_person": ["person_id","person_name","role","department"],
    "dim_organization": ["organization_id","organization_name","org_type","sector"],
    "dim_risk_category": ["risk_category_id","risk_category_name","governance_area"],
    "dim_source": ["source_id","source_type","source_name"],
    "dim_topic": ["topic_id","topic_name"],
    "dim_department": ["dept_id","dept_name","dept_head_person_id","description"],
    "dim_work_type": ["work_type_id","work_type_name"],
    "fact_strategic_kpi": ["kpi_fact_id","date_id","strategy_id","organization_id","kpi_id","actual_value","target_value","variance_value","kpi_status","last_updated_ts"],
    "fact_strategy_project": ["project_fact_id","strategy_id","project_name","owner_person_id","start_date_id","end_date_id","progress_percent","budget_plan","budget_used","project_status","risk_level"],
    "fact_risk_event": ["risk_event_id","date_id","organization_id","risk_category_id","risk_description","likelihood_score","impact_score","risk_score","mitigation_plan","mitigation_status"],
    "fact_external_signal": ["signal_id","date_id","source_id","topic_id","signal_type","relevance_score","sentiment_score","linked_strategy_id"],
    "fact_dept_work_item": ["work_id","dept_id","organization_id","strategy_id","kpi_id","owner_person_id","work_title","work_type_id","priority","status","start_date_id","due_date_id","progress_percent","risk_level","decision_needed","notes","last_updated_ts"],
    "fact_work_update": ["update_id","work_id","date_id","progress_percent","update_text","blockers","decision_needed","created_ts"],
    "fact_work_evidence": ["evidence_id","work_id","date_id","file_name","stored_path","note","uploaded_ts"],
}

def make_template_csv(table: str) -> bytes:
    df = pd.DataFrame(columns=TABLE_COLUMNS[table])
    return df.to_csv(index=False).encode("utf-8")

# -------------------------
# Demo seed
# -------------------------
def seed_demo_data() -> None:
    today = date.today()
    ensure_dim_date(today - timedelta(days=180), today)

    # Minimal org
    replace_table("dim_organization", pd.DataFrame([{
        "organization_id":"ORG1","organization_name":"MASCI (HQ)","org_type":"Foundation","sector":"Certification"
    }]))

    replace_table("dim_strategy", pd.DataFrame([
        {"strategy_id":"S1","strategy_name":"ยกระดับคุณภาพบริการรับรอง","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
        {"strategy_id":"S2","strategy_name":"Digital Transformation","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
        {"strategy_id":"S3","strategy_name":"การบริหารความเสี่ยง/ธรรมาภิบาล","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
    ]))

    replace_table("dim_kpi", pd.DataFrame([
        {"kpi_id":"K1","kpi_name":"On-time Delivery","kpi_definition":"งานส่งมอบตามกำหนด","calculation_logic":"on_time/total","kpi_owner":"Strategy","refresh_frequency":"Weekly"},
        {"kpi_id":"K2","kpi_name":"Customer Satisfaction","kpi_definition":"ความพึงพอใจลูกค้า","calculation_logic":"avg_score","kpi_owner":"Strategy","refresh_frequency":"Monthly"},
        {"kpi_id":"K3","kpi_name":"Compliance Findings","kpi_definition":"ประเด็นไม่สอดคล้อง","calculation_logic":"count(findings)","kpi_owner":"Governance","refresh_frequency":"Monthly"},
    ]))

    replace_table("dim_person", pd.DataFrame([
        {"person_id":"P1","person_name":"Owner A","role":"Manager","department":"Strategy"},
        {"person_id":"P2","person_name":"Owner B","role":"Manager","department":"Operations"},
        {"person_id":"P3","person_name":"Owner C","role":"Manager","department":"Governance"},
    ]))

    replace_table("dim_department", pd.DataFrame([
        {"dept_id":"D1","dept_name":"Operations","dept_head_person_id":"P2","description":"งานปฏิบัติการ/บริการ"},
        {"dept_id":"D2","dept_name":"Strategy","dept_head_person_id":"P1","description":"งานยุทธศาสตร์/แผน"},
        {"dept_id":"D3","dept_name":"Governance","dept_head_person_id":"P3","description":"งานกำกับดูแล/ความเสี่ยง"},
    ]))

    replace_table("dim_work_type", pd.DataFrame([
        {"work_type_id":"WT1","work_type_name":"Routine"},
        {"work_type_id":"WT2","work_type_name":"Project"},
        {"work_type_id":"WT3","work_type_name":"Improvement"},
        {"work_type_id":"WT4","work_type_name":"Risk Mitigation"},
    ]))

    d0 = to_date_id(today)

    replace_table("fact_strategic_kpi", pd.DataFrame([
        {"kpi_fact_id":"F_KPI_1","date_id":d0,"strategy_id":"S1","organization_id":"ORG1","kpi_id":"K1","actual_value":0.91,"target_value":0.95,"variance_value":-0.04,"kpi_status":"Amber","last_updated_ts":datetime.now().isoformat()},
        {"kpi_fact_id":"F_KPI_2","date_id":d0,"strategy_id":"S3","organization_id":"ORG1","kpi_id":"K3","actual_value":12,"target_value":8,"variance_value":4,"kpi_status":"Red","last_updated_ts":datetime.now().isoformat()},
    ]))

    # Dept work items
    replace_table("fact_dept_work_item", pd.DataFrame([
        {"work_id":"W1","dept_id":"D1","organization_id":"ORG1","strategy_id":"S1","kpi_id":"K1","owner_person_id":"P2",
         "work_title":"ลดงานส่งมอบล่าช้า (ปรับ workflow + checklist)","work_type_id":"WT3","priority":"High","status":"In Progress",
         "start_date_id":d0-30,"due_date_id":d0+30,"progress_percent":55,"risk_level":"Medium","decision_needed":0,
         "notes":"ทดลองใช้ checklist 2 ทีม","last_updated_ts":datetime.now().isoformat()},
        {"work_id":"W2","dept_id":"D3","organization_id":"ORG1","strategy_id":"S3","kpi_id":"K3","owner_person_id":"P3",
         "work_title":"ปิดประเด็น compliance findings ตามรอบ","work_type_id":"WT4","priority":"High","status":"At Risk",
         "start_date_id":d0-14,"due_date_id":d0+14,"progress_percent":25,"risk_level":"High","decision_needed":1,
         "notes":"ต้องการทรัพยากรตรวจทวนเอกสารเพิ่ม","last_updated_ts":datetime.now().isoformat()},
    ]))

# -------------------------
# Executive data loaders
# -------------------------
def load_exec_data(as_of_id: int, org_id: str):
    kpi = read_df(f"""
        SELECT f.*, k.kpi_name, s.strategy_name
        FROM fact_strategic_kpi f
        LEFT JOIN dim_kpi k ON f.kpi_id = k.kpi_id
        LEFT JOIN dim_strategy s ON f.strategy_id = s.strategy_id
        WHERE f.date_id = {as_of_id}
          AND f.organization_id = '{org_id}'
    """)
    projects = read_df("""
        SELECT p.*, s.strategy_name, per.person_name
        FROM fact_strategy_project p
        LEFT JOIN dim_strategy s ON p.strategy_id = s.strategy_id
        LEFT JOIN dim_person per ON p.owner_person_id = per.person_id
    """)
    risks = read_df(f"""
        SELECT r.*, c.risk_category_name
        FROM fact_risk_event r
        LEFT JOIN dim_risk_category c ON r.risk_category_id = c.risk_category_id
        WHERE r.date_id = {as_of_id}
          AND r.organization_id = '{org_id}'
    """)
    signals = read_df(f"""
        SELECT e.*, src.source_name, t.topic_name, s.strategy_name
        FROM fact_external_signal e
        LEFT JOIN dim_source src ON e.source_id = src.source_id
        LEFT JOIN dim_topic t ON e.topic_id = t.topic_id
        LEFT JOIN dim_strategy s ON e.linked_strategy_id = s.strategy_id
        WHERE e.date_id = {as_of_id}
    """)
    dept_work = read_df(f"""
        SELECT w.*, d.dept_name, p.person_name, s.strategy_name, k.kpi_name, wt.work_type_name
        FROM fact_dept_work_item w
        LEFT JOIN dim_department d ON w.dept_id = d.dept_id
        LEFT JOIN dim_person p ON w.owner_person_id = p.person_id
        LEFT JOIN dim_strategy s ON w.strategy_id = s.strategy_id
        LEFT JOIN dim_kpi k ON w.kpi_id = k.kpi_id
        LEFT JOIN dim_work_type wt ON w.work_type_id = wt.work_type_id
        WHERE w.organization_id = '{org_id}'
    """)
    return kpi, projects, risks, signals, dept_work

# -------------------------
# App start
# -------------------------
init_db()

with st.sidebar:
    st.title("🧠 Intelligence Hub")
    st.caption("MVP+ • Executive + Department Work")
    st.divider()

    page = st.radio("เมนู", ["Executive View", "Department Workspace", "Data Import", "Schema / Templates"], index=0)

    st.divider()
    st.markdown("### As-of & Filters")
    as_of_txt = st.text_input("As-of date_id (YYYYMMDD)", value=date.today().strftime("%Y%m%d"))
    org_id = st.text_input("organization_id", value="ORG1")
    try:
        as_of_id = int(as_of_txt)
    except:
        as_of_id = int(date.today().strftime("%Y%m%d"))

# -------------------------
# Page: Executive View
# -------------------------
if page == "Executive View":
    st.markdown("## 📌 Executive View (1 หน้า)")
    st.caption("รวม KPI / โครงการ / ความเสี่ยง / สัญญาณภายนอก + งานรับผิดชอบจากแต่ละแผนก")

    c = st.columns([1,4,4,4,4])
    with c[0]:
        if st.button("🌱 Seed Demo Data"):
            seed_demo_data()
            st.success("Seed demo data เรียบร้อย")
            st.rerun()

    kpi, projects, risks, signals, dept_work = load_exec_data(as_of_id, org_id)

    # Health snapshot
    c1, c2, c3, c4 = st.columns(4)
    if not kpi.empty:
        green = int((kpi["kpi_status"] == "Green").sum())
        amber = int((kpi["kpi_status"] == "Amber").sum())
        red = int((kpi["kpi_status"] == "Red").sum())
        total = int(len(kpi))
    else:
        green = amber = red = total = 0
    c1.metric("KPI (ทั้งหมด)", total)
    c2.metric("Green", green)
    c3.metric("Amber", amber)
    c4.metric("Red", red)

    colA, colB = st.columns([1.35, 1])
    with colA:
        st.markdown("### 1) KPI สำคัญ")
        if kpi.empty:
            st.info("ยังไม่มีข้อมูล fact_strategic_kpi → กด Seed Demo Data หรือ Import CSV")
        else:
            st.dataframe(
                kpi[["kpi_name","strategy_name","actual_value","target_value","variance_value","kpi_status"]],
                use_container_width=True, hide_index=True
            )
    with colB:
        st.markdown("### KPI Status Distribution")
        if total > 0:
            fig = px.pie(pd.DataFrame({"status":["Green","Amber","Red"], "count":[green, amber, red]}),
                         values="count", names="status")
            fig.update_layout(height=300, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Department Work snapshot (NEW)
    st.markdown("### 2) Department Work Snapshot")
    if dept_work.empty:
        st.info("ยังไม่มีงานจากแผนก → ไปที่ Department Workspace เพื่อบันทึกงานรับผิดชอบ")
    else:
        a, b = st.columns([1.4, 1])
        with a:
            show_cols = ["dept_name","work_title","work_type_name","priority","status","progress_percent","risk_level","decision_needed","person_name","due_date_id"]
            st.dataframe(dept_work[show_cols].sort_values(["risk_level","priority"], ascending=[False, False]),
                         use_container_width=True, hide_index=True)
        with b:
            fig2 = px.bar(dept_work, x="dept_name", y="work_id", color="status", barmode="group")
            fig2.update_layout(height=320, margin=dict(l=10,r=10,t=10,b=10), yaxis_title="จำนวนงาน")
            st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # Action panel
    st.markdown("### 3) Executive Action Panel (สิ่งที่ต้องตัดสินใจ)")
    action_items = []

    if not kpi.empty:
        for _, row in kpi[kpi["kpi_status"] == "Red"].iterrows():
            action_items.append(f"⚠️ KPI แดง: **{row['kpi_name']}** ({row['strategy_name']}) → ขอแผนแก้ไข/เจ้าภาพ/เส้นตาย")

    if not dept_work.empty:
        # Decision needed by dept
        need_decision = dept_work[dept_work["decision_needed"].fillna(0).astype(int) == 1]
        for _, row in need_decision.head(8).iterrows():
            action_items.append(f"🧩 ต้องการการตัดสินใจ: **{row['dept_name']}** — {row['work_title']} (เจ้าภาพ: {row.get('person_name','-')})")

        # Overdue rule (simple): due_date_id < as_of_id and status not Done/Closed
        dw = dept_work.copy()
        dw["due_date_id"] = pd.to_numeric(dw["due_date_id"], errors="coerce")
        overdue = dw[(dw["due_date_id"].notna()) & (dw["due_date_id"] < as_of_id) & (~dw["status"].isin(["Done","Closed","Completed"]))]

        for _, row in overdue.head(8).iterrows():
            action_items.append(f"⏰ งานเกินกำหนด: **{row['dept_name']}** — {row['work_title']} (due {int(row['due_date_id'])})")

        # High risk
        high_risk = dept_work[dept_work["risk_level"].isin(["High","สูง","H"])]
        for _, row in high_risk.head(6).iterrows():
            action_items.append(f"🔥 งานเสี่ยงสูง: **{row['dept_name']}** — {row['work_title']} (progress {row['progress_percent']}%)")

    if not action_items:
        st.success("ยังไม่มีประเด็นเร่งด่วน (หรือยังไม่มีข้อมูล)")
    else:
        for a in action_items[:15]:
            st.write(a)

# -------------------------
# Page: Department Workspace
# -------------------------
elif page == "Department Workspace":
    st.markdown("## 🧩 Department Workspace")
    st.caption("ให้แต่ละแผนกบันทึก “งานรับผิดชอบ” + อัปเดตความคืบหน้า + แนบหลักฐาน เพื่อให้ผู้บริหารเห็นภาพรวมเดียวกัน")

    depts = read_df("SELECT * FROM dim_department ORDER BY dept_name")
    persons = read_df("SELECT * FROM dim_person ORDER BY person_name")
    strategies = read_df("SELECT * FROM dim_strategy ORDER BY strategy_name")
    kpis = read_df("SELECT * FROM dim_kpi ORDER BY kpi_name")
    wtypes = read_df("SELECT * FROM dim_work_type ORDER BY work_type_name")

    if depts.empty:
        st.warning("ยังไม่มี dim_department — แนะนำ Import หรือ Seed Demo Data ก่อน")
    else:
        dept_label = st.selectbox("เลือกแผนก", depts["dept_name"].tolist())
        dept_id = depts.loc[depts["dept_name"] == dept_label, "dept_id"].iloc[0]

        # KPIs / strategy optional link
        with st.expander("➕ เพิ่มงานรับผิดชอบใหม่", expanded=True):
            c1, c2, c3 = st.columns([1.2, 1, 1])
            with c1:
                work_title = st.text_input("ชื่องาน/ภารกิจ *", placeholder="เช่น ปรับปรุงกระบวนการตรวจทานเอกสารก่อนออกใบรับรอง")
                work_type = st.selectbox("ประเภทงาน", (wtypes["work_type_name"].tolist() if not wtypes.empty else ["Routine","Project","Improvement","Risk Mitigation"]))
                priority = st.selectbox("ความสำคัญ", ["High","Medium","Low"], index=0)
                status = st.selectbox("สถานะ", ["Planned","In Progress","At Risk","Done"], index=1)
            with c2:
                owner = st.selectbox("เจ้าภาพ", (persons["person_name"].tolist() if not persons.empty else ["-"]))
                risk_level = st.selectbox("ระดับความเสี่ยง", ["Low","Medium","High"], index=1)
                decision_needed = st.checkbox("ต้องการการตัดสินใจจากผู้บริหาร", value=False)
                progress = st.slider("ความคืบหน้า (%)", 0, 100, 0)
            with c3:
                start_d = st.date_input("วันเริ่ม", value=date.today())
                due_d = st.date_input("กำหนดเสร็จ", value=date.today() + timedelta(days=30))
                strategy_opt = st.selectbox("เชื่อมยุทธศาสตร์ (ถ้ามี)", (["(ไม่ระบุ)"] + (strategies["strategy_name"].tolist() if not strategies.empty else [])))
                kpi_opt = st.selectbox("เชื่อม KPI (ถ้ามี)", (["(ไม่ระบุ)"] + (kpis["kpi_name"].tolist() if not kpis.empty else [])))
                notes = st.text_area("หมายเหตุ", height=90)

            if st.button("บันทึกงาน", type="primary"):
                if not work_title.strip():
                    st.error("กรุณากรอกชื่องาน/ภารกิจ")
                else:
                    ensure_dim_date(start_d, due_d)

                    work_type_id = None
                    if not wtypes.empty and work_type in wtypes["work_type_name"].tolist():
                        work_type_id = wtypes.loc[wtypes["work_type_name"] == work_type, "work_type_id"].iloc[0]

                    owner_person_id = None
                    if not persons.empty and owner in persons["person_name"].tolist():
                        owner_person_id = persons.loc[persons["person_name"] == owner, "person_id"].iloc[0]

                    strategy_id = None
                    if strategy_opt != "(ไม่ระบุ)" and not strategies.empty:
                        strategy_id = strategies.loc[strategies["strategy_name"] == strategy_opt, "strategy_id"].iloc[0]

                    kpi_id = None
                    if kpi_opt != "(ไม่ระบุ)" and not kpis.empty:
                        kpi_id = kpis.loc[kpis["kpi_name"] == kpi_opt, "kpi_id"].iloc[0]

                    work_id = uid("W")
                    exec_sql("""
                        INSERT INTO fact_dept_work_item(
                            work_id, dept_id, organization_id, strategy_id, kpi_id, owner_person_id,
                            work_title, work_type_id, priority, status, start_date_id, due_date_id,
                            progress_percent, risk_level, decision_needed, notes, last_updated_ts
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        work_id, dept_id, org_id, strategy_id, kpi_id, owner_person_id,
                        work_title, work_type_id, priority, status, to_date_id(start_d), to_date_id(due_d),
                        float(progress), risk_level, 1 if decision_needed else 0, notes, datetime.now().isoformat()
                    ))
                    st.success("บันทึกงานเรียบร้อย")
                    st.rerun()

        st.divider()

        st.markdown("### 📋 งานของแผนก")
        work_df = read_df(f"""
            SELECT w.*, p.person_name, wt.work_type_name, s.strategy_name, k.kpi_name
            FROM fact_dept_work_item w
            LEFT JOIN dim_person p ON w.owner_person_id = p.person_id
            LEFT JOIN dim_work_type wt ON w.work_type_id = wt.work_type_id
            LEFT JOIN dim_strategy s ON w.strategy_id = s.strategy_id
            LEFT JOIN dim_kpi k ON w.kpi_id = k.kpi_id
            WHERE w.dept_id = '{dept_id}'
              AND w.organization_id = '{org_id}'
            ORDER BY w.last_updated_ts DESC
        """)

        if work_df.empty:
            st.info("ยังไม่มีงานของแผนกนี้")
        else:
            st.dataframe(
                work_df[["work_id","work_title","work_type_name","priority","status","progress_percent","risk_level","decision_needed","person_name","due_date_id","last_updated_ts"]],
                use_container_width=True, hide_index=True
            )

            st.markdown("### 🔄 อัปเดตความคืบหน้า / แนบหลักฐาน")
            selected_work = st.selectbox("เลือก work_id เพื่ออัปเดต/แนบหลักฐาน", work_df["work_id"].tolist())
            wrow = work_df[work_df["work_id"] == selected_work].iloc[0]

            u1, u2 = st.columns([1.2, 1])
            with u1:
                with st.form("update_form", clear_on_submit=True):
                    upd_progress = st.slider("ความคืบหน้าล่าสุด (%)", 0, 100, int(wrow["progress_percent"] if pd.notna(wrow["progress_percent"]) else 0))
                    upd_text = st.text_area("Update", placeholder="สิ่งที่ทำไป/ผลลัพธ์/ประเด็นสำคัญ")
                    blockers = st.text_area("Blockers", placeholder="อุปสรรค/สิ่งที่ติดขัด (ถ้ามี)")
                    dec_need = st.checkbox("ต้องการการตัดสินใจ", value=bool(int(wrow["decision_needed"]) if pd.notna(wrow["decision_needed"]) else 0))
                    if st.form_submit_button("บันทึก Update"):
                        today = date.today()
                        ensure_dim_date(today, today)
                        exec_sql("""
                            INSERT INTO fact_work_update(update_id, work_id, date_id, progress_percent, update_text, blockers, decision_needed, created_ts)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            uid("UPD"), selected_work, to_date_id(today), float(upd_progress), upd_text, blockers, 1 if dec_need else 0, datetime.now().isoformat()
                        ))
                        # also update main work item
                        exec_sql("""
                            UPDATE fact_dept_work_item
                            SET progress_percent = ?, decision_needed = ?, last_updated_ts = ?
                            WHERE work_id = ?
                        """, (float(upd_progress), 1 if dec_need else 0, datetime.now().isoformat(), selected_work))
                        st.success("บันทึก update เรียบร้อย")
                        st.rerun()

            with u2:
                st.markdown("#### แนบหลักฐาน (Evidence)")
                upfile = st.file_uploader("อัปโหลดไฟล์หลักฐาน", type=None, key="evidence_uploader")
                ev_note = st.text_input("หมายเหตุหลักฐาน", placeholder="เช่น รายงาน, รูปถ่าย, บันทึกการประชุม")
                if upfile is not None and st.button("แนบหลักฐาน"):
                    today = date.today()
                    ensure_dim_date(today, today)
                    safe_name = f"{selected_work}_{int(datetime.now().timestamp())}_{upfile.name}"
                    stored_path = UPLOAD_DIR / safe_name
                    with open(stored_path, "wb") as f:
                        f.write(upfile.getbuffer())
                    exec_sql("""
                        INSERT INTO fact_work_evidence(evidence_id, work_id, date_id, file_name, stored_path, note, uploaded_ts)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        uid("EVI"), selected_work, to_date_id(today), upfile.name, str(stored_path), ev_note, datetime.now().isoformat()
                    ))
                    st.success("แนบหลักฐานเรียบร้อย")
                    st.rerun()

                st.markdown("#### หลักฐานที่แนบแล้ว")
                ev = read_df(f"SELECT * FROM fact_work_evidence WHERE work_id = '{selected_work}' ORDER BY uploaded_ts DESC")
                if ev.empty:
                    st.info("ยังไม่มีหลักฐาน")
                else:
                    for _, r in ev.iterrows():
                        cols = st.columns([3,2,2])
                        cols[0].write(f"📎 {r['file_name']}")
                        cols[1].write(r.get("note",""))
                        path = r["stored_path"]
                        if os.path.exists(path):
                            with open(path, "rb") as f:
                                cols[2].download_button("Download", data=f.read(), file_name=r["file_name"], key=f"dl_{r['evidence_id']}")
                        else:
                            cols[2].write("ไฟล์ไม่พบ (อาจถูกรีสตาร์ทบน cloud)")

            st.markdown("#### ประวัติ Update")
            upd = read_df(f"SELECT * FROM fact_work_update WHERE work_id = '{selected_work}' ORDER BY created_ts DESC")
            if upd.empty:
                st.info("ยังไม่มี update")
            else:
                st.dataframe(upd, use_container_width=True, hide_index=True)

# -------------------------
# Page: Data Import
# -------------------------
elif page == "Data Import":
    st.markdown("## ⬆️ Data Import (CSV → Replace Table)")
    st.caption("MVP: Import จะ **replace ทั้งตาราง** เพื่อความง่ายในการทดสอบ/เดโม")

    table = st.selectbox("เลือกตาราง", list(TABLE_COLUMNS.keys()))
    st.markdown("### 1) ดาวน์โหลด Template CSV")
    st.download_button(
        label=f"Download template: {table}.csv",
        data=make_template_csv(table),
        file_name=f"{table}.csv",
        mime="text/csv",
    )

    st.markdown("### 2) อัปโหลด CSV เพื่อ Import")
    up = st.file_uploader("Upload CSV", type=["csv"])
    if up is not None:
        df = pd.read_csv(up)
        st.markdown("### Preview")
        st.dataframe(df.head(50), use_container_width=True)

        missing = [c for c in TABLE_COLUMNS[table] if c not in df.columns]
        if missing:
            st.error(f"CSV ขาดคอลัมน์: {missing}")

        if st.button("Import (Replace Table)"):
            replace_table(table, df)
            st.success(f"Imported -> {table}")
            st.rerun()

# -------------------------
# Page: Schema / Templates
# -------------------------
else:
    st.markdown("## 🧾 Schema / Templates")
    st.write("ตารางทั้งหมดที่ใช้ใน MVP+ นี้:")
    for t, cols in TABLE_COLUMNS.items():
        with st.expander(t):
            st.code(", ".join(cols))

    st.markdown("### แนวทางใช้งานจริงแบบแผนกกรอกงาน")
    st.markdown("""
- ให้แต่ละแผนกใช้หน้า **Department Workspace** เพื่อบันทึก *งานรับผิดชอบ* (work items)\n
- ทุกงานควรมี: เจ้าภาพ, กำหนดเสร็จ, สถานะ, % ความคืบหน้า, ความเสี่ยง, และติ๊ก *ต้องการการตัดสินใจ* เมื่อจำเป็น\n
- แนบหลักฐาน/เอกสารประกอบไว้ที่งานนั้น เพื่อให้ตรวจสอบย้อนหลังได้\n
""")

