import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta

# =========================================================
# MASCI Intelligence Hub (MVP) - Clean Streamlit App
# - SQLite embedded DB (good for PoC / demo on Streamlit Cloud)
# - Physical tables: dim_* + fact_* (replace-table import via CSV)
# - Executive View: 1-page dashboard (health, execution, risk, signals, actions)
# =========================================================

st.set_page_config(
    page_title="Intelligence Hub (MVP)",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path("intelligence_hub.db")

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

    conn.commit()
    conn.close()

def read_df(sql: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(sql, conn)
    finally:
        conn.close()
    return df

def replace_table(table: str, df: pd.DataFrame) -> None:
    conn = get_conn()
    try:
        df.to_sql(table, conn, if_exists="replace", index=False)
    finally:
        conn.close()

def ensure_dim_date(start: date, end: date) -> None:
    """Create dim_date rows for [start, end] inclusive. date_id = YYYYMMDD."""
    conn = get_conn()
    cur = conn.cursor()
    d = start
    while d <= end:
        date_id = int(d.strftime("%Y%m%d"))
        cur.execute("""
            INSERT OR IGNORE INTO dim_date(date_id, date, month, quarter, year)
            VALUES (?, ?, ?, ?, ?)
        """, (date_id, d.isoformat(), d.month, (d.month - 1)//3 + 1, d.year))
        d = d + timedelta(days=1)
    conn.commit()
    conn.close()

# -------------------------
# Demo seed + templates
# -------------------------
TABLE_COLUMNS = {
    "dim_strategy": ["strategy_id","strategy_name","strategy_owner","strategy_level","start_year","end_year"],
    "dim_kpi": ["kpi_id","kpi_name","kpi_definition","calculation_logic","kpi_owner","refresh_frequency"],
    "dim_person": ["person_id","person_name","role","department"],
    "dim_organization": ["organization_id","organization_name","org_type","sector"],
    "dim_risk_category": ["risk_category_id","risk_category_name","governance_area"],
    "dim_source": ["source_id","source_type","source_name"],
    "dim_topic": ["topic_id","topic_name"],
    "fact_strategic_kpi": ["kpi_fact_id","date_id","strategy_id","organization_id","kpi_id","actual_value","target_value","variance_value","kpi_status","last_updated_ts"],
    "fact_strategy_project": ["project_fact_id","strategy_id","project_name","owner_person_id","start_date_id","end_date_id","progress_percent","budget_plan","budget_used","project_status","risk_level"],
    "fact_risk_event": ["risk_event_id","date_id","organization_id","risk_category_id","risk_description","likelihood_score","impact_score","risk_score","mitigation_plan","mitigation_status"],
    "fact_external_signal": ["signal_id","date_id","source_id","topic_id","signal_type","relevance_score","sentiment_score","linked_strategy_id"],
}

def make_template_csv(table: str) -> bytes:
    df = pd.DataFrame(columns=TABLE_COLUMNS[table])
    return df.to_csv(index=False).encode("utf-8")

def seed_demo_data() -> None:
    today = date.today()
    ensure_dim_date(today - timedelta(days=120), today)

    dim_strategy = pd.DataFrame([
        {"strategy_id":"S1","strategy_name":"ยกระดับคุณภาพบริการรับรอง","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
        {"strategy_id":"S2","strategy_name":"Digital Transformation","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
        {"strategy_id":"S3","strategy_name":"การบริหารความเสี่ยง/ธรรมาภิบาล","strategy_owner":"Director","strategy_level":"องค์กร","start_year":2026,"end_year":2028},
    ])
    dim_kpi = pd.DataFrame([
        {"kpi_id":"K1","kpi_name":"On-time Delivery","kpi_definition":"งานส่งมอบตามกำหนด","calculation_logic":"on_time/total","kpi_owner":"Strategy","refresh_frequency":"Weekly"},
        {"kpi_id":"K2","kpi_name":"Customer Satisfaction","kpi_definition":"ความพึงพอใจลูกค้า","calculation_logic":"avg_score","kpi_owner":"Strategy","refresh_frequency":"Monthly"},
        {"kpi_id":"K3","kpi_name":"Compliance Findings","kpi_definition":"ประเด็นไม่สอดคล้อง","calculation_logic":"count(findings)","kpi_owner":"Governance","refresh_frequency":"Monthly"},
        {"kpi_id":"K4","kpi_name":"Digital Project Progress","kpi_definition":"ความคืบหน้าโครงการดิจิทัล","calculation_logic":"avg(progress)","kpi_owner":"IT/Strategy","refresh_frequency":"Weekly"},
        {"kpi_id":"K5","kpi_name":"Revenue Pipeline","kpi_definition":"แนวโน้มรายได้","calculation_logic":"sum(pipeline)","kpi_owner":"Finance","refresh_frequency":"Weekly"},
    ])
    dim_org = pd.DataFrame([
        {"organization_id":"ORG1","organization_name":"MASCI (HQ)","org_type":"Foundation","sector":"Certification"},
    ])
    dim_person = pd.DataFrame([
        {"person_id":"P1","person_name":"Owner A","role":"Project Owner","department":"Strategy"},
        {"person_id":"P2","person_name":"Owner B","role":"Project Owner","department":"IT"},
    ])
    dim_risk_cat = pd.DataFrame([
        {"risk_category_id":"R1","risk_category_name":"Operational","governance_area":"Operations"},
        {"risk_category_id":"R2","risk_category_name":"Compliance","governance_area":"Governance"},
        {"risk_category_id":"R3","risk_category_name":"Strategic","governance_area":"Strategy"},
    ])
    dim_source = pd.DataFrame([
        {"source_id":"SRC1","source_type":"External","source_name":"Regulator"},
        {"source_id":"SRC2","source_type":"External","source_name":"Market"},
    ])
    dim_topic = pd.DataFrame([
        {"topic_id":"T1","topic_name":"Regulation"},
        {"topic_id":"T2","topic_name":"Market Trend"},
        {"topic_id":"T3","topic_name":"Technology"},
    ])

    replace_table("dim_strategy", dim_strategy)
    replace_table("dim_kpi", dim_kpi)
    replace_table("dim_organization", dim_org)
    replace_table("dim_person", dim_person)
    replace_table("dim_risk_category", dim_risk_cat)
    replace_table("dim_source", dim_source)
    replace_table("dim_topic", dim_topic)

    d0 = int(today.strftime("%Y%m%d"))
    d1 = int((today - timedelta(days=30)).strftime("%Y%m%d"))

    fact_kpi = pd.DataFrame([
        {"kpi_fact_id":"F_KPI_1","date_id":d0,"strategy_id":"S1","organization_id":"ORG1","kpi_id":"K1","actual_value":0.91,"target_value":0.95,"variance_value":-0.04,"kpi_status":"Amber","last_updated_ts":datetime.now().isoformat()},
        {"kpi_fact_id":"F_KPI_2","date_id":d0,"strategy_id":"S1","organization_id":"ORG1","kpi_id":"K2","actual_value":4.2,"target_value":4.5,"variance_value":-0.3,"kpi_status":"Amber","last_updated_ts":datetime.now().isoformat()},
        {"kpi_fact_id":"F_KPI_3","date_id":d0,"strategy_id":"S3","organization_id":"ORG1","kpi_id":"K3","actual_value":12,"target_value":8,"variance_value":4,"kpi_status":"Red","last_updated_ts":datetime.now().isoformat()},
        {"kpi_fact_id":"F_KPI_4","date_id":d0,"strategy_id":"S2","organization_id":"ORG1","kpi_id":"K4","actual_value":0.55,"target_value":0.60,"variance_value":-0.05,"kpi_status":"Amber","last_updated_ts":datetime.now().isoformat()},
        {"kpi_fact_id":"F_KPI_5","date_id":d0,"strategy_id":"S1","organization_id":"ORG1","kpi_id":"K5","actual_value":32.0,"target_value":30.0,"variance_value":2.0,"kpi_status":"Green","last_updated_ts":datetime.now().isoformat()},
    ])
    replace_table("fact_strategic_kpi", fact_kpi)

    fact_proj = pd.DataFrame([
        {"project_fact_id":"PRJ_1","strategy_id":"S2","project_name":"Data Catalog & KPI Dictionary","owner_person_id":"P1","start_date_id":d1,"end_date_id":d0,"progress_percent":65,"budget_plan":800000,"budget_used":520000,"project_status":"In Progress","risk_level":"Medium"},
        {"project_fact_id":"PRJ_2","strategy_id":"S2","project_name":"Executive Dashboard v1","owner_person_id":"P2","start_date_id":d1,"end_date_id":d0,"progress_percent":45,"budget_plan":600000,"budget_used":300000,"project_status":"In Progress","risk_level":"High"},
    ])
    replace_table("fact_strategy_project", fact_proj)

    fact_risk = pd.DataFrame([
        {"risk_event_id":"RSK_1","date_id":d0,"organization_id":"ORG1","risk_category_id":"R2","risk_description":"เอกสารหลักฐานไม่ครบตามรอบ","likelihood_score":4,"impact_score":4,"risk_score":16,"mitigation_plan":"ทำ checklist + training","mitigation_status":"Open"},
        {"risk_event_id":"RSK_2","date_id":d0,"organization_id":"ORG1","risk_category_id":"R1","risk_description":"ทรัพยากรผู้ตรวจไม่พอใน peak season","likelihood_score":3,"impact_score":5,"risk_score":15,"mitigation_plan":"ปรับแผน capacity","mitigation_status":"In Progress"},
        {"risk_event_id":"RSK_3","date_id":d0,"organization_id":"ORG1","risk_category_id":"R3","risk_description":"การเปลี่ยนแปลงข้อกำหนดภายนอกกระทบ scope","likelihood_score":3,"impact_score":4,"risk_score":12,"mitigation_plan":"ติดตามประกาศ + gap review","mitigation_status":"Open"},
    ])
    replace_table("fact_risk_event", fact_risk)

    fact_signal = pd.DataFrame([
        {"signal_id":"SIG_1","date_id":d0,"source_id":"SRC1","topic_id":"T1","signal_type":"Risk","relevance_score":0.9,"sentiment_score":-0.4,"linked_strategy_id":"S3"},
        {"signal_id":"SIG_2","date_id":d0,"source_id":"SRC2","topic_id":"T2","signal_type":"Opportunity","relevance_score":0.7,"sentiment_score":0.2,"linked_strategy_id":"S1"},
        {"signal_id":"SIG_3","date_id":d0,"source_id":"SRC2","topic_id":"T3","signal_type":"Opportunity","relevance_score":0.8,"sentiment_score":0.4,"linked_strategy_id":"S2"},
    ])
    replace_table("fact_external_signal", fact_signal)

# -------------------------
# Load for Executive View
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
    return kpi, projects, risks, signals

# -------------------------
# App
# -------------------------
init_db()

with st.sidebar:
    st.title("🧠 Intelligence Hub")
    st.caption("MVP (Clean) • Strategy • Risk • Signals")
    st.divider()

    page = st.radio("เมนู", ["Executive View", "Data Import", "Schema / Templates"], index=0)

    st.divider()
    st.markdown("### As-of & Filters")
    as_of_txt = st.text_input("As-of date_id (YYYYMMDD)", value=date.today().strftime("%Y%m%d"))
    org_id = st.text_input("organization_id", value="ORG1")

    try:
        as_of_id = int(as_of_txt)
    except:
        as_of_id = int(date.today().strftime("%Y%m%d"))

# =========================
# Page: Executive View
# =========================
if page == "Executive View":
    st.markdown("## 📌 Executive View (1 หน้า)")
    st.caption("เปิดแล้วต้องรู้: สุขภาพองค์กร / ความคืบหน้า / ความเสี่ยง / สัญญาณภายนอก / สิ่งที่ต้องตัดสินใจ")

    kpi, projects, risks, signals = load_exec_data(as_of_id, org_id)

    top = st.columns([1,1,1,1,1])
    with top[0]:
        if st.button("🌱 Seed Demo Data"):
            seed_demo_data()
            st.success("Seed demo data เรียบร้อย")
            st.rerun()
    with top[1]:
        st.write("")
    with top[2]:
        st.write("")
    with top[3]:
        st.write("")
    with top[4]:
        st.write("")

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
                use_container_width=True,
                hide_index=True
            )
    with colB:
        st.markdown("### KPI Status Distribution")
        if total > 0:
            fig = px.pie(
                pd.DataFrame({"status":["Green","Amber","Red"], "count":[green, amber, red]}),
                values="count",
                names="status",
            )
            fig.update_layout(height=300, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Execution
    st.markdown("### 2) Strategy Execution Status")
    left, right = st.columns([1.2, 1])
    with left:
        if projects.empty:
            st.info("ยังไม่มีข้อมูล fact_strategy_project")
        else:
            st.dataframe(
                projects[["project_name","strategy_name","person_name","progress_percent","project_status","risk_level","budget_plan","budget_used"]],
                use_container_width=True,
                hide_index=True
            )
    with right:
        if not projects.empty:
            figp = px.bar(projects, x="project_name", y="progress_percent")
            figp.update_layout(height=320, margin=dict(l=10,r=10,t=10,b=10), yaxis=dict(range=[0, 100]))
            st.plotly_chart(figp, use_container_width=True)

    st.divider()

    # Risk
    st.markdown("### 3) Risk & Governance Radar")
    r1, r2 = st.columns([1.1, 1])
    with r1:
        if risks.empty:
            st.info("ยังไม่มีข้อมูล fact_risk_event")
        else:
            # Scatter: likelihood vs impact, bubble = risk_score
            fig_risk = px.scatter(
                risks,
                x="likelihood_score",
                y="impact_score",
                size="risk_score",
                hover_data=["risk_category_name","risk_description","mitigation_status","risk_score"],
            )
            fig_risk.update_layout(
                height=360,
                xaxis=dict(range=[0, 5]),
                yaxis=dict(range=[0, 5]),
                margin=dict(l=10,r=10,t=10,b=10),
            )
            st.plotly_chart(fig_risk, use_container_width=True)

    with r2:
        if not risks.empty:
            open_cnt = int(risks["mitigation_status"].isin(["Open", "In Progress"]).sum())
            st.metric("Risks ที่ยังเปิดอยู่", open_cnt)
            st.markdown("**Top Risks (คะแนนสูงสุด)**")
            top_r = risks.sort_values("risk_score", ascending=False).head(3)
            st.dataframe(
                top_r[["risk_category_name","risk_description","risk_score","mitigation_status"]],
                use_container_width=True,
                hide_index=True
            )

    st.divider()

    # Signals
    st.markdown("### 4) External Intelligence Signals")
    s1, s2 = st.columns([1.3, 1])
    with s1:
        if signals.empty:
            st.info("ยังไม่มีข้อมูล fact_external_signal")
        else:
            st.dataframe(
                signals[["topic_name","signal_type","relevance_score","sentiment_score","source_name","strategy_name"]],
                use_container_width=True,
                hide_index=True
            )
    with s2:
        if not signals.empty:
            figs = px.scatter(
                signals,
                x="relevance_score",
                y="sentiment_score",
                color="signal_type",
                hover_data=["topic_name","source_name","strategy_name"]
            )
            figs.update_layout(height=320, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(figs, use_container_width=True)

    st.divider()

    # Action panel
    st.markdown("### 5) Executive Action Panel (สิ่งที่ต้องตัดสินใจ)")
    action_items = []

    if not kpi.empty:
        for _, row in kpi[kpi["kpi_status"] == "Red"].iterrows():
            action_items.append(
                f"⚠️ KPI แดง: **{row['kpi_name']}** ({row['strategy_name']}) → ขอแผนแก้ไข/เจ้าภาพ/เส้นตาย"
            )

    if not projects.empty:
        high = projects[projects["risk_level"].isin(["High","สูง","H"])]
        for _, row in high.iterrows():
            action_items.append(
                f"🚧 โครงการเสี่ยงสูง: **{row['project_name']}** → ขอผู้บริหารกำหนดแนวทาง/ทรัพยากร"
            )

    if not risks.empty:
        critical = risks[risks["risk_score"] >= 15]
        for _, row in critical.iterrows():
            action_items.append(
                f"🔥 Risk สูง: **{row['risk_category_name']}** (score {row['risk_score']}) → {row['mitigation_status']}"
            )

    if not signals.empty:
        high_rel = signals[signals["relevance_score"] >= 0.85]
        for _, row in high_rel.iterrows():
            action_items.append(
                f"📣 Signal สำคัญ: **{row['topic_name']}** ({row['signal_type']}) กระทบ {row['strategy_name']}"
            )

    if not action_items:
        st.success("ไม่มีประเด็นเร่งด่วน (หรือยังไม่มีข้อมูล) — ลอง Seed/Import แล้วรีเฟรช")
    else:
        for a in action_items[:12]:
            st.write(a)

    with st.expander("🔎 Data Workbench"):
        t1, t2, t3, t4 = st.tabs(["KPI Facts", "Projects", "Risks", "Signals"])
        with t1:
            st.dataframe(kpi, use_container_width=True)
        with t2:
            st.dataframe(projects, use_container_width=True)
        with t3:
            st.dataframe(risks, use_container_width=True)
        with t4:
            st.dataframe(signals, use_container_width=True)

# =========================
# Page: Data Import
# =========================
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
        extra = [c for c in df.columns if c not in TABLE_COLUMNS[table]]

        if missing:
            st.error(f"CSV ขาดคอลัมน์: {missing}")
        if extra:
            st.warning(f"CSV มีคอลัมน์เกิน (ไม่เป็นไรแต่จะเก็บไปด้วย): {extra}")

        if st.button("Import (Replace Table)"):
            replace_table(table, df)
            st.success(f"Imported -> {table}")
            st.rerun()

# =========================
# Page: Schema / Templates
# =========================
else:
    st.markdown("## 🧾 Schema / Templates")
    st.write("ตารางทั้งหมดที่ใช้ใน MVP นี้:")
    for t, cols in TABLE_COLUMNS.items():
        with st.expander(t):
            st.code(", ".join(cols))

    st.markdown("### Quick Start (Streamlit Cloud)")
    st.markdown("""
1) สร้าง GitHub repo แล้วใส่ไฟล์ `app.py` และ `requirements.txt`\n
2) ไปที่ Streamlit Cloud → New app → เลือก repo/branch → main file = `app.py`\n
3) เปิดแอป → กด **Seed Demo Data** หรือ Import CSV เพื่อใช้ข้อมูลจริง\n
""")
