import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="SMD Dataverse Platform",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Cyberpunk/Professional Look
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    .metric-card {
        background-color: #1e2530;
        border: 1px solid #334155;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    /* Custom divider */
    hr { border-top: 1px solid #334155; }
    
    /* Animation for Pulse */
    @keyframes pulse-ring {
        0% { transform: scale(0.33); opacity: 1; }
        80%, 100% { opacity: 0; }
    }
    .pulse-dot {
        position: relative;
    }
    .pulse-dot::before {
        content: '';
        position: relative;
        display: block;
        width: 100%;
        height: 100%;
        box-shadow: 0 0 8px #06b6d4;
        border-radius: 50%;
        animation: pulse-ring 1.25s cubic-bezier(0.215, 0.61, 0.355, 1) infinite;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. SESSION STATE (Mock Database) ---
if 'db_mds' not in st.session_state:
    st.session_state.db_mds = [
        {"timestamp": "09:00:00", "name": "Thai Rung Ruang", "source": "SME Project", "value": "Hot", "meta": "High Value"},
        {"timestamp": "09:15:00", "name": "ABC Logistics", "source": "Website", "value": "Warm", "meta": "Medium Value"},
        {"timestamp": "10:30:00", "name": "Green Eco", "source": "Facebook", "value": "Cold", "meta": "Low Value"}
    ]
if 'db_sgs' not in st.session_state:
    st.session_state.db_sgs = [
        {"timestamp": "08:30:00", "name": "AI Grant Phase 1", "funding": 100, "risk": "High"},
        {"timestamp": "11:00:00", "name": "ESG Training", "funding": 50, "risk": "Low"}
    ]
if 'db_bms' not in st.session_state:
    st.session_state.db_bms = [
        {"timestamp": "Yesterday", "topic": "Q1 Budget", "status": "Approved"},
        {"timestamp": "Today", "topic": "Policy Update", "status": "Pending"}
    ]
if 'db_it' not in st.session_state:
    st.session_state.db_it = [
        {"timestamp": "00:00:00", "server": "Gateway A", "uptime": 99.9, "status": "Normal"},
        {"timestamp": "06:00:00", "server": "Database B", "uptime": 99.9, "status": "Normal"}
    ]

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("💎 SMD Dataverse")
st.sidebar.caption("v4.0 | Python Streamlit Edition")

menu = st.sidebar.radio(
    "Navigation",
    ["Ecosystem Map", "Input Portal", "Smart Reporting", "Dataverse Explorer"]
)

st.sidebar.markdown("---")
st.sidebar.info(f"**System Status:** Online 🟢\n\n**Time:** {datetime.now().strftime('%H:%M:%S')}")

# --- 4. PAGE: ECOSYSTEM MAP ---
if menu == "Ecosystem Map":
    st.title("🌐 Ecosystem Overview")
    st.markdown("Visualizing Data Flow: **Input Nodes** → **Central Hub** → **Value Creation**")

    # Metrics Row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("MDS Leads (MongoDB)", len(st.session_state.db_mds), "Sensor Active")
    c2.metric("SGS Projects (PostgreSQL)", len(st.session_state.db_sgs), f"{sum(x['funding'] for x in st.session_state.db_sgs)} MB")
    c3.metric("BMS Docs (Elasticsearch)", len(st.session_state.db_bms), "Indexed")
    c4.metric("IT Uptime (Timescale)", "99.99%", "Stable")

    st.markdown("---")

    # ECOSYSTEM VISUALIZATION (Using Graphviz for Python)
    # This creates the diagram dynamically
    import graphviz
    
    # Logic to colorize nodes if they have data
    mds_color = "lightblue" if len(st.session_state.db_mds) > 0 else "white"
    
    graph = graphviz.Digraph()
    graph.attr(rankdir='LR', bgcolor='#0e1117')
    
    # Nodes
    graph.attr('node', shape='box', style='filled', fontname='Helvetica', color='white')
    
    graph.node('MDS', label=f'MDS (Sensor)\n{len(st.session_state.db_mds)} Leads', fillcolor='#06b6d4', fontcolor='white')
    graph.node('SGS', label=f'SGS (Navigator)\n{len(st.session_state.db_sgs)} Projects', fillcolor='#f59e0b', fontcolor='black')
    graph.node('BMS', label=f'BMS (Governor)\n{len(st.session_state.db_bms)} Docs', fillcolor='#10b981', fontcolor='white')
    graph.node('IT', label='IT (Backbone)\nInfrastructure', fillcolor='#8b5cf6', fontcolor='white')
    
    graph.attr('node', shape='circle', style='filled', width='1.5', fixedsize='true', fontsize='12')
    graph.node('HUB', label='Central\nHub\n(Kafka)', fillcolor='#fbbf24', fontcolor='black', penwidth='3')

    graph.attr('node', shape='doublecircle', fillcolor='white', fontcolor='black')
    graph.node('VALUE', label='Value\nCreation', fillcolor='#a78bfa')

    # Edges
    graph.edge('MDS', 'HUB', color='white', style='dashed', label='JSON')
    graph.edge('SGS', 'HUB', color='white', style='dashed', label='SQL')
    graph.edge('BMS', 'HUB', color='white', style='dashed', label='Logs')
    graph.edge('IT', 'HUB', color='white', style='dashed', label='Metrics')
    graph.edge('HUB', 'VALUE', color='#fbbf24', penwidth='2', label='Insight')

    c_viz, c_log = st.columns([2, 1])
    
    with c_viz:
        st.graphviz_chart(graph, use_container_width=True)
    
    with c_log:
        st.subheader("📡 Live Ingestion Log")
        log_df = pd.DataFrame(st.session_state.db_mds[-5:]).sort_index(ascending=False)
        if not log_df.empty:
            for idx, row in log_df.iterrows():
                st.code(f"[{row['timestamp']}] MDS Ingested: {row['name']} ({row['value']})", language="bash")
        else:
            st.info("No logs available.")

# --- 5. PAGE: INPUT PORTAL ---
elif menu == "Input Portal":
    st.title("📥 Data Ingestion Portal")
    
    tab1, tab2, tab3, tab4 = st.tabs(["MDS (Marketing)", "SGS (Strategy)", "BMS (Governance)", "IT (Ops)"])
    
    with tab1:
        st.subheader("MDS Sensor Input")
        with st.form("mds_form"):
            c1, c2 = st.columns(2)
            name = c1.text_input("Customer / Lead Name")
            source = c2.selectbox("Source", ["SME Project", "Website", "Facebook"])
            value = st.select_slider("Lead Temperature", options=["Cold", "Warm", "Hot"])
            
            submitted = st.form_submit_button("Ingest to MongoDB")
            if submitted:
                new_rec = {
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "name": name, "source": source, "value": value, "meta": "Live Entry"
                }
                st.session_state.db_mds.append(new_rec)
                st.success(f"Lead '{name}' ingested successfully!")
                time.sleep(1)
                st.rerun()

    with tab2:
        st.subheader("SGS Navigator Input")
        with st.form("sgs_form"):
            name = st.text_input("Project Name")
            funding = st.number_input("Budget (MB)", min_value=0)
            risk = st.selectbox("Risk Level", ["Low", "Medium", "High"])
            
            submitted = st.form_submit_button("Ingest to PostgreSQL")
            if submitted:
                st.session_state.db_sgs.append({
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "name": name, "funding": funding, "risk": risk
                })
                st.success("Project Strategy Updated!")
                time.sleep(1)
                st.rerun()

# --- 6. PAGE: SMART REPORTING ---
elif menu == "Smart Reporting":
    st.title("📊 Smart Analytics Console")
    
    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        dept = st.selectbox("Select Department", ["MDS", "SGS", "BMS", "IT"])
        
    # Logic based on selection
    if dept == "MDS":
        st.subheader("Marketing Funnel Analysis")
        
        # Prepare Data
        df = pd.DataFrame(st.session_state.db_mds)
        if not df.empty:
            funnel_counts = df['value'].value_counts().reset_index()
            funnel_counts.columns = ['Stage', 'Count']
            
            # Chart
            fig = px.funnel(funnel_counts, x='Count', y='Stage', color='Stage', 
                            color_discrete_sequence=px.colors.sequential.Bluyl)
            st.plotly_chart(fig, use_container_width=True)
            
            # AI Insight Mockup
            st.info(f"💡 **AI Insight:** You have {len(df[df['value']=='Hot'])} Hot Leads ready for closing. Conversion rate is trending up.")
        else:
            st.warning("No data found in MDS Database.")

    elif dept == "SGS":
        st.subheader("Strategic Risk & Budget Profile")
        df = pd.DataFrame(st.session_state.db_sgs)
        
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1:
                # Pie Chart
                fig_pie = px.pie(df, names='risk', title='Project Risk Distribution', 
                                 color_discrete_map={'High':'red', 'Medium':'orange', 'Low':'green'})
                st.plotly_chart(fig_pie, use_container_width=True)
            with c2:
                # Bar Chart
                fig_bar = px.bar(df, x='name', y='funding', title='Funding Allocation (MB)', color='funding')
                st.plotly_chart(fig_bar, use_container_width=True)
                
            total_fund = df['funding'].sum()
            st.info(f"💡 **AI Insight:** Total funding secured is **{total_fund} MB**. High risk projects account for {len(df[df['risk']=='High'])} items.")

# --- 7. PAGE: DATAVERSE EXPLORER ---
elif menu == "Dataverse Explorer":
    st.title("💾 Dataverse (Raw Data Layer)")
    st.markdown("Direct access to the underlying data stores (Simulated).")
    
    tab_mds, tab_sgs, tab_bms = st.tabs(["MongoDB (MDS)", "PostgreSQL (SGS)", "Elasticsearch (BMS)"])
    
    with tab_mds:
        st.dataframe(pd.DataFrame(st.session_state.db_mds), use_container_width=True)
    with tab_sgs:
        st.dataframe(pd.DataFrame(st.session_state.db_sgs), use_container_width=True)
    with tab_bms:
        st.dataframe(pd.DataFrame(st.session_state.db_bms), use_container_width=True)