import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import io
import base64
from datetime import datetime, timedelta
import random
import hashlib

# ============================================================
# 1. PAGE CONFIGURATION
# ============================================================
st.set_page_config(
    page_title="SMD Dataverse Platform",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 2. CUSTOM CSS STYLING
# ============================================================
st.markdown("""
<style>
    /* Main Theme */
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
    }
    
    /* Cards */
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border: 1px solid #475569;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        margin-bottom: 1rem;
    }
    
    .metric-card h3 {
        color: #94a3b8;
        font-size: 0.85rem;
        margin-bottom: 0.5rem;
    }
    
    .metric-card .value {
        color: #f1f5f9;
        font-size: 2rem;
        font-weight: bold;
    }
    
    /* Status Badges */
    .badge {
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .badge-success { background: #065f46; color: #6ee7b7; }
    .badge-warning { background: #78350f; color: #fcd34d; }
    .badge-danger { background: #7f1d1d; color: #fca5a5; }
    .badge-info { background: #1e3a5f; color: #7dd3fc; }
    
    /* Data Table */
    .dataframe { font-size: 0.85rem !important; }
    
    /* Tabs Styling */
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
        padding: 0.5rem 1rem;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #3b82f6 !important;
        color: white !important;
    }
    
    /* Section Headers */
    .section-header {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    
    /* Upload Zone */
    .upload-zone {
        border: 2px dashed #475569;
        border-radius: 12px;
        padding: 2rem;
        text-align: center;
        background: rgba(30, 41, 59, 0.5);
        transition: all 0.3s ease;
    }
    
    .upload-zone:hover {
        border-color: #3b82f6;
        background: rgba(59, 130, 246, 0.1);
    }
    
    /* Hide Streamlit Branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom Scrollbar */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #1e293b; }
    ::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #64748b; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# 3. SESSION STATE INITIALIZATION
# ============================================================
def init_session_state():
    """Initialize all session state variables"""
    
    # Master Data Tables
    if 'master_data' not in st.session_state:
        st.session_state.master_data = {
            'customers': pd.DataFrame({
                'id': ['CUS001', 'CUS002', 'CUS003'],
                'name': ['Thai Rung Ruang Co.', 'ABC Logistics', 'Green Eco Ltd.'],
                'type': ['Corporate', 'SME', 'Corporate'],
                'industry': ['Manufacturing', 'Logistics', 'Energy'],
                'status': ['Active', 'Active', 'Prospect'],
                'created_at': [datetime.now() - timedelta(days=x) for x in [30, 20, 10]]
            }),
            'products': pd.DataFrame({
                'id': ['PRD001', 'PRD002', 'PRD003'],
                'name': ['SME Loan', 'Trade Finance', 'Green Bond'],
                'category': ['Lending', 'Trade', 'Investment'],
                'min_amount': [1000000, 5000000, 10000000],
                'max_amount': [50000000, 100000000, 500000000],
                'status': ['Active', 'Active', 'Active']
            }),
            'employees': pd.DataFrame({
                'id': ['EMP001', 'EMP002', 'EMP003'],
                'name': ['สมชาย ใจดี', 'สมหญิง รักงาน', 'สมศักดิ์ เก่งกาจ'],
                'department': ['MDS', 'SGS', 'BMS'],
                'position': ['Manager', 'Analyst', 'Director'],
                'email': ['somchai@smd.com', 'somying@smd.com', 'somsak@smd.com']
            }),
            'projects': pd.DataFrame({
                'id': ['PRJ001', 'PRJ002'],
                'name': ['AI Grant Phase 1', 'ESG Training'],
                'department': ['SGS', 'BMS'],
                'budget': [100000000, 50000000],
                'status': ['In Progress', 'Planning'],
                'risk_level': ['High', 'Low']
            })
        }
    
    # Transaction Data (MDS, SGS, BMS, IT)
    if 'transactions' not in st.session_state:
        st.session_state.transactions = {
            'mds': pd.DataFrame({
                'id': ['MDS001', 'MDS002', 'MDS003'],
                'timestamp': [datetime.now() - timedelta(hours=x) for x in [2, 5, 8]],
                'customer_id': ['CUS001', 'CUS002', 'CUS003'],
                'lead_name': ['Thai Rung Ruang', 'ABC Logistics', 'Green Eco'],
                'source': ['SME Project', 'Website', 'Facebook'],
                'value': [5000000, 2000000, 8000000],
                'status': ['Hot', 'Warm', 'Cold'],
                'assigned_to': ['EMP001', 'EMP001', 'EMP002']
            }),
            'sgs': pd.DataFrame({
                'id': ['SGS001', 'SGS002'],
                'timestamp': [datetime.now() - timedelta(days=x) for x in [1, 3]],
                'project_id': ['PRJ001', 'PRJ002'],
                'project_name': ['AI Grant Phase 1', 'ESG Training'],
                'funding': [100, 50],
                'risk': ['High', 'Low'],
                'progress': [45, 20]
            }),
            'bms': pd.DataFrame({
                'id': ['BMS001', 'BMS002'],
                'timestamp': [datetime.now() - timedelta(days=x) for x in [0, 1]],
                'topic': ['Q1 Budget Approval', 'Policy Update Review'],
                'meeting_date': [datetime.now().date(), datetime.now().date() - timedelta(days=1)],
                'status': ['Approved', 'Pending'],
                'participants': [5, 8],
                'resolution': ['Approved with conditions', 'Pending review']
            }),
            'it': pd.DataFrame({
                'id': ['IT001', 'IT002', 'IT003'],
                'timestamp': [datetime.now() - timedelta(hours=x) for x in [1, 3, 6]],
                'server': ['Gateway A', 'Database B', 'API Server'],
                'uptime': [99.9, 99.8, 99.95],
                'status': ['Normal', 'Normal', 'Normal'],
                'cpu_usage': [45, 62, 38],
                'memory_usage': [68, 75, 52]
            })
        }
    
    # Uploaded Files History
    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = []
    
    # Report Templates
    if 'report_templates' not in st.session_state:
        st.session_state.report_templates = [
            {'id': 'RPT001', 'name': 'Daily Sales Report', 'type': 'MDS', 'format': 'Excel'},
            {'id': 'RPT002', 'name': 'Risk Assessment Report', 'type': 'SGS', 'format': 'PDF'},
            {'id': 'RPT003', 'name': 'Board Meeting Summary', 'type': 'BMS', 'format': 'Word'},
        ]

init_session_state()

# ============================================================
# 4. UTILITY FUNCTIONS
# ============================================================

def generate_id(prefix):
    """Generate unique ID"""
    return f"{prefix}{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6].upper()}"

def parse_excel(file):
    """Parse Excel file and return DataFrame"""
    try:
        # Try with openpyxl first
        df = pd.read_excel(file, engine='openpyxl')
        return df, None
    except Exception as e1:
        try:
            # Fallback without specifying engine
            file.seek(0)  # Reset file pointer
            df = pd.read_excel(file)
            return df, None
        except Exception as e2:
            return None, str(e2)

def parse_csv(file):
    """Parse CSV file and return DataFrame"""
    try:
        df = pd.read_csv(file)
        return df, None
    except Exception as e:
        return None, str(e)

def parse_pdf(file):
    """Parse PDF file - extract text (simplified)"""
    try:
        return {"status": "success", "message": "PDF uploaded successfully", "pages": "N/A"}, None
    except Exception as e:
        return None, str(e)

def parse_word(file):
    """Parse Word file - extract text (simplified)"""
    try:
        return {"status": "success", "message": "Word document uploaded successfully"}, None
    except Exception as e:
        return None, str(e)

def create_download_link(df, filename, file_format):
    """Create download link for various formats"""
    if file_format == 'excel':
        output = io.BytesIO()
        try:
            # Try xlsxwriter first (more reliable on cloud)
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Data')
        except Exception:
            # Fallback to openpyxl
            try:
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
            except Exception:
                # Final fallback - return CSV instead
                csv = df.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()
                return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">📥 Download CSV (Excel unavailable)</a>'
        b64 = base64.b64encode(output.getvalue()).decode()
        return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}.xlsx">📥 Download Excel</a>'
    
    elif file_format == 'csv':
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">📥 Download CSV</a>'
    
    elif file_format == 'json':
        json_str = df.to_json(orient='records', date_format='iso')
        b64 = base64.b64encode(json_str.encode()).decode()
        return f'<a href="data:file/json;base64,{b64}" download="{filename}.json">📥 Download JSON</a>'

def get_status_color(status):
    """Get color based on status"""
    colors = {
        'Hot': '#ef4444', 'Warm': '#f59e0b', 'Cold': '#3b82f6',
        'High': '#ef4444', 'Medium': '#f59e0b', 'Low': '#22c55e',
        'Active': '#22c55e', 'Inactive': '#6b7280', 'Prospect': '#8b5cf6',
        'Approved': '#22c55e', 'Pending': '#f59e0b', 'Rejected': '#ef4444',
        'Normal': '#22c55e', 'Warning': '#f59e0b', 'Critical': '#ef4444',
        'In Progress': '#3b82f6', 'Planning': '#8b5cf6', 'Completed': '#22c55e'
    }
    return colors.get(status, '#6b7280')

# ============================================================
# 5. SIDEBAR
# ============================================================

with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <div style="font-size: 3rem;">🧊</div>
        <h1 style="margin: 0; font-size: 1.5rem; background: linear-gradient(90deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">SMD Dataverse</h1>
        <p style="color: #64748b; font-size: 0.8rem; margin: 0;">Enterprise Data Platform v5.0</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # System Status
    st.markdown("### 📊 System Status")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Uptime", "99.9%", "0.1%")
    with col2:
        st.metric("Active Users", "24", "3")
    
    st.markdown("---")
    
    # Quick Stats
    st.markdown("### 📈 Quick Stats")
    st.markdown(f"""
    - **MDS Records:** {len(st.session_state.transactions['mds'])}
    - **SGS Projects:** {len(st.session_state.transactions['sgs'])}
    - **BMS Documents:** {len(st.session_state.transactions['bms'])}
    - **IT Systems:** {len(st.session_state.transactions['it'])}
    """)
    
    st.markdown("---")
    
    # Current Time
    st.markdown(f"**🕐 Server Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ============================================================
# 6. MAIN CONTENT - TAB NAVIGATION
# ============================================================

# Main Tabs
tabs = st.tabs([
    "🏠 Dashboard",
    "📥 Data Import",
    "📊 Master Data",
    "💼 MDS (Marketing)",
    "🧭 SGS (Strategy)",
    "⚖️ BMS (Governance)",
    "🖥️ IT (Operations)",
    "📝 Report Generator",
    "⚙️ Settings"
])

# ============================================================
# TAB 1: DASHBOARD
# ============================================================
with tabs[0]:
    st.markdown('<p class="section-header">📊 Executive Dashboard</p>', unsafe_allow_html=True)
    
    # Key Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_leads = len(st.session_state.transactions['mds'])
        hot_leads = len(st.session_state.transactions['mds'][st.session_state.transactions['mds']['status'] == 'Hot'])
        st.metric("Total Leads", total_leads, f"{hot_leads} Hot")
    
    with col2:
        total_value = st.session_state.transactions['mds']['value'].sum()
        st.metric("Pipeline Value", f"฿{total_value:,.0f}", "↑ 12%")
    
    with col3:
        total_projects = len(st.session_state.transactions['sgs'])
        st.metric("Active Projects", total_projects, "2 High Risk")
    
    with col4:
        avg_uptime = st.session_state.transactions['it']['uptime'].mean()
        st.metric("System Uptime", f"{avg_uptime:.2f}%", "Stable")
    
    st.markdown("---")
    
    # Charts Row
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Lead Funnel Analysis")
        mds_df = st.session_state.transactions['mds']
        funnel_data = mds_df['status'].value_counts().reset_index()
        funnel_data.columns = ['Status', 'Count']
        
        fig_funnel = px.funnel(
            funnel_data, x='Count', y='Status',
            color='Status',
            color_discrete_map={'Hot': '#ef4444', 'Warm': '#f59e0b', 'Cold': '#3b82f6'}
        )
        fig_funnel.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#94a3b8',
            showlegend=False,
            height=300
        )
        st.plotly_chart(fig_funnel, use_container_width=True)
    
    with col2:
        st.markdown("#### 🎯 Risk Distribution")
        sgs_df = st.session_state.transactions['sgs']
        risk_data = sgs_df['risk'].value_counts().reset_index()
        risk_data.columns = ['Risk Level', 'Count']
        
        fig_pie = px.pie(
            risk_data, values='Count', names='Risk Level',
            color='Risk Level',
            color_discrete_map={'High': '#ef4444', 'Medium': '#f59e0b', 'Low': '#22c55e'}
        )
        fig_pie.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#94a3b8',
            height=300
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    # Ecosystem Visualization
    st.markdown("---")
    st.markdown("#### 🌐 Ecosystem Data Flow")
    
    # Network Diagram using Plotly
    node_x = [0, 0, 0, 0, 2, 4]
    node_y = [3, 2, 1, 0, 1.5, 1.5]
    
    node_labels = [
        f"MDS<br>{len(st.session_state.transactions['mds'])} Records",
        f"SGS<br>{len(st.session_state.transactions['sgs'])} Projects",
        f"BMS<br>{len(st.session_state.transactions['bms'])} Docs",
        f"IT<br>{len(st.session_state.transactions['it'])} Systems",
        "Central<br>Hub",
        "Value<br>Creation"
    ]
    
    node_colors = ['#06b6d4', '#f59e0b', '#10b981', '#8b5cf6', '#fbbf24', '#a78bfa']
    
    # Create edges
    edge_traces = []
    edges = [(0, 4), (1, 4), (2, 4), (3, 4), (4, 5)]
    
    for src, dst in edges:
        edge_traces.append(go.Scatter(
            x=[node_x[src], node_x[dst]], y=[node_y[src], node_y[dst]],
            mode='lines',
            line=dict(width=2, color='#475569', dash='dash'),
            hoverinfo='none',
            showlegend=False
        ))
    
    # Create nodes
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=node_labels,
        textposition='middle center',
        textfont=dict(size=10, color='white'),
        marker=dict(size=[50, 50, 50, 50, 70, 60], color=node_colors, line=dict(width=2, color='white')),
        hoverinfo='text',
        showlegend=False
    )
    
    fig_network = go.Figure(data=edge_traces + [node_trace])
    fig_network.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-1, 5]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-0.5, 3.5]),
        height=350,
        margin=dict(l=20, r=20, t=20, b=20)
    )
    st.plotly_chart(fig_network, use_container_width=True)

# ============================================================
# TAB 2: DATA IMPORT
# ============================================================
with tabs[1]:
    st.markdown('<p class="section-header">📥 Data Import Center</p>', unsafe_allow_html=True)
    
    import_tabs = st.tabs(["📊 Excel/CSV", "📄 PDF", "📝 Word", "🔗 API Import", "📜 Import History"])
    
    # Excel/CSV Import
    with import_tabs[0]:
        st.markdown("### Import from Excel or CSV")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_file = st.file_uploader(
                "Choose Excel or CSV file",
                type=['xlsx', 'xls', 'csv'],
                key='excel_uploader'
            )
            
            if uploaded_file:
                st.success(f"📁 File uploaded: {uploaded_file.name}")
                
                # Parse file
                if uploaded_file.name.endswith('.csv'):
                    df, error = parse_csv(uploaded_file)
                else:
                    df, error = parse_excel(uploaded_file)
                
                if error:
                    st.error(f"Error parsing file: {error}")
                else:
                    st.markdown("#### Preview Data")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    st.markdown(f"**Total Rows:** {len(df)} | **Columns:** {len(df.columns)}")
                    
                    # Target Selection
                    st.markdown("---")
                    col_a, col_b = st.columns(2)
                    
                    with col_a:
                        target_table = st.selectbox(
                            "Import to Master Data Table",
                            ['customers', 'products', 'employees', 'projects']
                        )
                    
                    with col_b:
                        import_mode = st.radio(
                            "Import Mode",
                            ['Append', 'Replace'],
                            horizontal=True
                        )
                    
                    if st.button("🚀 Import Data", type="primary"):
                        # Add to session state
                        if import_mode == 'Append':
                            st.session_state.master_data[target_table] = pd.concat([
                                st.session_state.master_data[target_table], df
                            ], ignore_index=True)
                        else:
                            st.session_state.master_data[target_table] = df
                        
                        # Log upload
                        st.session_state.uploaded_files.append({
                            'filename': uploaded_file.name,
                            'type': 'Excel/CSV',
                            'target': target_table,
                            'rows': len(df),
                            'timestamp': datetime.now()
                        })
                        
                        st.success(f"✅ Successfully imported {len(df)} rows to {target_table}!")
                        st.balloons()
        
        with col2:
            st.markdown("### 📋 Import Guidelines")
            st.info("""
            **Supported Formats:**
            - Excel (.xlsx, .xls)
            - CSV (.csv)
            
            **Best Practices:**
            - Ensure headers in first row
            - Remove empty rows
            - Use consistent date formats
            - Check for duplicate IDs
            """)
            
            # Download Template
            st.markdown("### 📥 Download Templates")
            template_type = st.selectbox("Select Template", ['customers', 'products', 'employees', 'projects'])
            
            if st.button("Download Template"):
                template_df = st.session_state.master_data[template_type].head(0)
                st.markdown(create_download_link(template_df, f'{template_type}_template', 'excel'), unsafe_allow_html=True)
    
    # PDF Import
    with import_tabs[1]:
        st.markdown("### Import from PDF")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            pdf_file = st.file_uploader(
                "Upload PDF Document",
                type=['pdf'],
                key='pdf_uploader'
            )
            
            if pdf_file:
                st.success(f"📁 PDF uploaded: {pdf_file.name}")
                
                result, error = parse_pdf(pdf_file)
                
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.info("ℹ️ PDF parsing requires additional libraries (PyPDF2/pdfplumber). For demo, showing placeholder.")
                    
                    # Simulated extraction
                    st.markdown("#### Extracted Content Preview")
                    st.text_area("Content", "PDF content would be extracted here...", height=200)
                    
                    target_module = st.selectbox(
                        "Link to Module",
                        ['MDS - Marketing', 'SGS - Strategy', 'BMS - Governance', 'IT - Operations']
                    )
                    
                    if st.button("📎 Attach to Module", key='pdf_attach'):
                        st.session_state.uploaded_files.append({
                            'filename': pdf_file.name,
                            'type': 'PDF',
                            'target': target_module,
                            'rows': 'Document',
                            'timestamp': datetime.now()
                        })
                        st.success("✅ PDF attached successfully!")
        
        with col2:
            st.markdown("### 📋 PDF Processing")
            st.info("""
            **Capabilities:**
            - Text extraction
            - Table detection
            - Form field reading
            - Image extraction
            
            **Use Cases:**
            - Contract analysis
            - Report digitization
            - Document archival
            """)
    
    # Word Import
    with import_tabs[2]:
        st.markdown("### Import from Word Document")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            word_file = st.file_uploader(
                "Upload Word Document",
                type=['docx', 'doc'],
                key='word_uploader'
            )
            
            if word_file:
                st.success(f"📁 Word document uploaded: {word_file.name}")
                
                result, error = parse_word(word_file)
                
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.info("ℹ️ Word parsing requires python-docx library. For demo, showing placeholder.")
                    
                    st.markdown("#### Document Content Preview")
                    st.text_area("Content", "Word document content would be extracted here...", height=200)
                    
                    doc_type = st.selectbox(
                        "Document Type",
                        ['Meeting Minutes', 'Policy Document', 'Report', 'Contract', 'Other']
                    )
                    
                    if st.button("📎 Process Document", key='word_attach'):
                        st.session_state.uploaded_files.append({
                            'filename': word_file.name,
                            'type': 'Word',
                            'target': doc_type,
                            'rows': 'Document',
                            'timestamp': datetime.now()
                        })
                        st.success("✅ Document processed successfully!")
        
        with col2:
            st.markdown("### 📋 Word Processing")
            st.info("""
            **Capabilities:**
            - Full text extraction
            - Table parsing
            - Style detection
            - Metadata reading
            
            **Auto-Classification:**
            - Meeting minutes → BMS
            - Strategy docs → SGS
            - Marketing content → MDS
            """)
    
    # API Import
    with import_tabs[3]:
        st.markdown("### API Integration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Configure API Endpoint")
            api_url = st.text_input("API URL", placeholder="https://api.example.com/data")
            api_method = st.selectbox("Method", ['GET', 'POST'])
            api_headers = st.text_area("Headers (JSON)", '{"Authorization": "Bearer YOUR_TOKEN"}')
            
            if api_method == 'POST':
                api_body = st.text_area("Request Body (JSON)", '{}')
            
            if st.button("🔗 Connect & Fetch", type="primary"):
                st.info("API integration would fetch data here. Demo mode active.")
                
        with col2:
            st.markdown("#### Pre-configured Integrations")
            integrations = [
                {'name': 'CRM System', 'status': 'Connected', 'last_sync': '2 hours ago'},
                {'name': 'ERP System', 'status': 'Connected', 'last_sync': '1 hour ago'},
                {'name': 'HR System', 'status': 'Disconnected', 'last_sync': 'N/A'},
            ]
            
            for intg in integrations:
                status_color = '#22c55e' if intg['status'] == 'Connected' else '#ef4444'
                st.markdown(f"""
                <div style="background: #1e293b; padding: 1rem; border-radius: 8px; margin-bottom: 0.5rem; border-left: 3px solid {status_color};">
                    <strong>{intg['name']}</strong><br>
                    <small style="color: #64748b;">Status: {intg['status']} | Last sync: {intg['last_sync']}</small>
                </div>
                """, unsafe_allow_html=True)
    
    # Import History
    with import_tabs[4]:
        st.markdown("### 📜 Import History")
        
        if st.session_state.uploaded_files:
            history_df = pd.DataFrame(st.session_state.uploaded_files)
            history_df['timestamp'] = pd.to_datetime(history_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            st.dataframe(history_df, use_container_width=True)
            
            if st.button("🗑️ Clear History"):
                st.session_state.uploaded_files = []
                st.rerun()
        else:
            st.info("No import history yet. Upload some files to see the history.")

# ============================================================
# TAB 3: MASTER DATA
# ============================================================
with tabs[2]:
    st.markdown('<p class="section-header">📊 Master Data Management</p>', unsafe_allow_html=True)
    
    master_tabs = st.tabs(["👥 Customers", "📦 Products", "👔 Employees", "📋 Projects"])
    
    # Customers
    with master_tabs[0]:
        st.markdown("### 👥 Customer Master Data")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Editable dataframe
            edited_customers = st.data_editor(
                st.session_state.master_data['customers'],
                use_container_width=True,
                num_rows="dynamic",
                key="customers_editor"
            )
            st.session_state.master_data['customers'] = edited_customers
        
        with col2:
            st.markdown("#### Quick Add")
            with st.form("add_customer"):
                new_name = st.text_input("Customer Name")
                new_type = st.selectbox("Type", ['Corporate', 'SME', 'Individual'])
                new_industry = st.text_input("Industry")
                
                if st.form_submit_button("➕ Add Customer"):
                    new_row = pd.DataFrame({
                        'id': [generate_id('CUS')],
                        'name': [new_name],
                        'type': [new_type],
                        'industry': [new_industry],
                        'status': ['Active'],
                        'created_at': [datetime.now()]
                    })
                    st.session_state.master_data['customers'] = pd.concat([
                        st.session_state.master_data['customers'], new_row
                    ], ignore_index=True)
                    st.success("Customer added!")
                    st.rerun()
            
            # Export
            st.markdown("#### Export")
            export_format = st.selectbox("Format", ['Excel', 'CSV', 'JSON'], key='cust_export')
            if st.button("📥 Export Customers"):
                df = st.session_state.master_data['customers']
                if export_format == 'Excel':
                    st.markdown(create_download_link(df, 'customers', 'excel'), unsafe_allow_html=True)
                elif export_format == 'CSV':
                    st.markdown(create_download_link(df, 'customers', 'csv'), unsafe_allow_html=True)
                else:
                    st.markdown(create_download_link(df, 'customers', 'json'), unsafe_allow_html=True)
    
    # Products
    with master_tabs[1]:
        st.markdown("### 📦 Product Master Data")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            edited_products = st.data_editor(
                st.session_state.master_data['products'],
                use_container_width=True,
                num_rows="dynamic",
                key="products_editor"
            )
            st.session_state.master_data['products'] = edited_products
        
        with col2:
            st.markdown("#### Quick Add")
            with st.form("add_product"):
                prod_name = st.text_input("Product Name")
                prod_cat = st.selectbox("Category", ['Lending', 'Trade', 'Investment', 'Insurance'])
                prod_min = st.number_input("Min Amount", value=1000000)
                prod_max = st.number_input("Max Amount", value=50000000)
                
                if st.form_submit_button("➕ Add Product"):
                    new_row = pd.DataFrame({
                        'id': [generate_id('PRD')],
                        'name': [prod_name],
                        'category': [prod_cat],
                        'min_amount': [prod_min],
                        'max_amount': [prod_max],
                        'status': ['Active']
                    })
                    st.session_state.master_data['products'] = pd.concat([
                        st.session_state.master_data['products'], new_row
                    ], ignore_index=True)
                    st.success("Product added!")
                    st.rerun()
    
    # Employees
    with master_tabs[2]:
        st.markdown("### 👔 Employee Master Data")
        
        edited_employees = st.data_editor(
            st.session_state.master_data['employees'],
            use_container_width=True,
            num_rows="dynamic",
            key="employees_editor"
        )
        st.session_state.master_data['employees'] = edited_employees
    
    # Projects
    with master_tabs[3]:
        st.markdown("### 📋 Project Master Data")
        
        edited_projects = st.data_editor(
            st.session_state.master_data['projects'],
            use_container_width=True,
            num_rows="dynamic",
            key="projects_editor"
        )
        st.session_state.master_data['projects'] = edited_projects

# ============================================================
# TAB 4: MDS (Marketing)
# ============================================================
with tabs[3]:
    st.markdown('<p class="section-header">💼 MDS - Marketing Data System</p>', unsafe_allow_html=True)
    
    mds_tabs = st.tabs(["📊 Overview", "➕ New Lead", "📋 Lead Management", "📈 Analytics"])
    
    with mds_tabs[0]:
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        mds_df = st.session_state.transactions['mds']
        
        with col1:
            st.metric("Total Leads", len(mds_df))
        with col2:
            hot_count = len(mds_df[mds_df['status'] == 'Hot'])
            st.metric("Hot Leads", hot_count, f"{hot_count/max(len(mds_df),1)*100:.0f}%")
        with col3:
            total_value = mds_df['value'].sum()
            st.metric("Total Value", f"฿{total_value:,.0f}")
        with col4:
            avg_value = mds_df['value'].mean()
            st.metric("Avg Deal Size", f"฿{avg_value:,.0f}")
        
        st.markdown("---")
        
        # Recent Leads
        st.markdown("#### Recent Leads")
        st.dataframe(mds_df.sort_values('timestamp', ascending=False), use_container_width=True)
    
    with mds_tabs[1]:
        st.markdown("### ➕ Add New Lead")
        
        with st.form("new_lead_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                lead_name = st.text_input("Lead Name *")
                lead_source = st.selectbox("Source", ['SME Project', 'Website', 'Facebook', 'Referral', 'Cold Call'])
                lead_value = st.number_input("Estimated Value (฿)", min_value=0, value=1000000)
            
            with col2:
                lead_status = st.select_slider("Temperature", options=['Cold', 'Warm', 'Hot'])
                customer_link = st.selectbox(
                    "Link to Customer",
                    ['-- New Customer --'] + st.session_state.master_data['customers']['name'].tolist()
                )
                assigned_to = st.selectbox(
                    "Assigned To",
                    st.session_state.master_data['employees']['name'].tolist()
                )
            
            notes = st.text_area("Notes")
            
            if st.form_submit_button("💾 Save Lead", type="primary"):
                if lead_name:
                    new_lead = pd.DataFrame({
                        'id': [generate_id('MDS')],
                        'timestamp': [datetime.now()],
                        'customer_id': ['NEW' if customer_link == '-- New Customer --' else customer_link],
                        'lead_name': [lead_name],
                        'source': [lead_source],
                        'value': [lead_value],
                        'status': [lead_status],
                        'assigned_to': [assigned_to]
                    })
                    st.session_state.transactions['mds'] = pd.concat([
                        st.session_state.transactions['mds'], new_lead
                    ], ignore_index=True)
                    st.success(f"✅ Lead '{lead_name}' added successfully!")
                    st.balloons()
                else:
                    st.error("Please enter lead name")
    
    with mds_tabs[2]:
        st.markdown("### 📋 Lead Management")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            filter_status = st.multiselect("Filter by Status", ['Hot', 'Warm', 'Cold'], default=['Hot', 'Warm', 'Cold'])
        with col2:
            filter_source = st.multiselect("Filter by Source", mds_df['source'].unique().tolist(), default=mds_df['source'].unique().tolist())
        with col3:
            sort_by = st.selectbox("Sort by", ['timestamp', 'value', 'status'])
        
        # Filtered data
        filtered_df = mds_df[
            (mds_df['status'].isin(filter_status)) &
            (mds_df['source'].isin(filter_source))
        ].sort_values(sort_by, ascending=False)
        
        # Editable table
        edited_mds = st.data_editor(filtered_df, use_container_width=True, num_rows="dynamic")
    
    with mds_tabs[3]:
        st.markdown("### 📈 MDS Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Source distribution
            source_data = mds_df.groupby('source')['value'].sum().reset_index()
            fig_source = px.bar(source_data, x='source', y='value', title='Value by Source',
                               color='source', color_discrete_sequence=px.colors.qualitative.Set2)
            fig_source.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig_source, use_container_width=True)
        
        with col2:
            # Status timeline
            fig_timeline = px.scatter(mds_df, x='timestamp', y='value', color='status',
                                     title='Lead Timeline',
                                     color_discrete_map={'Hot': '#ef4444', 'Warm': '#f59e0b', 'Cold': '#3b82f6'})
            fig_timeline.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig_timeline, use_container_width=True)

# ============================================================
# TAB 5: SGS (Strategy)
# ============================================================
with tabs[4]:
    st.markdown('<p class="section-header">🧭 SGS - Strategic Guidance System</p>', unsafe_allow_html=True)
    
    sgs_tabs = st.tabs(["📊 Overview", "➕ New Project", "⚠️ Risk Matrix", "📈 Analytics"])
    
    with sgs_tabs[0]:
        sgs_df = st.session_state.transactions['sgs']
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Projects", len(sgs_df))
        with col2:
            total_funding = sgs_df['funding'].sum()
            st.metric("Total Funding", f"{total_funding} MB")
        with col3:
            high_risk = len(sgs_df[sgs_df['risk'] == 'High'])
            st.metric("High Risk Projects", high_risk)
        
        st.markdown("---")
        st.dataframe(sgs_df, use_container_width=True)
    
    with sgs_tabs[1]:
        st.markdown("### ➕ Add New Project")
        
        with st.form("new_project_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                proj_name = st.text_input("Project Name *")
                proj_funding = st.number_input("Budget (MB)", min_value=0, value=50)
            
            with col2:
                proj_risk = st.selectbox("Risk Level", ['Low', 'Medium', 'High'])
                proj_progress = st.slider("Progress %", 0, 100, 0)
            
            proj_desc = st.text_area("Description")
            
            if st.form_submit_button("💾 Save Project", type="primary"):
                if proj_name:
                    new_proj = pd.DataFrame({
                        'id': [generate_id('SGS')],
                        'timestamp': [datetime.now()],
                        'project_id': [generate_id('PRJ')],
                        'project_name': [proj_name],
                        'funding': [proj_funding],
                        'risk': [proj_risk],
                        'progress': [proj_progress]
                    })
                    st.session_state.transactions['sgs'] = pd.concat([
                        st.session_state.transactions['sgs'], new_proj
                    ], ignore_index=True)
                    st.success(f"✅ Project '{proj_name}' added!")
    
    with sgs_tabs[2]:
        st.markdown("### ⚠️ Risk Assessment Matrix")
        
        # Risk matrix visualization
        risk_categories = ['Financial', 'Operational', 'Strategic', 'Compliance', 'Technical']
        likelihood = [4, 3, 5, 2, 3]
        impact = [5, 4, 4, 3, 4]
        
        fig_risk = go.Figure()
        fig_risk.add_trace(go.Scatter(
            x=likelihood, y=impact,
            mode='markers+text',
            marker=dict(size=30, color=['#ef4444', '#f59e0b', '#ef4444', '#22c55e', '#f59e0b']),
            text=risk_categories,
            textposition='top center',
            textfont=dict(color='white')
        ))
        
        # Add quadrants
        fig_risk.add_shape(type="rect", x0=0, y0=0, x1=2.5, y1=2.5, fillcolor="rgba(34, 197, 94, 0.2)", line=dict(width=0))
        fig_risk.add_shape(type="rect", x0=2.5, y0=0, x1=5, y1=2.5, fillcolor="rgba(245, 158, 11, 0.2)", line=dict(width=0))
        fig_risk.add_shape(type="rect", x0=0, y0=2.5, x1=2.5, y1=5, fillcolor="rgba(245, 158, 11, 0.2)", line=dict(width=0))
        fig_risk.add_shape(type="rect", x0=2.5, y0=2.5, x1=5, y1=5, fillcolor="rgba(239, 68, 68, 0.2)", line=dict(width=0))
        
        fig_risk.update_layout(
            title="Risk Heat Map",
            xaxis_title="Likelihood",
            yaxis_title="Impact",
            xaxis=dict(range=[0, 5], tickvals=[1, 2, 3, 4, 5]),
            yaxis=dict(range=[0, 5], tickvals=[1, 2, 3, 4, 5]),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#94a3b8',
            height=500
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    with sgs_tabs[3]:
        st.markdown("### 📈 Strategic Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_funding = px.bar(sgs_df, x='project_name', y='funding', color='risk',
                                title='Funding by Project',
                                color_discrete_map={'High': '#ef4444', 'Medium': '#f59e0b', 'Low': '#22c55e'})
            fig_funding.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig_funding, use_container_width=True)
        
        with col2:
            fig_progress = px.bar(sgs_df, x='project_name', y='progress', title='Project Progress',
                                 color='progress', color_continuous_scale='Blues')
            fig_progress.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#94a3b8')
            st.plotly_chart(fig_progress, use_container_width=True)

# ============================================================
# TAB 6: BMS (Governance)
# ============================================================
with tabs[5]:
    st.markdown('<p class="section-header">⚖️ BMS - Board Management System</p>', unsafe_allow_html=True)
    
    bms_tabs = st.tabs(["📊 Overview", "➕ New Meeting", "📋 Meeting Records", "📄 Documents"])
    
    with bms_tabs[0]:
        bms_df = st.session_state.transactions['bms']
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Meetings", len(bms_df))
        with col2:
            approved = len(bms_df[bms_df['status'] == 'Approved'])
            st.metric("Approved Items", approved)
        with col3:
            pending = len(bms_df[bms_df['status'] == 'Pending'])
            st.metric("Pending Items", pending)
        
        st.markdown("---")
        st.dataframe(bms_df, use_container_width=True)
    
    with bms_tabs[1]:
        st.markdown("### ➕ Schedule New Meeting")
        
        with st.form("new_meeting_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                meet_topic = st.text_input("Meeting Topic *")
                meet_date = st.date_input("Meeting Date", datetime.now())
            
            with col2:
                meet_time = st.time_input("Time", datetime.now().time())
                meet_participants = st.number_input("Expected Participants", min_value=1, value=5)
            
            meet_agenda = st.text_area("Agenda")
            
            if st.form_submit_button("📅 Schedule Meeting", type="primary"):
                if meet_topic:
                    new_meet = pd.DataFrame({
                        'id': [generate_id('BMS')],
                        'timestamp': [datetime.now()],
                        'topic': [meet_topic],
                        'meeting_date': [meet_date],
                        'status': ['Scheduled'],
                        'participants': [meet_participants],
                        'resolution': ['Pending']
                    })
                    st.session_state.transactions['bms'] = pd.concat([
                        st.session_state.transactions['bms'], new_meet
                    ], ignore_index=True)
                    st.success(f"✅ Meeting '{meet_topic}' scheduled!")
    
    with bms_tabs[2]:
        st.markdown("### 📋 Meeting Records")
        
        edited_bms = st.data_editor(
            st.session_state.transactions['bms'],
            use_container_width=True,
            num_rows="dynamic"
        )
        st.session_state.transactions['bms'] = edited_bms
    
    with bms_tabs[3]:
        st.markdown("### 📄 Document Repository")
        
        # Simulated document list
        docs = [
            {'name': 'Board Charter 2024.pdf', 'type': 'Policy', 'uploaded': '2024-01-15'},
            {'name': 'Risk Committee TOR.docx', 'type': 'Terms of Reference', 'uploaded': '2024-01-10'},
            {'name': 'Annual Report 2023.pdf', 'type': 'Report', 'uploaded': '2024-02-01'},
        ]
        
        for doc in docs:
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            with col1:
                st.write(f"📄 {doc['name']}")
            with col2:
                st.write(doc['type'])
            with col3:
                st.write(doc['uploaded'])
            with col4:
                st.button("⬇️", key=f"dl_{doc['name']}")
        
        st.markdown("---")
        st.file_uploader("Upload New Document", type=['pdf', 'docx', 'xlsx'])

# ============================================================
# TAB 7: IT (Operations)
# ============================================================
with tabs[6]:
    st.markdown('<p class="section-header">🖥️ IT - Infrastructure Operations</p>', unsafe_allow_html=True)
    
    it_tabs = st.tabs(["📊 Dashboard", "🖥️ Servers", "📈 Metrics", "🔔 Alerts"])
    
    with it_tabs[0]:
        it_df = st.session_state.transactions['it']
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Systems", len(it_df))
        with col2:
            avg_uptime = it_df['uptime'].mean()
            st.metric("Avg Uptime", f"{avg_uptime:.2f}%")
        with col3:
            avg_cpu = it_df['cpu_usage'].mean()
            st.metric("Avg CPU", f"{avg_cpu:.1f}%")
        with col4:
            avg_mem = it_df['memory_usage'].mean()
            st.metric("Avg Memory", f"{avg_mem:.1f}%")
        
        st.markdown("---")
        
        # Server status cards
        for _, server in it_df.iterrows():
            status_color = '#22c55e' if server['status'] == 'Normal' else '#ef4444'
            st.markdown(f"""
            <div style="background: #1e293b; padding: 1rem; border-radius: 10px; margin-bottom: 0.5rem; border-left: 4px solid {status_color};">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong style="color: white;">{server['server']}</strong>
                        <br><small style="color: #64748b;">Status: {server['status']} | Uptime: {server['uptime']}%</small>
                    </div>
                    <div style="text-align: right;">
                        <span style="color: #3b82f6;">CPU: {server['cpu_usage']}%</span> |
                        <span style="color: #8b5cf6;">MEM: {server['memory_usage']}%</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with it_tabs[1]:
        st.markdown("### 🖥️ Server Management")
        st.data_editor(it_df, use_container_width=True)
    
    with it_tabs[2]:
        st.markdown("### 📈 Performance Metrics")
        
        # Simulated time series data
        times = pd.date_range(start=datetime.now() - timedelta(hours=24), periods=24, freq='H')
        cpu_data = [random.randint(30, 70) for _ in range(24)]
        mem_data = [random.randint(50, 80) for _ in range(24)]
        
        fig_metrics = go.Figure()
        fig_metrics.add_trace(go.Scatter(x=times, y=cpu_data, name='CPU %', line=dict(color='#3b82f6')))
        fig_metrics.add_trace(go.Scatter(x=times, y=mem_data, name='Memory %', line=dict(color='#8b5cf6')))
        fig_metrics.update_layout(
            title='System Metrics (24h)',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#94a3b8',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            height=400
        )
        st.plotly_chart(fig_metrics, use_container_width=True)
    
    with it_tabs[3]:
        st.markdown("### 🔔 Alert Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Active Alerts")
            alerts = [
                {'type': 'Warning', 'message': 'Database B CPU > 60%', 'time': '10 min ago'},
                {'type': 'Info', 'message': 'Scheduled maintenance in 2 hours', 'time': '1 hour ago'},
            ]
            for alert in alerts:
                color = '#f59e0b' if alert['type'] == 'Warning' else '#3b82f6'
                st.markdown(f"""
                <div style="background: #1e293b; padding: 0.75rem; border-radius: 8px; margin-bottom: 0.5rem; border-left: 3px solid {color};">
                    <strong style="color: {color};">{alert['type']}</strong>: {alert['message']}
                    <br><small style="color: #64748b;">{alert['time']}</small>
                </div>
                """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("#### Alert Thresholds")
            cpu_threshold = st.slider("CPU Alert Threshold", 0, 100, 80)
            mem_threshold = st.slider("Memory Alert Threshold", 0, 100, 85)
            uptime_threshold = st.slider("Uptime Alert Threshold", 90.0, 100.0, 99.0)
            
            if st.button("💾 Save Thresholds"):
                st.success("Thresholds updated!")

# ============================================================
# TAB 8: REPORT GENERATOR
# ============================================================
with tabs[7]:
    st.markdown('<p class="section-header">📝 Report Generator</p>', unsafe_allow_html=True)
    
    report_tabs = st.tabs(["📊 Quick Report", "📋 Custom Report", "📁 Templates", "📜 Report History"])
    
    with report_tabs[0]:
        st.markdown("### 📊 Quick Report Generator")
        
        col1, col2 = st.columns(2)
        
        with col1:
            report_module = st.selectbox("Select Module", ['MDS - Marketing', 'SGS - Strategy', 'BMS - Governance', 'IT - Operations', 'All Modules'])
            date_range = st.date_input("Date Range", [datetime.now() - timedelta(days=30), datetime.now()])
        
        with col2:
            report_format = st.selectbox("Output Format", ['Excel', 'PDF', 'Word', 'JSON'])
            include_charts = st.checkbox("Include Charts", value=True)
        
        if st.button("🚀 Generate Report", type="primary"):
            with st.spinner("Generating report..."):
                # Determine data based on module
                if 'MDS' in report_module:
                    report_data = st.session_state.transactions['mds']
                elif 'SGS' in report_module:
                    report_data = st.session_state.transactions['sgs']
                elif 'BMS' in report_module:
                    report_data = st.session_state.transactions['bms']
                elif 'IT' in report_module:
                    report_data = st.session_state.transactions['it']
                else:
                    # Combine all
                    report_data = pd.concat([
                        st.session_state.transactions['mds'].assign(module='MDS'),
                        st.session_state.transactions['sgs'].assign(module='SGS'),
                        st.session_state.transactions['bms'].assign(module='BMS'),
                        st.session_state.transactions['it'].assign(module='IT')
                    ], ignore_index=True)
                
                st.success("✅ Report generated successfully!")
                
                # Preview
                st.markdown("#### Report Preview")
                st.dataframe(report_data, use_container_width=True)
                
                # Download links
                st.markdown("#### Download Report")
                col_dl1, col_dl2, col_dl3, col_dl4 = st.columns(4)
                
                with col_dl1:
                    st.markdown(create_download_link(report_data, 'report', 'excel'), unsafe_allow_html=True)
                with col_dl2:
                    st.markdown(create_download_link(report_data, 'report', 'csv'), unsafe_allow_html=True)
                with col_dl3:
                    st.markdown(create_download_link(report_data, 'report', 'json'), unsafe_allow_html=True)
                with col_dl4:
                    st.info("PDF/Word requires additional libraries")
    
    with report_tabs[1]:
        st.markdown("### 📋 Custom Report Builder")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Data source selection
            data_sources = st.multiselect(
                "Select Data Sources",
                ['MDS Leads', 'SGS Projects', 'BMS Meetings', 'IT Systems', 'Customer Master', 'Product Master']
            )
            
            # Field selection (dynamic based on sources)
            if data_sources:
                available_fields = []
                if 'MDS Leads' in data_sources:
                    available_fields.extend(['lead_name', 'source', 'value', 'status'])
                if 'SGS Projects' in data_sources:
                    available_fields.extend(['project_name', 'funding', 'risk', 'progress'])
                if 'BMS Meetings' in data_sources:
                    available_fields.extend(['topic', 'meeting_date', 'status', 'participants'])
                
                selected_fields = st.multiselect("Select Fields", available_fields)
            
            # Filters
            st.markdown("#### Filters")
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                date_filter = st.date_input("Date Filter", [datetime.now() - timedelta(days=30), datetime.now()], key='custom_date')
            with filter_col2:
                status_filter = st.multiselect("Status Filter", ['All', 'Active', 'Pending', 'Completed'])
            
            # Grouping
            st.markdown("#### Aggregation")
            group_by = st.selectbox("Group By", ['None', 'Status', 'Source', 'Department', 'Date'])
            
            if st.button("🔨 Build Report", type="primary"):
                st.success("Custom report built! (Demo mode)")
                st.info("In production, this would generate a report based on selected options.")
        
        with col2:
            st.markdown("#### Report Options")
            st.checkbox("Include Summary Statistics", value=True)
            st.checkbox("Include Trend Analysis", value=True)
            st.checkbox("Include Recommendations", value=False)
            st.checkbox("Add Cover Page", value=True)
            st.checkbox("Add Table of Contents", value=True)
            
            st.markdown("#### Output Settings")
            output_format = st.radio("Format", ['Excel', 'PDF', 'Word', 'JSON', 'HTML'])
            
            if output_format == 'PDF':
                st.selectbox("Page Size", ['A4', 'Letter', 'Legal'])
                st.selectbox("Orientation", ['Portrait', 'Landscape'])
    
    with report_tabs[2]:
        st.markdown("### 📁 Report Templates")
        
        templates = st.session_state.report_templates
        
        for template in templates:
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            with col1:
                st.write(f"📄 {template['name']}")
            with col2:
                st.write(template['type'])
            with col3:
                st.write(template['format'])
            with col4:
                if st.button("Use", key=f"use_{template['id']}"):
                    st.info(f"Template '{template['name']}' selected")
        
        st.markdown("---")
        st.markdown("#### Create New Template")
        
        with st.form("new_template"):
            tpl_name = st.text_input("Template Name")
            tpl_type = st.selectbox("Module Type", ['MDS', 'SGS', 'BMS', 'IT', 'All'])
            tpl_format = st.selectbox("Default Format", ['Excel', 'PDF', 'Word', 'JSON'])
            
            if st.form_submit_button("💾 Save Template"):
                st.session_state.report_templates.append({
                    'id': generate_id('RPT'),
                    'name': tpl_name,
                    'type': tpl_type,
                    'format': tpl_format
                })
                st.success("Template saved!")
    
    with report_tabs[3]:
        st.markdown("### 📜 Report History")
        
        # Simulated history
        history = [
            {'name': 'Daily Sales Report', 'generated': '2024-01-20 09:00', 'format': 'Excel', 'status': 'Completed'},
            {'name': 'Risk Assessment Q1', 'generated': '2024-01-19 14:30', 'format': 'PDF', 'status': 'Completed'},
            {'name': 'Board Summary', 'generated': '2024-01-18 16:00', 'format': 'Word', 'status': 'Completed'},
        ]
        
        st.dataframe(pd.DataFrame(history), use_container_width=True)

# ============================================================
# TAB 9: SETTINGS
# ============================================================
with tabs[8]:
    st.markdown('<p class="section-header">⚙️ System Settings</p>', unsafe_allow_html=True)
    
    settings_tabs = st.tabs(["🎨 Appearance", "🔐 Security", "🔗 Integrations", "📊 Data Management", "ℹ️ About"])
    
    with settings_tabs[0]:
        st.markdown("### 🎨 Appearance Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.selectbox("Theme", ['Dark (Default)', 'Light', 'System'])
            st.selectbox("Language", ['English', 'ไทย'])
            st.selectbox("Date Format", ['YYYY-MM-DD', 'DD/MM/YYYY', 'MM/DD/YYYY'])
        
        with col2:
            st.selectbox("Number Format", ['1,234.56', '1.234,56', '1 234.56'])
            st.selectbox("Currency", ['THB (฿)', 'USD ($)', 'EUR (€)'])
            st.slider("Dashboard Refresh Rate (seconds)", 10, 300, 60)
    
    with settings_tabs[1]:
        st.markdown("### 🔐 Security Settings")
        
        st.checkbox("Enable Two-Factor Authentication", value=False)
        st.checkbox("Session Timeout (30 minutes)", value=True)
        st.checkbox("Audit Logging", value=True)
        
        st.markdown("#### Password Policy")
        st.slider("Minimum Password Length", 8, 20, 12)
        st.checkbox("Require Special Characters", value=True)
        st.checkbox("Require Numbers", value=True)
    
    with settings_tabs[2]:
        st.markdown("### 🔗 API & Integrations")
        
        st.text_input("API Endpoint", "https://api.smd-dataverse.com/v1")
        st.text_input("API Key", type="password", value="sk-xxxxx")
        
        st.markdown("#### Connected Services")
        services = [
            {'name': 'Microsoft 365', 'status': 'Connected'},
            {'name': 'Salesforce', 'status': 'Not Connected'},
            {'name': 'SAP', 'status': 'Connected'},
        ]
        
        for svc in services:
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.write(svc['name'])
            with col2:
                status_color = 'green' if svc['status'] == 'Connected' else 'red'
                st.markdown(f":{status_color}[{svc['status']}]")
            with col3:
                st.button("Configure", key=f"cfg_{svc['name']}")
    
    with settings_tabs[3]:
        st.markdown("### 📊 Data Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Export All Data")
            if st.button("📥 Export Complete Backup"):
                # Combine all data
                all_data = {
                    'master_data': {k: v.to_dict() for k, v in st.session_state.master_data.items()},
                    'transactions': {k: v.to_dict() for k, v in st.session_state.transactions.items()}
                }
                json_str = json.dumps(all_data, default=str, indent=2)
                b64 = base64.b64encode(json_str.encode()).decode()
                href = f'<a href="data:file/json;base64,{b64}" download="smd_dataverse_backup.json">📥 Download Backup File</a>'
                st.markdown(href, unsafe_allow_html=True)
        
        with col2:
            st.markdown("#### Clear Data")
            st.warning("⚠️ This action cannot be undone!")
            
            clear_option = st.selectbox("Select data to clear", [
                '-- Select --',
                'MDS Transactions',
                'SGS Transactions', 
                'BMS Transactions',
                'IT Transactions',
                'All Transactions',
                'All Data (Factory Reset)'
            ])
            
            if st.button("🗑️ Clear Selected Data", type="secondary"):
                if clear_option != '-- Select --':
                    st.error(f"Would clear: {clear_option} (Disabled in demo)")
    
    with settings_tabs[4]:
        st.markdown("### ℹ️ About SMD Dataverse")
        
        st.markdown("""
        <div style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%); padding: 2rem; border-radius: 12px; text-align: center;">
            <h1 style="font-size: 3rem; margin: 0;">🧊</h1>
            <h2 style="background: linear-gradient(90deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">SMD Dataverse Platform</h2>
            <p style="color: #94a3b8;">Enterprise Data Management Solution</p>
            <p style="color: #64748b;">Version 5.0.0</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        ---
        
        **Features:**
        - 📥 Multi-format data import (Excel, CSV, PDF, Word)
        - 📊 Master data management
        - 💼 MDS - Marketing Data System
        - 🧭 SGS - Strategic Guidance System
        - ⚖️ BMS - Board Management System
        - 🖥️ IT - Operations Management
        - 📝 Custom report generation (Excel, PDF, Word, JSON)
        - 🔗 API integrations
        
        **Technology Stack:**
        - Python 3.11+
        - Streamlit
        - Pandas
        - Plotly
        - OpenPyXL
        
        ---
        
        © 2024 SMD Dataverse. All rights reserved.
        """)

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #64748b; font-size: 0.8rem;">
    SMD Dataverse Platform v5.0 | © 2024 | 
    <a href="#" style="color: #3b82f6;">Documentation</a> | 
    <a href="#" style="color: #3b82f6;">Support</a>
</div>
""", unsafe_allow_html=True)
