import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from analytics import (
    DATA_PATH,
    PANNE_DESCRIPTIONS,
    RECOMMANDATIONS,
    get_gold_parquet_path,
    load_aggregation,
    load_data,
    load_quality_report
)

# ============================================================================
# CONFIGURATION DE LA PAGE
# ============================================================================

st.set_page_config(
    page_title="AutoStream Command Center",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration Plotly Dark Theme
import plotly.io as pio
pio.templates.default = "plotly_dark"

# ============================================================================
# SYSTÈME DE THÈMES
# ============================================================================

if 'theme' not in st.session_state:
    st.session_state.theme = 'Dark Tech'

THEMES = {
    'Dark Tech': {
        'primary': '#00d4ff',
        'secondary': '#00ffaa',
        'accent': '#ff006e',
        'bg': '#0E1117',
        'card': '#1A1C24',
        'text': '#ffffff'
    },
    'Cyber Purple': {
        'primary': '#a855f7',
        'secondary': '#ec4899',
        'accent': '#fbbf24',
        'bg': '#0E1117',
        'card': '#1A1C24',
        'text': '#ffffff'
    }
}

current_theme = THEMES[st.session_state.theme]

# ============================================================================
# DARK MODE CSS - COMMAND CENTER STYLE
# ============================================================================

st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    
    /* ========== GLOBAL DARK MODE ========== */
    * {{
        font-family: 'Inter', sans-serif;
    }}
    
    .stApp {{
        background: {current_theme['bg']};
        color: {current_theme['text']};
    }}
    
    /* Remove default padding */
    .block-container {{
        padding-top: 2rem;
        padding-bottom: 0rem;
        padding-left: 3rem;
        padding-right: 3rem;
    }}
    
    /* ========== DARK SIDEBAR ========== */
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #0a0a0f 0%, #14141a 100%);
        border-right: 1px solid {current_theme['primary']}44;
    }}
    
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] p {{
        color: {current_theme['text']} !important;
    }}
    
    /* ========== HEADER STYLE ========== */
    .command-header {{
        background: linear-gradient(135deg, {current_theme['card']} 0%, #0a0a0f 100%);
        border: 1px solid {current_theme['primary']}66;
        border-radius: 12px;
        padding: 1.5rem 2rem;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0, 212, 255, 0.15);
    }}
    
    .command-header h1 {{
        color: {current_theme['primary']};
        font-size: 2rem;
        font-weight: 800;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 2px;
    }}
    
    .command-header p {{
        color: #8b949e;
        margin: 0.5rem 0 0 0;
        font-size: 0.9rem;
        font-weight: 400;
    }}
    
    /* ========== CUSTOM KPI CARDS ========== */
    .kpi-card {{
        background: {current_theme['card']};
        border: 1px solid {current_theme['primary']}44;
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }}
    
    .kpi-card:hover {{
        border-color: {current_theme['primary']};
        box-shadow: 0 8px 24px {current_theme['primary']}33;
        transform: translateY(-2px);
    }}
    
    .kpi-label {{
        color: #8b949e;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 0.5rem;
    }}
    
    .kpi-value {{
        color: {current_theme['primary']};
        font-size: 2.5rem;
        font-weight: 800;
        line-height: 1;
        margin: 0.5rem 0;
    }}
    
    .kpi-delta {{
        color: #8b949e;
        font-size: 0.85rem;
        font-weight: 500;
    }}
    
    /* ========== TABS STYLING ========== */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 2rem;
        background: transparent;
        border-bottom: 2px solid {current_theme['primary']}22;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        border: none;
        color: #8b949e;
        font-weight: 600;
        font-size: 1rem;
        padding: 1rem 0;
        border-bottom: 2px solid transparent;
    }}
    
    .stTabs [aria-selected="true"] {{
        color: {current_theme['primary']};
        border-bottom: 2px solid {current_theme['primary']};
    }}
    
    /* ========== ALERT CARD ========== */
    .alert-card {{
        background: {current_theme['card']};
        border-left: 4px solid {current_theme['accent']};
        border-radius: 8px;
        padding: 1rem 1.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }}
    
    .alert-critical {{
        border-left-color: #ff006e;
    }}
    
    .alert-warning {{
        border-left-color: #fbbf24;
    }}
    
    /* ========== DATAFRAME DARK ========== */
    [data-testid="stDataFrame"] {{
        background: {current_theme['card']};
        border: 1px solid {current_theme['primary']}33;
        border-radius: 12px;
    }}
    
    /* ========== BUTTONS DARK ========== */
    .stButton button {{
        background: linear-gradient(135deg, {current_theme['primary']} 0%, {current_theme['secondary']} 100%);
        color: #000;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.5rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        transition: all 0.3s ease;
    }}
    
    .stButton button:hover {{
        box-shadow: 0 4px 16px {current_theme['primary']}66;
        transform: translateY(-2px);
    }}
    
    /* ========== SELECTBOX / MULTISELECT DARK ========== */
    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        background: {current_theme['card']};
        border: 1px solid {current_theme['primary']}44;
        color: {current_theme['text']};
    }}
    
    /* ========== SECTION DIVIDER ========== */
    .section-divider {{
        height: 1px;
        background: linear-gradient(90deg, transparent, {current_theme['primary']}44, transparent);
        margin: 2rem 0;
    }}
    
    /* ========== INFO STATS ========== */
    .info-stat {{
        background: {current_theme['card']};
        border: 1px solid {current_theme['primary']}33;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }}
    
    .info-stat-value {{
        color: {current_theme['secondary']};
        font-size: 1.8rem;
        font-weight: 700;
    }}
    
    .info-stat-label {{
        color: #8b949e;
        font-size: 0.8rem;
        text-transform: uppercase;
    }}
    
    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# VÉRIFICATION DU FICHIER DE DONNÉES
# ============================================================================

gold_parquet_path = get_gold_parquet_path()
if not gold_parquet_path:
    st.error("❌ Fichier de données Parquet introuvable dans data/gold/parquet")
    st.info("💡 **Comment générer les données ?**")
    st.code("python creation_data/pipeline_spark.py\npython creation_data/pipeline_gold.py", language="bash")
    st.stop()

# Chargement des données
df = load_data(gold_parquet_path)

# ============================================================================
# SIDEBAR - COMMAND PANEL (ALL CONTROLS)
# ============================================================================

with st.sidebar:
    st.markdown(f"""
        <div style="text-align: center; padding: 1.5rem 0; border-bottom: 1px solid {current_theme['primary']}44;">
            <h2 style="color: {current_theme['primary']}; font-size: 1.5rem; font-weight: 800; margin: 0;">⚙️ COMMAND PANEL</h2>
            <p style="color: #8b949e; font-size: 0.75rem; margin: 0.5rem 0 0 0;">CONTROL CENTER</p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Theme Selector
    st.markdown("### 🎨 Theme")
    if st.button('🌊 Dark Tech' if st.session_state.theme == 'Cyber Purple' else '💜 Cyber Purple', use_container_width=True):
        st.session_state.theme = 'Cyber Purple' if st.session_state.theme == 'Dark Tech' else 'Dark Tech'
        st.rerun()
    
    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    
    # Extraction des valeurs uniques
    vin_values = sorted(df["vin"].dropna().unique()) if "vin" in df.columns else []
    modele_values = sorted(df["modele"].dropna().unique()) if "modele" in df.columns else []
    statut_values = sorted(df["statut"].dropna().unique()) if "statut" in df.columns else []
    panne_values = sorted(df["panne_type_simple"].dropna().unique()) if "panne_type_simple" in df.columns else []
    
    # Vehicle Filters
    st.markdown("### 🚗 Vehicle Filters")
    selected_vins = st.multiselect(
        "VIN Selection", 
        vin_values, 
        default=vin_values,
        help="Select specific vehicles"
    )
    
    selected_modeles = st.multiselect(
        "Model Selection", 
        modele_values, 
        default=modele_values,
        help="Filter by vehicle model"
    )
    
    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    
    # Status Filters
    st.markdown("### ⚠️ Status Filters")
    selected_statuts = st.multiselect(
        "Operational Status", 
        statut_values, 
        default=statut_values,
        help="OK, ALERT, or CRITICAL status"
    )
    
    if panne_values:
        selected_pannes = st.multiselect(
            "Fault Types", 
            panne_values, 
            default=panne_values,
            help="Predicted fault types by AI"
        )
    else:
        selected_pannes = None
    
    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    
    # Risk Threshold
    st.markdown("### 🎯 Risk Threshold")
    prob_min = st.slider(
        "Minimum Failure Probability", 
        0.0, 1.0, 0.0, 0.05,
        help="Show only vehicles above this threshold"
    )
    st.caption(f"🔍 Display: ≥ {prob_min:.0%} risk")
    
    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    
    # Quick Views
    st.markdown("### 👁️ Quick Views")
    vue_rapide = st.radio(
        "Display Mode",
        ["📊 All Vehicles", "🔴 Urgent Only (≥70%)", "🟡 Watch List (≥30%)"],
        index=0,
        help="Select predefined view"
    )

# ============================================================================
# APPLICATION DES FILTRES
# ============================================================================

filtered = df.copy()

if selected_vins:
    filtered = filtered[filtered["vin"].isin(selected_vins)]
if selected_modeles:
    filtered = filtered[filtered["modele"].isin(selected_modeles)]
if selected_statuts:
    filtered = filtered[filtered["statut"].isin(selected_statuts)]
if selected_pannes and "panne_type_simple" in filtered.columns:
    filtered = filtered[filtered["panne_type_simple"].isin(selected_pannes)]
if "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= prob_min]

# Application de la vue rapide
if "🔴 Urgent" in vue_rapide and "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= 0.7]
elif "🟡 Watch" in vue_rapide and "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= 0.3]

# ============================================================================
# MAIN AREA - HEADER
# ============================================================================

st.markdown(f"""
    <div class="command-header">
        <h1>🚗 AUTOSTREAM COMMAND CENTER</h1>
        <p>Real-time Predictive Maintenance Intelligence System • {len(filtered)} / {len(df)} vehicles displayed</p>
    </div>
""", unsafe_allow_html=True)

# ============================================================================
# TABS ORGANIZATION
# ============================================================================

tab1, tab2, tab3 = st.tabs(["🚀 COCKPIT", "📊 ANALYTICS", "📋 FLEET DATA"])

# ============================================================================
# TAB 1: COCKPIT - CRITICAL KPIs & ALERTS ONLY
# ============================================================================

with tab1:
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Executive Summary KPIs in 4 columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        nb_vehicules = int(filtered["vin"].nunique()) if "vin" in filtered.columns else 0
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">🚗 Total Vehicles</div>
                <div class="kpi-value">{nb_vehicules}</div>
                <div class="kpi-delta">Active in fleet</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        critical_count = int((filtered["statut"] == "CRITIQUE").sum()) if "statut" in filtered.columns else 0
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">🔴 Critical</div>
                <div class="kpi-value" style="color: #ff006e;">{critical_count}</div>
                <div class="kpi-delta">Immediate action required</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        if "type_panne_predit" in filtered.columns:
            interventions_count = int((filtered["type_panne_predit"] != 0).sum())
            st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">🚨 Interventions</div>
                    <div class="kpi-value" style="color: #fbbf24;">{interventions_count}</div>
                    <div class="kpi-delta">Maintenance needed</div>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">🚨 Interventions</div>
                    <div class="kpi-value">N/A</div>
                    <div class="kpi-delta">Data unavailable</div>
                </div>
            """, unsafe_allow_html=True)
    
    with col4:
        nb_ok = len(filtered[filtered["type_panne_predit"] == 0]) if "type_panne_predit" in filtered.columns else 0
        nb_total = len(filtered)
        health_percentage = (nb_ok / nb_total * 100) if nb_total > 0 else 0
        health_color = '#00ffaa' if health_percentage >= 70 else ('#fbbf24' if health_percentage >= 50 else '#ff006e')
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">💚 Fleet Health</div>
                <div class="kpi-value" style="color: {health_color};">{health_percentage:.0f}%</div>
                <div class="kpi-delta">Operational status</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # Critical Alerts List
    st.markdown(f"""
        <h3 style="color: {current_theme['primary']}; font-weight: 700; margin-bottom: 1rem;">🚨 PRIORITY ALERTS</h3>
    """, unsafe_allow_html=True)
    
    if "prob_panne" in filtered.columns and "panne_type_simple" in filtered.columns:
        urgent = filtered[filtered["type_panne_predit"] != 0].copy()
        
        if not urgent.empty:
            urgent_sorted = urgent.sort_values(["statut", "prob_panne"], ascending=[True, False]).head(10)
            
            for idx, row in urgent_sorted.iterrows():
                status_color = '#ff006e' if row['statut'] == 'CRITIQUE' else ('#fbbf24' if row['statut'] == 'ALERTE' else '#00ffaa')
                st.markdown(f"""
                    <div class="alert-card alert-critical">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong style="color: {current_theme['text']}; font-size: 1.1rem;">{row.get('vin', 'N/A')}</strong>
                                <span style="color: #8b949e; margin-left: 1rem;">• {row.get('modele', 'N/A')}</span>
                            </div>
                            <div style="text-align: right;">
                                <span style="color: {status_color}; font-weight: 700; font-size: 1.2rem;">{row.get('prob_panne', 0):.0%}</span>
                                <span style="color: #8b949e; margin-left: 0.5rem;">• {row.get('panne_type_simple', 'Unknown')}</span>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="background: {current_theme['card']}; border: 1px solid #00ffaa44; border-radius: 12px; padding: 2rem; text-align: center;">
                    <h3 style="color: #00ffaa; margin: 0;">✅ ALL SYSTEMS OPERATIONAL</h3>
                    <p style="color: #8b949e; margin: 0.5rem 0 0 0;">No critical alerts detected</p>
                </div>
            """, unsafe_allow_html=True)

# ============================================================================
# TAB 2: ANALYTICS - ALL CHARTS & VISUALIZATIONS
# ============================================================================

with tab2:
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 1: 3 main charts
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if "modele" in filtered.columns and "score_risque" in filtered.columns:
            st.markdown(f"<h4 style='color: {current_theme['primary']}; font-weight: 600;'>Risk Score by Model</h4>", unsafe_allow_html=True)
            by_modele = filtered.groupby("modele", dropna=True)["score_risque"].mean().sort_values(ascending=False)
            
            fig = px.bar(
                x=by_modele.values,
                y=by_modele.index,
                orientation='h',
                labels={'x': 'Risk Score', 'y': 'Model'},
                color=by_modele.values,
                color_continuous_scale=['#00ffaa', '#fbbf24', '#ff006e']
            )
            fig.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Inter', size=11, color='#8b949e'),
                xaxis=dict(showgrid=True, gridcolor='rgba(139,148,158,0.1)'),
                yaxis=dict(showgrid=False)
            )
            fig.update_traces(marker=dict(line=dict(width=0)))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with col2:
        if "statut" in filtered.columns:
            st.markdown(f"<h4 style='color: {current_theme['primary']}; font-weight: 600;'>Operational Status</h4>", unsafe_allow_html=True)
            statut_counts = filtered["statut"].value_counts().sort_index()
            
            colors_map = {'OK': '#00ffaa', 'ALERTE': '#fbbf24', 'CRITIQUE': '#ff006e', 'SURVEILLANCE': '#00d4ff'}
            fig = go.Figure(data=[go.Pie(
                labels=statut_counts.index,
                values=statut_counts.values,
                marker=dict(colors=[colors_map.get(s, '#999') for s in statut_counts.index]),
                hole=0.6,
                textinfo='label+percent',
                textfont=dict(size=12, family='Inter', color='#ffffff')
            )])
            fig.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Inter', size=11, color='#8b949e')
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with col3:
        if "panne_type_simple" in filtered.columns:
            st.markdown(f"<h4 style='color: {current_theme['primary']}; font-weight: 600;'>Predicted Fault Types</h4>", unsafe_allow_html=True)
            panne_counts = filtered["panne_type_simple"].value_counts().sort_index()
            
            fig = px.bar(
                x=panne_counts.index,
                y=panne_counts.values,
                labels={'x': 'Fault Type', 'y': 'Count'},
                color=panne_counts.values,
                color_continuous_scale=['#00d4ff', '#ff006e']
            )
            fig.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Inter', size=11, color='#8b949e'),
                xaxis=dict(showgrid=False, tickangle=-45),
                yaxis=dict(showgrid=True, gridcolor='rgba(139,148,158,0.1)')
            )
            fig.update_traces(marker=dict(line=dict(width=0)))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 2: Distribution and Correlation
    col1, col2 = st.columns(2)
    
    with col1:
        if "prob_panne" in filtered.columns:
            st.markdown(f"<h4 style='color: {current_theme['primary']}; font-weight: 600;'>Risk Level Distribution</h4>", unsafe_allow_html=True)
            
            prob_bins = pd.cut(
                filtered["prob_panne"], 
                bins=[0, 0.3, 0.5, 0.7, 1.0],
                labels=["🟢 Low", "🟡 Medium", "🟠 High", "🔴 Critical"]
            )
            prob_dist = prob_bins.value_counts().sort_index()
            
            colors_map = {'🟢 Low': '#00ffaa', '🟡 Medium': '#fbbf24', '🟠 High': '#ff9800', '🔴 Critical': '#ff006e'}
            fig = px.bar(
                x=prob_dist.index,
                y=prob_dist.values,
                labels={'x': 'Risk Level', 'y': 'Vehicle Count'},
                color=prob_dist.index,
                color_discrete_map=colors_map
            )
            fig.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Inter', size=11, color='#8b949e'),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='rgba(139,148,158,0.1)')
            )
            fig.update_traces(marker=dict(line=dict(width=0)))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with col2:
        if all(col in filtered.columns for col in ["temp_moteur", "voltage_batterie", "pression_huile", "statut"]):
            st.markdown(f"<h4 style='color: {current_theme['primary']}; font-weight: 600;'>OBD Parameters Correlation</h4>", unsafe_allow_html=True)
            
            statut_map = {"CRITIQUE": 2, "ALERTE": 1, "SURVEILLANCE": 0.5, "OK": 0}
            agg_by_statut = filtered.groupby("statut").agg({
                "temp_moteur": "mean",
                "voltage_batterie": "mean",
                "pression_huile": "mean"
            }).reset_index()
            agg_by_statut["severity"] = agg_by_statut["statut"].map(statut_map)
            agg_by_statut = agg_by_statut.sort_values("severity", ascending=False)
            
            fig = go.Figure()
            fig.add_trace(go.Bar(name='Temp (°C)', x=agg_by_statut['statut'], y=agg_by_statut['temp_moteur'], marker_color='#ff006e'))
            fig.add_trace(go.Bar(name='Voltage (V)', x=agg_by_statut['statut'], y=agg_by_statut['voltage_batterie']*10, marker_color='#00d4ff'))
            fig.add_trace(go.Bar(name='Pressure (bar)', x=agg_by_statut['statut'], y=agg_by_statut['pression_huile']*20, marker_color='#00ffaa'))
            
            fig.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                barmode='group',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Inter', size=11, color='#8b949e'),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='rgba(139,148,158,0.1)'),
                legend=dict(orientation='h', yanchor='top', y=-0.15, xanchor='center', x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

# ============================================================================
# TAB 3: FLEET DATA - ALL TABLES
# ============================================================================

with tab3:
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Priority Vehicles Table
    if "prob_panne" in filtered.columns and "panne_type_simple" in filtered.columns:
        urgent = filtered[filtered["type_panne_predit"] != 0].copy()
        
        if not urgent.empty:
            st.markdown(f"<h3 style='color: {current_theme['primary']}; font-weight: 700; margin-bottom: 1rem;'>🚨 PRIORITY MAINTENANCE LIST</h3>", unsafe_allow_html=True)
            
            urgent_sorted = urgent.sort_values(["statut", "prob_panne"], ascending=[True, False])
            
            priority_display = urgent_sorted.copy()
            if "panne_type_simple" in priority_display.columns:
                priority_display["action"] = priority_display["panne_type_simple"].apply(
                    lambda x: RECOMMANDATIONS.get(x, ["Diagnostic required"])[0]
                )
            
            display_cols = {
                "alerte_emoji": "🚦",
                "vin": "VIN",
                "modele": "Model",
                "statut": "Status",
                "panne_type_simple": "Fault Type",
                "prob_panne": "Probability",
                "km_actuel": "Mileage",
                "action": "Required Action"
            }
            
            cols_to_show = [col for col in display_cols.keys() if col in priority_display.columns]
            priority_table = priority_display[cols_to_show].copy()
            priority_table.columns = [display_cols[col] for col in cols_to_show]
            
            if "Probability" in priority_table.columns:
                priority_table["Probability"] = priority_table["Probability"].apply(lambda x: f"{x:.1%}")
            if "Mileage" in priority_table.columns:
                priority_table["Mileage"] = priority_table["Mileage"].apply(lambda x: f"{int(x):,}".replace(",", " "))
            
            st.dataframe(priority_table, use_container_width=True, hide_index=True, height=400)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Complete Fleet Data
    st.markdown(f"<h3 style='color: {current_theme['primary']}; font-weight: 700; margin-bottom: 1rem;'>📋 COMPLETE FLEET DATA</h3>", unsafe_allow_html=True)
    
    display_data = filtered.copy()
    
    core_cols = ["alerte_emoji", "vin", "modele", "statut", "panne_emoji", "panne_type_simple", "prob_panne"]
    if "km_estime" in display_data.columns:
        core_cols.append("km_estime")
    
    obd_cols = ["temp_moteur", "voltage_batterie", "pression_huile", "regime_moteur"]
    obd_available = [col for col in obd_cols if col in display_data.columns]
    
    all_cols = core_cols + obd_available
    existing_cols = [c for c in all_cols if c in display_data.columns]
    
    full_table = display_data[existing_cols].copy()
    
    if "prob_panne" in full_table.columns:
        full_table["prob_panne"] = full_table["prob_panne"].apply(lambda x: f"{x:.1%}")
    if "km_estime" in full_table.columns:
        full_table["km_estime"] = full_table["km_estime"].apply(
            lambda x: "⚠️ Now" if pd.notna(x) and int(x) == 0 else (f"{int(x):,} km".replace(",", " ") if pd.notna(x) else "N/A")
        )
    
    st.dataframe(full_table, use_container_width=True, hide_index=True, height=500)
    
    # Export button
    csv = filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 EXPORT TO CSV",
        data=csv,
        file_name=f"autostream_fleet_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown(f"""
    <div style="text-align: center; padding: 2rem; border-top: 1px solid {current_theme['primary']}22;">
        <p style="color: #8b949e; font-size: 0.85rem; margin: 0;">
            <strong style="color: {current_theme['primary']};">AUTOSTREAM COMMAND CENTER</strong> • 
            Powered by AI & Big Data • 
            Real-time Predictive Maintenance Intelligence
        </p>
    </div>
""", unsafe_allow_html=True)
