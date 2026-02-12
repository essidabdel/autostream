import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# External libraries for enhanced UI
try:
    from streamlit_option_menu import option_menu
except ImportError:
    st.error("Please install: pip install streamlit-option-menu")
    st.stop()

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
    from st_aggrid.shared import JsCode
except ImportError:
    st.error("Please install: pip install streamlit-aggrid")
    st.stop()

try:
    from streamlit_lottie import st_lottie
    import requests
except ImportError:
    st_lottie = None  # Optional

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
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="AutoStream • Sci-Fi Command Center",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# GLASSMORPHISM CSS INJECTION
# ============================================================================

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
    
    /* ========== GLOBAL GLASSMORPHISM THEME ========== */
    * {
        font-family: 'Inter', sans-serif;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
        background-attachment: fixed;
    }
    
    /* ========== GLASS CARD CLASS ========== */
    .glass-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.18);
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
        padding: 1.5rem;
        margin: 1rem 0;
        transition: all 0.3s ease;
    }
    
    .glass-card:hover {
        background: rgba(255, 255, 255, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.3);
        box-shadow: 0 12px 40px 0 rgba(31, 38, 135, 0.5);
        transform: translateY(-2px);
    }
    
    /* ========== SIDEBAR GLASSMORPHISM ========== */
    section[data-testid="stSidebar"] {
        background: rgba(15, 12, 41, 0.85);
        backdrop-filter: blur(20px);
        border-right: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    section[data-testid="stSidebar"] * {
        color: #ffffff !important;
    }
    
    /* ========== HEADER GLOW ========== */
    .main-header {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(15px);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.2);
        padding: 2rem;
        text-align: center;
        box-shadow: 0 8px 32px 0 rgba(138, 43, 226, 0.3);
        margin-bottom: 2rem;
    }
    
    .main-header h1 {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3rem;
        font-weight: 900;
        margin: 0;
        letter-spacing: 3px;
        text-transform: uppercase;
    }
    
    .main-header p {
        color: rgba(255, 255, 255, 0.7);
        font-size: 1.1rem;
        margin: 0.5rem 0 0 0;
        font-weight: 300;
    }
    
    /* ========== KPI GLASS CARDS ========== */
    .kpi-glass {
        background: rgba(255, 255, 255, 0.06);
        backdrop-filter: blur(12px);
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.15);
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 8px 32px 0 rgba(102, 126, 234, 0.2);
        transition: all 0.3s ease;
        min-height: 180px;
        height: 180px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }
    
    .kpi-glass:hover {
        background: rgba(255, 255, 255, 0.1);
        transform: translateY(-5px);
        box-shadow: 0 12px 40px 0 rgba(102, 126, 234, 0.4);
    }
    
    .kpi-label {
        color: rgba(255, 255, 255, 0.6);
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 0.5rem;
    }
    
    .kpi-value {
        font-size: 2.8rem;
        font-weight: 900;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1;
        margin: 0.5rem 0;
    }
    
    .kpi-delta {
        color: rgba(255, 255, 255, 0.5);
        font-size: 0.85rem;
        font-weight: 400;
    }
    
    /* ========== ALERT CARDS ========== */
    .alert-glass {
        background: rgba(255, 0, 110, 0.1);
        backdrop-filter: blur(10px);
        border-left: 4px solid #ff006e;
        border-radius: 12px;
        padding: 1rem 1.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 4px 16px rgba(255, 0, 110, 0.3);
    }
    
    .alert-glass:hover {
        background: rgba(255, 0, 110, 0.15);
    }
    
    /* ========== BUTTONS GLASSMORPHISM ========== */
    .stButton button {
        background: rgba(102, 126, 234, 0.2);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(102, 126, 234, 0.5);
        border-radius: 12px;
        color: #ffffff;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        padding: 0.7rem 2rem;
        transition: all 0.3s ease;
    }
    
    .stButton button:hover {
        background: rgba(102, 126, 234, 0.4);
        border: 1px solid rgba(102, 126, 234, 0.8);
        box-shadow: 0 8px 24px rgba(102, 126, 234, 0.4);
        transform: translateY(-2px);
    }
    
    /* ========== DATAFRAME GLASS ========== */
    [data-testid="stDataFrame"] {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(10px);
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    /* ========== EXPANDER GLASS ========== */
    [data-testid="stExpander"] {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.15);
        border-radius: 12px;
    }
    
    /* ========== SELECTBOX / MULTISELECT GLASS ========== */
    .stSelectbox > div > div,
    .stMultiSelect > div > div {
        background: rgba(255, 255, 255, 0.08) !important;
        backdrop-filter: blur(8px);
        border: 1px solid rgba(255, 255, 255, 0.2) !important;
        border-radius: 8px;
        color: #ffffff !important;
    }
    
    /* ========== SLIDER GLASS ========== */
    .stSlider > div > div {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 8px;
    }
    
    /* ========== REMOVE STREAMLIT BRANDING ========== */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* ========== SECTION DIVIDER ========== */
    .glass-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
        margin: 2rem 0;
    }
    
    /* ========== METRIC OVERRIDE ========== */
    [data-testid="stMetricValue"] {
        font-size: 2.5rem !important;
        font-weight: 900 !important;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    /* ========== AGGRID DARK THEME ========== */
    .ag-theme-streamlit {
        --ag-background-color: rgba(255, 255, 255, 0.03);
        --ag-foreground-color: #ffffff;
        --ag-header-background-color: rgba(102, 126, 234, 0.2);
        --ag-odd-row-background-color: rgba(255, 255, 255, 0.02);
        --ag-row-hover-color: rgba(102, 126, 234, 0.1);
        --ag-border-color: rgba(255, 255, 255, 0.1);
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# LOTTIE ANIMATION HELPER
# ============================================================================

def load_lottie_url(url: str):
    """Load Lottie animation from URL"""
    if st_lottie is None:
        return None
    try:
        r = requests.get(url)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None

# ============================================================================
# DATA LOADING
# ============================================================================

gold_parquet_path = get_gold_parquet_path()
if not gold_parquet_path:
    st.error("❌ Fichier de données Parquet introuvable")
    st.info("💡 Exécutez: python creation_data/pipeline_spark.py")
    st.stop()

df = load_data(gold_parquet_path)

# ============================================================================
# SIDEBAR - NAVIGATION & CONTROL PANEL
# ============================================================================

with st.sidebar:
    # Lottie Animation (Optional)
    if st_lottie:
        lottie_radar = load_lottie_url("https://assets5.lottiefiles.com/packages/lf20_5tl1xxnz.json")
        if lottie_radar:
            st_lottie(lottie_radar, height=150, key="radar")
    
    st.markdown("""
        <div style='text-align: center; padding: 1rem 0;'>
            <h2 style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                       -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                       font-size: 1.5rem; font-weight: 900; margin: 0;'>
                ⚡ COMMAND CENTER
            </h2>
            <p style='color: rgba(255,255,255,0.6); font-size: 0.8rem; margin: 0.5rem 0 0 0;'>
                Predictive Intelligence System
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='glass-divider'></div>", unsafe_allow_html=True)
    
    # Navigation Menu
    selected = option_menu(
        menu_title=None,
        options=["🏠 Tableau de Bord", "🔍 Analyses", "📋 Données"],
        icons=["speedometer2", "graph-up", "table"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "rgba(15, 12, 41, 0.5)", "border-radius": "12px"},
            "icon": {"color": "#667eea", "font-size": "20px"}, 
            "nav-link": {
                "font-size": "14px",
                "text-align": "left",
                "margin":"0px",
                "color": "#ffffff",
                "--hover-color": "rgba(102, 126, 234, 0.3)",
                "background-color": "rgba(15, 12, 41, 0.3)",
                "border-radius": "8px",
            },
            "nav-link-selected": {
                "background": "linear-gradient(135deg, rgba(102, 126, 234, 0.6), rgba(118, 75, 162, 0.6))",
                "color": "#ffffff",
                "font-weight": "700",
                "border": "1px solid rgba(102, 126, 234, 0.8)",
            },
        }
    )
    
    st.markdown("<div class='glass-divider'></div>", unsafe_allow_html=True)
    
    # Control Panel (Filters)
    with st.expander("⚙️ CONTROL PANEL", expanded=True):
        # Vehicle Filters
        st.markdown("**🚗 Sélection Véhicules**")
        vin_values = sorted(df["vin"].dropna().unique()) if "vin" in df.columns else []
        modele_values = sorted(df["modele"].dropna().unique()) if "modele" in df.columns else []
        
        selected_vins = st.multiselect("Filtre VIN", vin_values, default=vin_values)
        selected_modeles = st.multiselect("Filtre Modèle", modele_values, default=modele_values)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Status Filters
        st.markdown("**⚠️ Filtres Statut**")
        statut_values = sorted(df["statut"].dropna().unique()) if "statut" in df.columns else []
        panne_values = sorted(df["panne_type_simple"].dropna().unique()) if "panne_type_simple" in df.columns else []
        
        selected_statuts = st.multiselect("Statut", statut_values, default=statut_values)
        if panne_values:
            selected_pannes = st.multiselect("Types de Panne", panne_values, default=panne_values)
        else:
            selected_pannes = None
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Risk Threshold
        st.markdown("**🎯 Seuil de Risque**")
        prob_min = st.slider("Probabilité Min", 0.0, 1.0, 0.0, 0.05)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Quick Views
        st.markdown("**👁️ Vue Rapide**")
        vue_rapide = st.radio(
            "Mode",
            ["Tous Véhicules", "Urgent (≥70%)", "Surveillance (≥30%)"],
            label_visibility="collapsed"
        )

# ============================================================================
# APPLY FILTERS
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

if "Urgent" in vue_rapide and "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= 0.7]
elif "Surveillance" in vue_rapide and "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= 0.3]

# ============================================================================
# MAIN HEADER
# ============================================================================

st.markdown(f"""
    <div class="main-header">
        <h1>🚀 AutoStream Centre de Commande</h1>
        <p>Intelligence Prédictive de Maintenance • {len(filtered)} / {len(df)} véhicules surveillés</p>
    </div>
""", unsafe_allow_html=True)

# ============================================================================
# CONDITIONAL RENDERING BASED ON MENU SELECTION
# ============================================================================

# ---------------------------------------------------------------------------
# 🏠 TABLEAU DE BORD - VUE CRITIQUE
# ---------------------------------------------------------------------------

if selected == "🏠 Tableau de Bord":
    st.markdown("<br>", unsafe_allow_html=True)
    
    # KPI Cards Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        nb_vehicules = int(filtered["vin"].nunique()) if "vin" in filtered.columns else 0
        st.markdown(f"""
            <div class="kpi-glass">
                <div class="kpi-label">🚗 Flotte Totale</div>
                <div class="kpi-value">{nb_vehicules}</div>
                <div class="kpi-delta">Unités Actives</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        critical_count = int((filtered["statut"] == "CRITIQUE").sum()) if "statut" in filtered.columns else 0
        st.markdown(f"""
            <div class="kpi-glass">
                <div class="kpi-label">🔴 Critiques</div>
                <div class="kpi-value" style="background: linear-gradient(135deg, #ff006e 0%, #ff4d8f 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">{critical_count}</div>
                <div class="kpi-delta">Action Immédiate</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        interventions_count = int((filtered["type_panne_predit"] != 0).sum()) if "type_panne_predit" in filtered.columns else 0
        st.markdown(f"""
            <div class="kpi-glass">
                <div class="kpi-label">🚨 Interventions</div>
                <div class="kpi-value" style="background: linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">{interventions_count}</div>
                <div class="kpi-delta">Planifiées</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col4:
        nb_ok = len(filtered[filtered["type_panne_predit"] == 0]) if "type_panne_predit" in filtered.columns else 0
        health = (nb_ok / len(filtered) * 100) if len(filtered) > 0 else 0
        health_color = "10, 200, 130" if health >= 70 else ("251, 191, 36" if health >= 50 else "255, 0, 110")
        st.markdown(f"""
            <div class="kpi-glass">
                <div class="kpi-label">💚 Santé Flotte</div>
                <div class="kpi-value" style="background: linear-gradient(135deg, rgb({health_color}) 0%, rgba({health_color}, 0.7) 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">{health:.0f}%</div>
                <div class="kpi-delta">Opérationnelle</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # Priority Alerts
    st.markdown("""
        <div class="glass-card">
            <h3 style="color: rgba(255,255,255,0.9); font-weight: 700; margin: 0 0 1rem 0;">
                🚨 ALERTES PRIORITAIRES
            </h3>
        </div>
    """, unsafe_allow_html=True)
    
    if "prob_panne" in filtered.columns and "panne_type_simple" in filtered.columns:
        urgent = filtered[filtered["type_panne_predit"] != 0].copy()
        
        if not urgent.empty:
            urgent_sorted = urgent.sort_values(["statut", "prob_panne"], ascending=[True, False]).head(10)
            
            for idx, row in urgent_sorted.iterrows():
                prob = row.get('prob_panne', 0)
                color = "255, 0, 110" if prob >= 0.7 else ("251, 191, 36" if prob >= 0.5 else "102, 126, 234")
                
                st.markdown(f"""
                    <div class="alert-glass" style="border-left-color: rgb({color});">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong style="color: #ffffff; font-size: 1.1rem;">{row.get('vin', 'N/A')}</strong>
                                <span style="color: rgba(255,255,255,0.6); margin-left: 1rem;">• {row.get('modele', 'N/A')}</span>
                            </div>
                            <div style="text-align: right;">
                                <span style="color: rgb({color}); font-weight: 700; font-size: 1.3rem;">{prob:.0%}</span>
                                <span style="color: rgba(255,255,255,0.6); margin-left: 0.5rem;">• {row.get('panne_type_simple', 'Unknown')}</span>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class="glass-card" style="text-align: center; padding: 3rem;">
                    <h3 style="color: #0ac882; margin: 0;">✅ TOUS SYSTÈMES OPÉRATIONNELS</h3>
                    <p style="color: rgba(255,255,255,0.6); margin: 0.5rem 0 0 0;">Aucune alerte critique détectée</p>
                </div>
            """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# 🔍 ANALYSES - GRAPHIQUES & VISUALISATIONS
# ---------------------------------------------------------------------------

elif selected == "🔍 Analyses":
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 1: 3 Charts
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""<div class="glass-card">
            <h4 style="color: rgba(255,255,255,0.9); font-weight: 600; margin: 0 0 1rem 0;">📊 Risque par Modèle</h4>
        """, unsafe_allow_html=True)
        
        if "modele" in filtered.columns and "score_risque" in filtered.columns:
            by_modele = filtered.groupby("modele", dropna=True)["score_risque"].mean().sort_values(ascending=False)
            fig = px.bar(x=by_modele.values, y=by_modele.index, orientation='h',
                        color=by_modele.values, color_continuous_scale=['#0ac882', '#fbbf24', '#ff006e'])
            fig.update_layout(height=300, margin=dict(l=0,r=0,t=0,b=0), showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='rgba(255,255,255,0.7)', size=10))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("""<div class="glass-card">
            <h4 style="color: rgba(255,255,255,0.9); font-weight: 600; margin: 0 0 1rem 0;">⚠️ Distribution Statuts</h4>
        """, unsafe_allow_html=True)
        
        if "statut" in filtered.columns:
            statut_counts = filtered["statut"].value_counts()
            colors_map = {'OK': '#0ac882', 'ALERTE': '#fbbf24', 'CRITIQUE': '#ff006e', 'SURVEILLANCE': '#667eea'}
            fig = go.Figure(data=[go.Pie(labels=statut_counts.index, values=statut_counts.values,
                marker=dict(colors=[colors_map.get(s, '#999') for s in statut_counts.index]), hole=0.5)])
            fig.update_layout(height=300, margin=dict(l=0,r=0,t=0,b=0), showlegend=False,
                            paper_bgcolor='rgba(0,0,0,0)', font=dict(color='rgba(255,255,255,0.7)', size=10))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col3:
        st.markdown("""<div class="glass-card">
            <h4 style="color: rgba(255,255,255,0.9); font-weight: 600; margin: 0 0 1rem 0;">🔧 Types de Pannes</h4>
        """, unsafe_allow_html=True)
        
        if "panne_type_simple" in filtered.columns:
            panne_counts = filtered["panne_type_simple"].value_counts()
            fig = px.bar(x=panne_counts.index, y=panne_counts.values, 
                        color=panne_counts.values, color_continuous_scale=['#667eea', '#ff006e'])
            fig.update_layout(height=300, margin=dict(l=0,r=0,t=0,b=0), showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='rgba(255,255,255,0.7)', size=10),
                            xaxis=dict(tickangle=-45))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 2: Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""<div class="glass-card">
            <h4 style="color: rgba(255,255,255,0.9); font-weight: 600; margin: 0 0 1rem 0;">📈 Distribution Risques</h4>
        """, unsafe_allow_html=True)
        
        if "prob_panne" in filtered.columns:
            prob_bins = pd.cut(filtered["prob_panne"], bins=[0, 0.3, 0.5, 0.7, 1.0],
                              labels=["Faible", "Moyen", "Élevé", "Critique"])
            prob_dist = prob_bins.value_counts().sort_index()
            colors = ['#0ac882', '#fbbf24', '#ff9800', '#ff006e']
            fig = px.bar(x=prob_dist.index, y=prob_dist.values, color=prob_dist.index,
                        color_discrete_sequence=colors)
            fig.update_layout(height=320, margin=dict(l=0,r=0,t=0,b=0), showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='rgba(255,255,255,0.7)', size=10))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("""<div class="glass-card">
            <h4 style="color: rgba(255,255,255,0.9); font-weight: 600; margin: 0 0 1rem 0;">� Kilométrage Moyen</h4>
        """, unsafe_allow_html=True)
        
        if "statut" in filtered.columns and "km_actuel" in filtered.columns:
            km_by_statut = filtered.groupby("statut", dropna=True)["km_actuel"].mean().sort_values(ascending=False)
            colors_bar = {'CRITIQUE': '#ff006e', 'ALERTE': '#fbbf24', 'SURVEILLANCE': '#667eea', 'OK': '#0ac882'}
            bar_colors = [colors_bar.get(s, '#999') for s in km_by_statut.index]
            fig = px.bar(x=km_by_statut.index, y=km_by_statut.values, 
                        color=km_by_statut.index,
                        color_discrete_map=colors_bar)
            fig.update_layout(height=320, margin=dict(l=0,r=0,t=0,b=0), showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='rgba(255,255,255,0.7)', size=10),
                            yaxis_title="Km Moyen")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown("</div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# 📋 DONNÉES - TABLES AGGRID
# ---------------------------------------------------------------------------

elif selected == "📋 Données":
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Priority Table with AgGrid
    if "prob_panne" in filtered.columns and "panne_type_simple" in filtered.columns:
        urgent = filtered[filtered["type_panne_predit"] != 0].copy()
        
        if not urgent.empty:
            st.markdown("""<div class="glass-card">
                <h3 style="color: rgba(255,255,255,0.9); font-weight: 700; margin: 0 0 1rem 0;">
                    🚨 TABLE MAINTENANCE PRIORITAIRE
                </h3>
            </div>""", unsafe_allow_html=True)
            
            urgent_sorted = urgent.sort_values(["statut", "prob_panne"], ascending=[True, False])
            
            # Prepare display columns
            display_cols = ["vin", "modele", "statut", "panne_type_simple", "prob_panne", "km_actuel"]
            cols_to_show = [col for col in display_cols if col in urgent_sorted.columns]
            aggrid_data = urgent_sorted[cols_to_show].copy()
            
            # Format columns
            if "prob_panne" in aggrid_data.columns:
                aggrid_data["prob_panne"] = (aggrid_data["prob_panne"] * 100).round(1)
            
            # Configure AgGrid
            gb = GridOptionsBuilder.from_dataframe(aggrid_data)
            gb.configure_default_column(cellStyle={'color': '#1a1a1a', 'background-color': 'rgba(255, 255, 255, 0.95)'})
            
            # Conditional formatting with JsCode
            cell_style_jscode = JsCode("""
                function(params) {
                    if (params.data.statut === 'CRITIQUE') {
                        return {
                            'color': '#ffffff',
                            'backgroundColor': 'rgba(255, 0, 110, 0.8)',
                            'fontWeight': '700'
                        }
                    } else if (params.data.statut === 'ALERTE') {
                        return {
                            'color': '#1a1a1a',
                            'backgroundColor': 'rgba(251, 191, 36, 0.7)'
                        }
                    } else if (params.data.statut === 'OK') {
                        return {
                            'color': '#ffffff',
                            'backgroundColor': 'rgba(10, 200, 130, 0.7)'
                        }
                    }
                    return {'color': '#1a1a1a', 'backgroundColor': 'rgba(255, 255, 255, 0.95)'};
                };
            """)
            
            gb.configure_column("statut", cellStyle=cell_style_jscode)
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            gb.configure_side_bar()
            
            gridOptions = gb.build()
            
            AgGrid(
                aggrid_data,
                gridOptions=gridOptions,
                height=400,
                theme='streamlit',
                allow_unsafe_jscode=True,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED
            )
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Complete Fleet Data
    st.markdown("""<div class="glass-card">
        <h3 style="color: rgba(255,255,255,0.9); font-weight: 700; margin: 0 0 1rem 0;">
            📋 BASE DE DONNÉES FLOTTE COMPLÈTE
        </h3>
    </div>""", unsafe_allow_html=True)
    
    core_cols = ["vin", "modele", "statut", "panne_type_simple", "prob_panne", "km_actuel"]
    cols_available = [col for col in core_cols if col in filtered.columns]
    full_data = filtered[cols_available].copy()
    
    if "prob_panne" in full_data.columns:
        full_data["prob_panne"] = (full_data["prob_panne"] * 100).round(1)
    
    gb = GridOptionsBuilder.from_dataframe(full_data)
    gb.configure_default_column(cellStyle={'color': '#1a1a1a', 'background-color': 'rgba(255, 255, 255, 0.95)'})
    
    # Same conditional formatting
    cell_style_jscode = JsCode("""
        function(params) {
            if (params.data.statut === 'CRITIQUE') {
                return {'color': '#ffffff', 'backgroundColor': 'rgba(255, 0, 110, 0.8)', 'fontWeight': '700'}
            } else if (params.data.statut === 'ALERTE') {
                return {'color': '#1a1a1a', 'backgroundColor': 'rgba(251, 191, 36, 0.7)'}
            } else if (params.data.statut === 'OK') {
                return {'color': '#ffffff', 'backgroundColor': 'rgba(10, 200, 130, 0.7)'}
            }
            return {'color': '#1a1a1a', 'backgroundColor': 'rgba(255, 255, 255, 0.95)'};
        };
    """)
    
    gb.configure_column("statut", cellStyle=cell_style_jscode)
    gb.configure_pagination(paginationPageSize=50)
    gb.configure_side_bar()
    
    gridOptions = gb.build()
    
    AgGrid(
        full_data,
        gridOptions=gridOptions,
        height=500,
        theme='streamlit',
        allow_unsafe_jscode=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED
    )
    
    # Export Button
    st.markdown("<br>", unsafe_allow_html=True)
    csv = filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 EXPORTER DONNÉES FLOTTE",
        data=csv,
        file_name=f"autostream_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("""
    <div style="text-align: center; padding: 2rem; border-top: 1px solid rgba(255,255,255,0.1);">
        <p style="color: rgba(255,255,255,0.5); font-size: 0.85rem; margin: 0;">
            <strong style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                           -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
                AUTOSTREAM CENTRE DE COMMANDE
            </strong> • 
            Édition Glassmorphism Sci-Fi • 
            Propulsé par IA Prédictive
        </p>
    </div>
""", unsafe_allow_html=True)
