import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

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
    page_title="AutoStream - Maintenance Prédictive",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Configuration Plotly par défaut
import plotly.io as pio
pio.templates.default = "plotly_white"

# ============================================================================
# SYSTÈME DE THÈMES PERSONNALISÉS
# ============================================================================

if 'theme' not in st.session_state:
    st.session_state.theme = 'Violet Élégant'

THEMES = {
    'Violet Élégant': {
        'primary': '#667eea',
        'secondary': '#764ba2',
        'accent': '#a855f7',
        'bg_start': '#f5f7fa',
        'bg_end': '#c3cfe2',
        'text': '#2c3e50'
    },
    'Bleu Professionnel': {
        'primary': '#2563eb',
        'secondary': '#1e40af',
        'accent': '#3b82f6',
        'bg_start': '#eff6ff',
        'bg_end': '#dbeafe',
        'text': '#1e3a8a'
    },
    'Vert Écologique': {
        'primary': '#10b981',
        'secondary': '#059669',
        'accent': '#34d399',
        'bg_start': '#f0fdf4',
        'bg_end': '#d1fae5',
        'text': '#064e3b'
    }
}

current_theme = THEMES[st.session_state.theme]

# ============================================================================
# EN-TÊTE DE L'APPLICATION
# ============================================================================

# Sélecteur de thème en haut
col_theme1, col_theme2, col_theme3, col_theme4 = st.columns([2, 1, 1, 1])
with col_theme1:
    st.markdown(f"<h3 style='margin:0; padding-top:10px; color:{current_theme['primary']}'>🎨 Sélecteur de Thème</h3>", unsafe_allow_html=True)
with col_theme2:
    if st.button('💜 Violet', use_container_width=True, disabled=st.session_state.theme=='Violet Élégant'):
        st.session_state.theme = 'Violet Élégant'
        st.rerun()
with col_theme3:
    if st.button('💙 Bleu', use_container_width=True, disabled=st.session_state.theme=='Bleu Professionnel'):
        st.session_state.theme = 'Bleu Professionnel'
        st.rerun()
with col_theme4:
    if st.button('💚 Vert', use_container_width=True, disabled=st.session_state.theme=='Vert Écologique'):
        st.session_state.theme = 'Vert Écologique'
        st.rerun()

st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');
    
    /* ========== STYLE GLOBAL PREMIUM ========== */
    * {{
        font-family: 'Poppins', sans-serif;
    }}
    
    .stApp {{
        background: linear-gradient(135deg, {current_theme['bg_start']} 0%, {current_theme['bg_end']} 100%);
    }}
    
    /* ========== HEADER PRINCIPAL ARTISTIQUE ========== */
    .main-header {{
        text-align: center;
        padding: 3rem 2rem;
        background: linear-gradient(135deg, {current_theme['primary']} 0%, {current_theme['secondary']} 100%);
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 20px 60px rgba(102, 126, 234, 0.3);
        position: relative;
        overflow: hidden;
        animation: slideDown 0.6s ease-out;
    }}
    @keyframes slideDown {{
        from {{ opacity: 0; transform: translateY(-30px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    .main-header::before {{
        content: '';
        position: absolute;
        top: -50%;
        right: -50%;
        width: 200%;
        height: 200%;
        background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
        animation: pulse 4s ease-in-out infinite;
    }}
    @keyframes pulse {{
        0%, 100% {{ transform: scale(1); opacity: 0.5; }}
        50% {{ transform: scale(1.1); opacity: 0.8; }}
    }}
    .main-header h1 {{
        color: white;
        margin: 0;
        font-size: 3rem;
        font-weight: 700;
        text-shadow: 0 2px 20px rgba(0,0,0,0.2);
        position: relative;
        z-index: 1;
    }}
    .main-header p {{
        color: rgba(255,255,255,0.95);
        margin: 0.8rem 0 0 0;
        font-size: 1.2rem;
        font-weight: 300;
        position: relative;
        z-index: 1;
    }}
    
    /* ========== SECTION HEADERS ÉLÉGANTS ========== */
    .section-header {{
        padding: 1.5rem 2rem;
        background: linear-gradient(120deg, #fdfbfb 0%, #ebedee 100%);
        border-radius: 15px;
        margin: 2rem 0 1.5rem 0;
        border-left: 5px solid {current_theme['primary']};
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        animation: fadeInUp 0.5s ease-out;
    }}
    @keyframes fadeInUp {{
        from {{ opacity: 0; transform: translateY(20px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    .section-header h2 {{
        margin: 0;
        color: {current_theme['text']};
        font-weight: 600;
        font-size: 1.8rem;
    }}
    .section-header p {{
        margin: 0.3rem 0 0 0;
        color: #7f8c8d;
        font-size: 0.95rem;
        font-weight: 300;
    }}
    
    /* ========== KPI CARDS PREMIUM ========== */
    [data-testid="stMetricValue"] {{
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        background: linear-gradient(135deg, {current_theme['primary']} 0%, {current_theme['secondary']} 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }}
    [data-testid="stMetricLabel"] {{
        font-size: 0.9rem !important;
        font-weight: 500 !important;
        color: #5a6c7d !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }}
    [data-testid="stMetricDelta"] {{
        font-size: 0.85rem !important;
    }}
    div[data-testid="metric-container"] {{
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border: 1px solid {current_theme['primary']}33;
        transition: all 0.3s ease;
        animation: scaleIn 0.4s ease-out;
    }}
    @keyframes scaleIn {{
        from {{ opacity: 0; transform: scale(0.9); }}
        to {{ opacity: 1; transform: scale(1); }}
    }}
    div[data-testid="metric-container"]:hover {{
        transform: translateY(-5px);
        box-shadow: 0 8px 30px {current_theme['primary']}44;
    }}
    
    /* ========== CARTES D'ALERTE ARTISTIQUES ========== */
    .info-box, .warning-box, .success-box, .danger-box {{
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1.5rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        border: none;
        position: relative;
        overflow: hidden;
        min-height: 280px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }}
    .info-box::before, .warning-box::before, .success-box::before, .danger-box::before {{
        content: '';
        position: absolute;
        left: 0;
        top: 0;
        height: 100%;
        width: 5px;
    }}
    .info-box {{ 
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); 
    }}
    .info-box::before {{ background: #2196F3; }}
    
    .warning-box {{ 
        background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%); 
    }}
    .warning-box::before {{ background: #ff9800; }}
    
    .success-box {{ 
        background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%); 
    }}
    .success-box::before {{ background: #4caf50; }}
    
    .danger-box {{ 
        background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%); 
    }}
    .danger-box::before {{ background: #f44336; }}
    
    /* ========== SIDEBAR PREMIUM ========== */
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #2c3e50 0%, #34495e 100%);
    }}
    section[data-testid="stSidebar"] h1, 
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {{
        color: white !important;
    }}
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] label {{
        color: rgba(255,255,255,0.9) !important;
    }}
    section[data-testid="stSidebar"] .stMarkdown {{
        color: rgba(255,255,255,0.9);
    }}
    
    /* ========== DATAFRAMES ÉLÉGANTS ========== */
    [data-testid="stDataFrame"] {{
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }}
    
    /* ========== EXPANDERS STYLÉS ========== */
    [data-testid="stExpander"] {{
        background: white;
        border-radius: 12px;
        border: 1px solid rgba(102, 126, 234, 0.2);
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        margin: 1rem 0;
    }}
    
    /* ========== BOUTONS PREMIUM ========== */
    .stButton button {{
        background: linear-gradient(135deg, {current_theme['primary']} 0%, {current_theme['secondary']} 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.6rem 2rem;
        font-weight: 600;
        box-shadow: 0 4px 15px {current_theme['primary']}55;
        transition: all 0.3s ease;
    }}
    .stButton button:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 20px {current_theme['primary']}77;
    }}
    .stButton button:disabled {{
        background: linear-gradient(135deg, #94a3b8 0%, #64748b 100%);
        cursor: not-allowed;
        opacity: 0.6;
    }}
    
    /* ========== SÉPARATEURS ARTISTIQUES ========== */
    hr {{
        margin: 3rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, {current_theme['primary']}, transparent);
    }}
    
    /* ========== SKELETON LOADERS ========== */
    @keyframes shimmer {{
        0% {{ background-position: -1000px 0; }}
        100% {{ background-position: 1000px 0; }}
    }}
    .skeleton {{
        background: linear-gradient(90deg, #f0f0f0 25%, #e0e0e0 50%, #f0f0f0 75%);
        background-size: 1000px 100%;
        animation: shimmer 2s infinite;
        border-radius: 8px;
    }}
    .skeleton-text {{
        height: 16px;
        margin: 8px 0;
    }}
    .skeleton-header {{
        height: 40px;
        width: 60%;
        margin-bottom: 20px;
    }}
    
    /* ========== ANIMATIONS ========== */
    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(20px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    .fade-in {{
        animation: fadeIn 0.5s ease-out;
    }}
    .stApp > div > div > div {{
        animation: fadeIn 0.5s ease-out;
    }}
    </style>
""", unsafe_allow_html=True)

st.markdown("""
    <div class="main-header">
        <h1>🚗 AutoStream - Maintenance Prédictive Intelligente</h1>
        <p>Anticipez les pannes avant qu'elles n'arrivent grâce à l'intelligence artificielle</p>
    </div>
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
# GUIDE D'UTILISATION RAPIDE (EXPANDABLE)
# ============================================================================

with st.expander("📖 Guide d'utilisation - Cliquez pour comprendre ce dashboard", expanded=False):
    st.markdown("""
    ### 🎯 Objectif de cette plateforme
    
    Ce dashboard utilise **l'intelligence artificielle** pour analyser les données de vos véhicules 
    et **prédire les pannes avant qu'elles ne surviennent**. Cela vous permet de :
    
    - 🔴 **Éviter les pannes imprévues** et l'immobilisation des véhicules
    - 💰 **Réduire les coûts** de maintenance corrective
    - 📅 **Planifier** les interventions au meilleur moment
    - 🛡️ **Améliorer la sécurité** en détectant les problèmes critiques
    
    ---
    
    ### 🔍 Comment ça marche ?
    
    1. **Collecte de données** : Les capteurs OBD du véhicule enregistrent en temps réel :
       - Température moteur, pression d'huile, régime moteur, voltage batterie, kilométrage
    
    2. **Analyse par IA** : Un modèle de Machine Learning (RandomForest) analyse ces données
       et compare avec 200+ historiques de pannes
    
    3. **Prédiction** : Le système calcule :
       - Le **type de panne** probable (Batterie, Moteur, Freins, Turbo)
       - La **probabilité** que cette panne arrive (0-100%)
         - L'**échéance estimée (km)** avant la panne
    
    ---
    
    ### 📊 Comment lire les indicateurs ?
    
    **Niveaux d'alerte :**
    - 🔴 **URGENT (≥70%)** : Intervention nécessaire sous 48h
    - 🟠 **ATTENTION (50-70%)** : Planifier maintenance sous 7 jours
    - 🟡 **SURVEILLANCE (30-50%)** : Surveiller de près
    - 🟢 **NORMAL (<30%)** : Aucune action immédiate
    
    **Probabilité de panne :**
    - 90-100% : Panne quasi-certaine si pas d'intervention
    - 70-90% : Risque élevé, action recommandée
    - 50-70% : Risque modéré, planification nécessaire
    - 0-50% : Risque faible, maintenance préventive standard
    
    ---
    
    ### 🛠️ Utilisation des filtres (sidebar à gauche)
    
    - **Filtres par VIN/Modèle** : Isoler des véhicules spécifiques
    - **Seuil de probabilité** : Ajuster pour voir uniquement les cas à risque
    - **Vue Rapide** : Accès direct aux véhicules urgents
    """)

st.markdown("---")

# ============================================================================
# SEPARATION BIG DATA VS IA
# ============================================================================

st.markdown("## 🧭 Sources d'Information : Donnees, Metier, IA")

col_raw, col_business, col_ai = st.columns(3)

with col_raw:
    st.markdown("""
    <div class="info-box">
        <h4>📥 Donnees brutes</h4>
        <p>Informations issues des sources et capteurs.</p>
        <ul>
            <li>Telemetrie OBD</li>
            <li>Maintenance (dates, type, km)</li>
            <li>Flotte (modele, annee)</li>
        </ul>
        <p><em>Pas de calcul, juste les valeurs de base.</em></p>
    </div>
    """, unsafe_allow_html=True)

with col_business:
    st.markdown("""
    <div class="success-box">
        <h4>🧮 Calculs metier</h4>
        <p>Regles deterministes basees sur les donnees.</p>
        <ul>
            <li>Score risque</li>
            <li>Km depuis revision / echeance</li>
            <li>Action requise</li>
        </ul>
        <p><em>Ce sont des regles fixes, sans IA.</em></p>
    </div>
    """, unsafe_allow_html=True)

with col_ai:
    st.markdown("""
    <div class="warning-box">
        <h4>🤖 IA (Prediction ML)</h4>
        <p>Resultats du modele de Machine Learning.</p>
        <ul>
            <li>Type de panne predit</li>
            <li>Probabilite de panne</li>
            <li>Statut (OK / ALERTE / CRITIQUE)</li>
        </ul>
        <p><em>C'est la partie predictive de l'application.</em></p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

st.markdown("---")

# ============================================================================
# RÉSUMÉ EXÉCUTIF - VUE D'ENSEMBLE RAPIDE
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>🎯 Résumé Exécutif</h2>
    <p>Indicateurs stratégiques pour une prise de décision rapide</p>
</div>
""", unsafe_allow_html=True)

if "prob_panne" in df.columns and "type_panne_predit" in df.columns:
    col_exec1, col_exec2, col_exec3 = st.columns(3)
    
    with col_exec1:
        # Trouver le véhicule le PLUS critique parmi TOUS les véhicules avec panne
        vehicles_with_panne = df[df["type_panne_predit"] != 0]
        
        if not vehicles_with_panne.empty:
            # Trier par probabilité décroissante
            top_critical = vehicles_with_panne.nlargest(1, "prob_panne").iloc[0]
            
            # Déterminer la couleur selon le statut
            if top_critical['statut'] == "CRITIQUE":
                box_class = "danger-box"
                icon = "🔴"
            elif top_critical['statut'] == "ALERTE":
                box_class = "warning-box"
                icon = "🟠"
            else:
                box_class = "info-box"
                icon = "🟡"
            
            st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%);
                    padding: 2rem;
                    border-radius: 20px;
                    box-shadow: 0 8px 25px rgba(244, 67, 54, 0.2);
                    border: 2px solid rgba(244, 67, 54, 0.3);
                    text-align: center;
                    min-height: 400px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                ">
                    <div style="font-size: 3rem; margin-bottom: 0.5rem;">{icon}</div>
                    <h3 style="color: #c62828; font-weight: 700; margin: 0.5rem 0;">PRIORITÉ ABSOLUE</h3>
                    <div style="background: white; padding: 1rem; border-radius: 10px; margin: 1rem 0; flex-grow: 1; display: flex; flex-direction: column; justify-content: center;">
                        <p style="margin: 0.3rem 0; font-size: 0.9rem; color: #666;"><strong>VIN</strong></p>
                        <p style="margin: 0; font-size: 1.1rem; font-weight: 600; color: #2c3e50;">{top_critical['vin']}</p>
                    </div>
                    <div style="display: flex; justify-content: space-between; gap: 0.5rem;">
                        <div style="flex: 1; background: rgba(255,255,255,0.7); padding: 0.8rem; border-radius: 8px;">
                            <p style="margin: 0; font-size: 0.75rem; color: #666;">Type</p>
                            <p style="margin: 0; font-weight: 600; color: #2c3e50;">{top_critical.get('panne_type_simple', 'N/A')}</p>
                        </div>
                        <div style="flex: 1; background: rgba(255,255,255,0.7); padding: 0.8rem; border-radius: 8px;">
                            <p style="margin: 0; font-size: 0.75rem; color: #666;">Risque</p>
                            <p style="margin: 0; font-weight: 700; font-size: 1.3rem; color: #c62828;">{top_critical['prob_panne']:.0%}</p>
                        </div>
                    </div>
                    <p style="margin-top: 1rem; color: #c62828; font-weight: 600; font-size: 0.9rem;">⚠️ INTERVENTION IMMÉDIATE</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%);
                    padding: 2rem;
                    border-radius: 20px;
                    box-shadow: 0 8px 25px rgba(76, 175, 80, 0.2);
                    border: 2px solid rgba(76, 175, 80, 0.3);
                    text-align: center;
                    min-height: 400px;
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                ">
                    <div style="font-size: 3rem; margin-bottom: 0.5rem;">✅</div>
                    <h3 style="color: #2e7d32; font-weight: 700; margin: 0.5rem 0;">FLOTTE OPTIMALE</h3>
                    <p style="font-size: 1.1rem; color: #1b5e20; margin: 1rem 0;">Aucune panne détectée</p>
                    <p style="color: #2e7d32; font-weight: 500;">✨ Continuez la maintenance préventive</p>
                </div>
            """, unsafe_allow_html=True)
    
    with col_exec2:
        panne_counts = df[df["type_panne_predit"] != 0].groupby("panne_type_simple").size()
        if not panne_counts.empty:
            most_common = panne_counts.idxmax()
            count = panne_counts.max()
            st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
                    padding: 2rem;
                    border-radius: 20px;
                    box-shadow: 0 8px 25px rgba(255, 152, 0, 0.2);
                    border: 2px solid rgba(255, 152, 0, 0.3);
                    text-align: center;
                    min-height: 400px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                ">
                    <div style="font-size: 3rem; margin-bottom: 0.5rem;">📈</div>
                    <h3 style="color: #e65100; font-weight: 700; margin: 0.5rem 0;">TENDANCE DÉTECTÉE</h3>
                    <div style="background: white; padding: 1.5rem; border-radius: 12px; margin: 1rem 0; flex-grow: 1; display: flex; flex-direction: column; justify-content: center;">
                        <p style="font-size: 2.5rem; font-weight: 700; margin: 0; color: #e65100;">{count}</p>
                        <p style="margin: 0.3rem 0 0 0; color: #666;">véhicules concernés</p>
                    </div>
                    <div style="background: rgba(255,255,255,0.7); padding: 1rem; border-radius: 10px;">
                        <p style="margin: 0; font-size: 0.8rem; color: #666;">Type de panne</p>
                        <p style="margin: 0.3rem 0 0 0; font-weight: 600; color: #2c3e50; font-size: 1.1rem;">{most_common}</p>
                    </div>
                    <p style="margin-top: 1rem; color: #e65100; font-weight: 600; font-size: 0.9rem;">💡 Anticiper approvisionnement</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
                    padding: 2rem;
                    border-radius: 20px;
                    box-shadow: 0 8px 25px rgba(33, 150, 243, 0.2);
                    border: 2px solid rgba(33, 150, 243, 0.3);
                    text-align: center;
                    min-height: 400px;
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                ">
                    <div style="font-size: 3rem; margin-bottom: 0.5rem;">📉</div>
                    <h3 style="color: #1565c0; font-weight: 700; margin: 0.5rem 0;">SITUATION STABLE</h3>
                    <p style="font-size: 1.1rem; color: #0d47a1; margin: 1rem 0;">Flotte en bon état</p>
                    <p style="color: #1565c0; font-weight: 500;">✨ Maintenance efficace</p>
                </div>
            """, unsafe_allow_html=True)
    
    with col_exec3:
        # Calculer santé basée sur le % de véhicules OK
        nb_ok = len(df[df["type_panne_predit"] == 0])
        nb_total = len(df)
        health_percentage = (nb_ok / nb_total) * 100 if nb_total > 0 else 0
        
        nb_critiques = len(df[df["statut"] == "CRITIQUE"])
        nb_alertes = len(df[df["statut"] == "ALERTE"])
        
        if health_percentage >= 70:
            box_class = "success-box"
            icon = "💚"
            status = "EXCELLENT"
        elif health_percentage >= 50:
            box_class = "warning-box"
            icon = "💛"
            status = "ACCEPTABLE"
        else:
            box_class = "danger-box"
            icon = "❤️"
            status = "ATTENTION REQUISE"
        
        gradient_start = '#e8f5e9' if health_percentage >= 70 else ('#fff3e0' if health_percentage >= 50 else '#ffebee')
        gradient_end = '#c8e6c9' if health_percentage >= 70 else ('#ffe0b2' if health_percentage >= 50 else '#ffcdd2')
        border_color = '#4caf50' if health_percentage >= 70 else ('#ff9800' if health_percentage >= 50 else '#f44336')
        text_color = '#2e7d32' if health_percentage >= 70 else ('#e65100' if health_percentage >= 50 else '#c62828')
        
        st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, {gradient_start} 0%, {gradient_end} 100%);
                padding: 2rem;
                border-radius: 20px;
                box-shadow: 0 8px 25px rgba(0,0,0,0.15);
                border: 2px solid {border_color};
                text-align: center;
                min-height: 400px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
                <div style="font-size: 3rem; margin-bottom: 0.5rem;">{icon}</div>
                <h3 style="color: {text_color}; font-weight: 700; margin: 0.5rem 0;">SANTÉ GLOBALE</h3>
                <div style="background: white; padding: 1.5rem; border-radius: 12px; margin: 1rem 0; flex-grow: 1; display: flex; flex-direction: column; justify-content: center;">
                    <p style="font-size: 3.5rem; font-weight: 700; margin: 0; color: {text_color};">{health_percentage:.0f}<span style="font-size: 2rem;">%</span></p>
                    <p style="margin: 0.5rem 0 0 0; color: {text_color}; font-weight: 600; font-size: 1.2rem;">{status}</p>
                </div>
                <div style="display: flex; gap: 0.5rem;">
                    <div style="flex: 1; background: rgba(255,255,255,0.7); padding: 0.8rem; border-radius: 8px;">
                        <p style="margin: 0; font-size: 1.8rem; font-weight: 700; color: #4caf50;">{nb_ok}</p>
                        <p style="margin: 0; font-size: 0.75rem; color: #666;">Opérationnels</p>
                    </div>
                    <div style="flex: 1; background: rgba(255,255,255,0.7); padding: 0.8rem; border-radius: 8px;">
                        <p style="margin: 0; font-size: 1.8rem; font-weight: 700; color: #f44336;">{nb_critiques}</p>
                        <p style="margin: 0; font-size: 0.75rem; color: #666;">Critiques</p>
                    </div>
                    <div style="flex: 1; background: rgba(255,255,255,0.7); padding: 0.8rem; border-radius: 8px;">
                        <p style="margin: 0; font-size: 1.8rem; font-weight: 700; color: #ff9800;">{nb_alertes}</p>
                        <p style="margin: 0; font-size: 0.75rem; color: #666;">Alertes</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

st.markdown("---")

# ============================================================================
# FILTRES EN ONGLETS HORIZONTAUX MODERNES
# ============================================================================

st.markdown(f"""
<div style="
    background: white;
    padding: 1rem 2rem;
    border-radius: 15px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    border-left: 5px solid {current_theme['primary']};
    margin: 2rem 0;
">
    <h3 style="margin: 0; color: {current_theme['primary']};">🔍 Panneau de Filtres Interactifs</h3>
    <p style="margin: 0.5rem 0 0 0; color: #666; font-size: 0.9rem;">Affinez votre analyse en sélectionnant des critères</p>
</div>
""", unsafe_allow_html=True)

# Extraction des valeurs uniques
vin_values = sorted(df["vin"].dropna().unique()) if "vin" in df.columns else []
modele_values = sorted(df["modele"].dropna().unique()) if "modele" in df.columns else []
statut_values = sorted(df["statut"].dropna().unique()) if "statut" in df.columns else []
panne_values = sorted(df["panne_type_simple"].dropna().unique()) if "panne_type_simple" in df.columns else []

tab1, tab2, tab3, tab4 = st.tabs(["🚗 Véhicules", "⚠️ Statuts & Pannes", "🎯 Seuil de Risque", "👁️ Vues Rapides"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        selected_vins = st.multiselect(
            "🆔 VIN (Numéro d'identification)", 
            vin_values, 
            default=vin_values,
            help="Sélectionnez un ou plusieurs véhicules spécifiques"
        )
    with col2:
        selected_modeles = st.multiselect(
            "🚙 Modèle de véhicule", 
            modele_values, 
            default=modele_values,
            help="Filtrer par type/modèle de véhicule"
        )

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        selected_statuts = st.multiselect(
            "🚦 Statut opérationnel", 
            statut_values, 
            default=statut_values,
            help="OK, ALERTE ou CRITIQUE selon les seuils définis"
        )
    with col2:
        if panne_values:
            selected_pannes = st.multiselect(
                "🔧 Type de panne prédit", 
                panne_values, 
                default=panne_values,
                help="Type de défaillance anticipée par l'IA"
            )
        else:
            selected_pannes = None
            st.info("Aucune panne détectée dans les données")

with tab3:
    prob_min = st.slider(
        "📊 Probabilité minimale de panne", 
        0.0, 1.0, 0.0, 0.05,
        help="Afficher uniquement les véhicules au-dessus de ce seuil"
    )
    st.caption(f"🔍 Affiche les véhicules avec ≥ {prob_min:.0%} de risque")

with tab4:
    vue_rapide = st.radio(
        "🎨 Mode d'affichage",
        ["📊 Tous les véhicules", "🔴 Uniquement urgents (≥70%)", "🟡 En surveillance (≥30%)"],
        index=0,
        help="Sélectionnez une vue prédéfinie pour accès rapide",
        horizontal=True
    )

# Application des filtres
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
if "🔴 Uniquement urgents" in vue_rapide and "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= 0.7]
elif "🟡 En surveillance" in vue_rapide and "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= 0.3]

# Info sur le filtrage
st.markdown(f"""
<div style="
    background: linear-gradient(135deg, {current_theme['primary']}22, {current_theme['secondary']}22);
    padding: 1rem 2rem;
    border-radius: 10px;
    text-align: center;
    margin: 1rem 0 2rem 0;
    border: 2px solid {current_theme['primary']}44;
">
    <p style="margin: 0; font-weight: 600; color: {current_theme['text']};"><strong>{len(filtered)}</strong> véhicules affichés sur <strong>{len(df)}</strong> total</p>
</div>
""", unsafe_allow_html=True)

# ============================================================================
# INDICATEURS CLÉS (KPIs) AVEC SPARKLINES
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>📊 Indicateurs Clés de Performance</h2>
    <p>Vue d'ensemble des métriques essentielles de votre flotte</p>
</div>
""", unsafe_allow_html=True)

# Ajout d'un petit espace
st.markdown("<br>", unsafe_allow_html=True)

# Fonction pour créer des sparklines
def create_sparkline(values, color):
    """Crée un mini graphique de tendance"""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=values,
        mode='lines',
        line=dict(color=color, width=2),
        fill='tozeroy',
        fillcolor=f'rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.2)',
        hovertemplate='%{y}<extra></extra>'
    ))
    fig.update_layout(
        height=60,
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
    )
    return fig

# Génération de données de tendance simulées (basées sur les données actuelles)
import numpy as np
np.random.seed(42)

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    nb_vehicules = int(filtered["vin"].nunique()) if "vin" in filtered.columns else 0
    total_vehicules = int(df["vin"].nunique())
    st.metric(
        label="🚗 Véhicules",
        value=nb_vehicules,
        help="Nombre de véhicules dans la sélection actuelle"
    )
    # Sparkline simulant l'évolution du nombre de véhicules
    trend = [total_vehicules * (1 + np.random.uniform(-0.05, 0.05)) for _ in range(10)]
    trend[-1] = nb_vehicules
    st.plotly_chart(create_sparkline(trend, current_theme['primary']), use_container_width=True, config={'displayModeBar': False})

with kpi2:
    avg_score = filtered["score_risque"].mean() if "score_risque" in filtered.columns else 0
    st.metric(
        label="⚠️ Score Risque (moyen)",
        value=f"{avg_score:.1f}",
        help="Indice de risque moyen calcule a partir des facteurs: km, age, maintenance"
    )
    # Sparkline du score de risque
    trend = [avg_score * (1 + np.random.uniform(-0.2, 0.2)) for _ in range(10)]
    trend[-1] = avg_score
    st.plotly_chart(create_sparkline(trend, '#ff9800'), use_container_width=True, config={'displayModeBar': False})

with kpi3:
    critical_count = int((filtered["statut"] == "CRITIQUE").sum()) if "statut" in filtered.columns else 0
    st.metric(
        label="🔴 Critiques",
        value=critical_count,
        help="Véhicules avec panne détectée et probabilité ≥ 70%"
    )
    # Sparkline des critiques
    trend = [max(0, critical_count + np.random.randint(-2, 3)) for _ in range(10)]
    trend[-1] = critical_count
    st.plotly_chart(create_sparkline(trend, '#f44336'), use_container_width=True, config={'displayModeBar': False})

with kpi4:
    if "type_panne_predit" in filtered.columns:
        # Compter TOUS les véhicules avec panne prédite
        interventions_count = int((filtered["type_panne_predit"] != 0).sum())
        st.metric(
            label="🚨 Interventions",
            value=interventions_count,
            help="Nombre de véhicules nécessitant intervention (panne prédite par le ML)"
        )
        # Sparkline des interventions
        trend = [max(0, interventions_count + np.random.randint(-3, 4)) for _ in range(10)]
        trend[-1] = interventions_count
        st.plotly_chart(create_sparkline(trend, '#e91e63'), use_container_width=True, config={'displayModeBar': False})
    else:
        st.metric("🚨 Interventions", "N/A")

# Retirer la note confuse sur le chevauchement

with kpi5:
    if "km_actuel" in filtered.columns:
        avg_km = int(filtered["km_actuel"].mean())
        st.metric(
            label="📍 Km Moyen",
            value=f"{avg_km:,}".replace(",", " "),
            help="Kilométrage moyen de la flotte sélectionnée"
        )
        # Sparkline du kilométrage
        trend = [avg_km * (1 + i * 0.01) for i in range(10)]
        st.plotly_chart(create_sparkline(trend, '#4caf50'), use_container_width=True, config={'displayModeBar': False})
    else:
        st.metric("📍 Km Moyen", "N/A")

# Espace supplémentaire après les KPIs
st.markdown("<br>", unsafe_allow_html=True)

# Info technique en bas des KPIs
with st.expander("💡 Guide d'interprétation des indicateurs", expanded=False):
    st.markdown("""
    ### 📖 Comprendre vos KPIs
    
    | Indicateur | Description | Action recommandée |
    |------------|-------------|-------------------|
    | 🚗 **Véhicules** | Nombre total dans la sélection active | Vérifier les filtres appliqués |
    | ⚠️ **Score Risque** | Métrique métier (0-100) basée sur âge/km/température | Valeur informative uniquement |
    | 🔴 **Critiques** | Véhicules avec probabilité ≥70% de panne | 🚨 Intervention sous 48h |
    | 🚨 **Interventions** | Tous véhicules avec panne prédite (>0%) | 📅 Planifier maintenance |
    | 📍 **Km Moyen** | Kilométrage moyen de la flotte | Indicateur d'usure globale |
    
    **💡 Astuce :** Les statuts (OK/ALERTE/CRITIQUE) sont générés par le modèle d'IA, pas par le score risque métier.
    """)

st.markdown("---")

# ============================================================================
# SECTION MAINTENANCE PRIORITAIRE - EXPLICATIONS DÉTAILLÉES
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>🚨 Liste de Maintenance Prioritaire</h2>
    <p>Véhicules nécessitant une attention immédiate</p>
</div>
""", unsafe_allow_html=True)

if "prob_panne" in filtered.columns and "panne_type_simple" in filtered.columns:
    # Filtrer TOUS les véhicules avec panne détectée (exclure uniquement les OK)
    urgent = filtered[
        (filtered["type_panne_predit"] != 0)  # Exclure uniquement les véhicules OK
    ].copy()
    
    if not urgent.empty:
        # Compter par catégorie
        nb_critiques = len(urgent[urgent["statut"] == "CRITIQUE"])
        nb_alertes = len(urgent[urgent["statut"] == "ALERTE"])
        nb_surveillance = len(urgent[urgent["statut"] == "SURVEILLANCE"])
        
        st.markdown(f"""
        <div class="{'danger-box' if nb_critiques > 5 else 'warning-box'}" style="padding: 1rem; margin: 1rem 0;">
            <h4>🚨 {len(urgent)} véhicule(s) nécessitant attention</h4>
            <p>🔴 {nb_critiques} critiques | 🟠 {nb_alertes} alertes | 🟡 {nb_surveillance} surveillance</p>
        </div>
        """, unsafe_allow_html=True)
        
        urgent_sorted = urgent.sort_values(["statut", "prob_panne"], ascending=[True, False])
        
        # Préparation du tableau
        priority_display = urgent_sorted.copy()
        
        # Ajouter recommandations
        if "panne_type_simple" in priority_display.columns:
            priority_display["action"] = priority_display["panne_type_simple"].apply(
                lambda x: RECOMMANDATIONS.get(x, ["Diagnostic requis"])[0]
            )
        
        # Formatage des colonnes
        display_cols = {
            "alerte_emoji": "🚦",
            "vin": "VIN",
            "modele": "Modèle",
            "statut": "Statut",
            "panne_emoji": "🔧",
            "panne_type_simple": "Panne",
            "prob_panne": "Probabilité",
            "km_estime": "Échéance (km)",
            "km_actuel": "Km Total",
            "km_depuis_revis": "Km depuis révision",
            "action": "Action Requise"
        }

        cols_to_show = [col for col in display_cols.keys() if col in priority_display.columns]
        priority_table = priority_display[cols_to_show].copy()
        priority_table.columns = [display_cols[col] for col in cols_to_show]

        # Formatage des valeurs
        if "Probabilité" in priority_table.columns:
            priority_table["Probabilité"] = priority_table["Probabilité"].apply(lambda x: f"{x:.1%}")
        if "Km Total" in priority_table.columns:
            priority_table["Km Total"] = priority_table["Km Total"].apply(lambda x: f"{int(x):,}".replace(",", " "))
        if "Km depuis révision" in priority_table.columns:
            priority_table["Km depuis révision"] = priority_table["Km depuis révision"].apply(lambda x: f"{int(x):,}".replace(",", " "))
        if "Échéance (km)" in priority_table.columns:
            priority_table["Échéance (km)"] = priority_table["Échéance (km)"].apply(
                lambda x: "⚠️ Immédiat" if pd.notna(x) and int(x) == 0 else (f"{int(x):,} km".replace(",", " ") if pd.notna(x) else "N/A")
            )

        # Affichage du tableau
        st.dataframe(
            priority_table,
            use_container_width=True,
            hide_index=True,
            height=300
        )
        
        # Actions recommandées COMPACT avec selectbox
        st.markdown("#### 🔧 Détails d'intervention")
        
        vehicle_options = [f"{row.get('alerte_emoji', '🔴')} {row.get('vin', 'N/A')} - {row.get('panne_type_simple', 'Inconnu')} ({row.get('prob_panne', 0):.0%})" 
                          for idx, row in urgent_sorted.iterrows()]
        
        selected_vehicle = st.selectbox(
            "Sélectionnez un véhicule pour voir les détails",
            vehicle_options,
            label_visibility="collapsed"
        )
        
        if selected_vehicle:
            selected_idx = vehicle_options.index(selected_vehicle)
            row = urgent_sorted.iloc[selected_idx]
            panne_type = row.get("panne_type_simple", "Inconnu")
            
            col_det1, col_det2 = st.columns([2, 1])
            
            with col_det1:
                st.markdown("**🔧 Actions à effectuer :**")
                actions = RECOMMANDATIONS.get(panne_type, ["Diagnostic complet requis"])
                for i, action in enumerate(actions, 1):
                    st.markdown(f"{i}. {action}")
                
                st.info(PANNE_DESCRIPTIONS.get(panne_type, "Description non disponible"))
            
            with col_det2:
                st.markdown("**📊 Données OBD**")
                if "temp_moteur" in row:
                    st.metric("🌡️ Temp.", f"{row['temp_moteur']:.1f}°C", 
                             delta="🔥 Élevée" if row['temp_moteur'] > 100 else None)
                if "voltage_batterie" in row:
                    st.metric("⚡ Voltage", f"{row['voltage_batterie']:.2f}V",
                             delta="🔋 Faible" if row['voltage_batterie'] < 12.0 else None)
                if "pression_huile" in row:
                    st.metric("🛢️ Press.", f"{row['pression_huile']:.2f} bar",
                             delta="⚠️ Basse" if row['pression_huile'] < 2.5 else None)
        
        # Bouton export compact
        csv = urgent_sorted.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Export CSV",
            data=csv,
            file_name=f"maintenance_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv"
        )
        
    else:
        st.markdown("""
            <div class="success-box">
                <h3>✅ Excellent ! Aucune panne détectée</h3>
                <p>Tous vos véhicules sont prédits comme étant en bon état (statut OK).</p>
                <p><strong>Recommandation :</strong> Continuez la maintenance préventive régulière.</p>
            </div>
        """, unsafe_allow_html=True)

st.markdown("---")

# ============================================================================
# TABLE COMPLÈTE - VUE DÉTAILLÉE
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>📋 Vue Détaillée Complète</h2>
    <p>Données exhaustives de tous les véhicules de votre sélection</p>
</div>
""", unsafe_allow_html=True)

display_data = filtered.copy()

core_cols = ["alerte_emoji", "vin", "modele", "statut", "panne_emoji", "panne_type_simple", "prob_panne"]
if "km_estime" in display_data.columns:
    core_cols.append("km_estime")

obd_cols = ["temp_moteur", "pression_huile", "voltage_batterie", "km_actuel", "km_depuis_revis"]

all_display_cols = core_cols + obd_cols
existing_cols = [col for col in all_display_cols if col in display_data.columns]

table_display = display_data[existing_cols].copy()

# Renommage
col_names = {
    "alerte_emoji": "🚦",
    "vin": "VIN",
    "modele": "Modèle",
    "statut": "Statut",
    "panne_emoji": "🔧",
    "panne_type_simple": "Type Panne",
    "prob_panne": "Prob.",
    "km_estime": "Échéance (km)",
    "score_risque": "Score Risque",
    "temp_moteur": "Temp.(°C)",
    "pression_huile": "Press.(bar)",
    "regime_moteur": "RPM",
    "voltage_batterie": "Volt.(V)",
    "km_actuel": "Km Total",
    "km_depuis_revis": "Km / Révis."
}

table_display.columns = [col_names.get(col, col) for col in table_display.columns]

# Formatage
if "Prob." in table_display.columns:
    table_display["Prob."] = table_display["Prob."].apply(lambda x: f"{x:.0%}" if pd.notna(x) else "N/A")
if "Score Risque" in table_display.columns:
    table_display["Score Risque"] = table_display["Score Risque"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
if "Temp.(°C)" in table_display.columns:
    table_display["Temp.(°C)"] = table_display["Temp.(°C)"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
if "Press.(bar)" in table_display.columns:
    table_display["Press.(bar)"] = table_display["Press.(bar)"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
if "RPM" in table_display.columns:
    table_display["RPM"] = table_display["RPM"].apply(lambda x: f"{int(x):,}".replace(",", " ") if pd.notna(x) else "N/A")
if "Volt.(V)" in table_display.columns:
    table_display["Volt.(V)"] = table_display["Volt.(V)"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
if "Km Total" in table_display.columns:
    table_display["Km Total"] = table_display["Km Total"].apply(lambda x: f"{int(x):,}".replace(",", " ") if pd.notna(x) else "N/A")
if "Km / Révis." in table_display.columns:
    table_display["Km / Révis."] = table_display["Km / Révis."].apply(lambda x: f"{int(x):,}".replace(",", " ") if pd.notna(x) else "N/A")
if "Échéance (km)" in table_display.columns:
    table_display["Échéance (km)"] = table_display["Échéance (km)"].apply(
        lambda x: "⚠️ Immédiat" if pd.notna(x) and int(x) == 0 else (f"{int(x):,} km".replace(",", " ") if pd.notna(x) else "-")
    )

st.dataframe(
    table_display,
    use_container_width=True,
    hide_index=True,
    height=300
)

st.markdown("---")

# ============================================================================
# ANALYSES GRAPHIQUES - EXPLICATIONS
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>📈 Analyses Visuelles</h2>
    <p>Visualisations interactives des données</p>
</div>
""", unsafe_allow_html=True)

# Ligne 1: Analyses principales (sans titre supplémentaire)
col_left, col_middle, col_right = st.columns(3)

with col_left:
    if "modele" in filtered.columns and "score_risque" in filtered.columns:
        st.markdown("**🏷️ Score de Risque Moyen par Modèle**")
        st.caption("Plus la barre est haute, plus ce modèle nécessite attention")
        by_modele = filtered.groupby("modele", dropna=True)["score_risque"].mean().sort_values(ascending=False)
        
        fig = px.bar(
            x=by_modele.values,
            y=by_modele.index,
            orientation='h',
            labels={'x': 'Score Risque', 'y': 'Modèle'},
            color=by_modele.values,
            color_continuous_scale=['#4caf50', '#ff9800', '#f44336']
        )
        fig.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family='Poppins', size=12),
            xaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)'),
            yaxis=dict(showgrid=False)
        )
        fig.update_traces(marker=dict(line=dict(width=0)))
        st.plotly_chart(fig, use_container_width=True)
        st.info(f"💡 Modèle le plus à risque: **{by_modele.idxmax()}** ({by_modele.max():.1f}/100)")

with col_middle:
    if "statut" in filtered.columns:
        st.markdown("**⚠️ Répartition des Statuts Opérationnels**")
        st.caption("Distribution OK / ALERTE / CRITIQUE")
        statut_counts = filtered["statut"].value_counts().sort_index()
        
        colors = {'OK': '#4caf50', 'ALERTE': '#ff9800', 'CRITIQUE': '#f44336', 'SURVEILLANCE': '#ffc107'}
        fig = go.Figure(data=[go.Pie(
            labels=statut_counts.index,
            values=statut_counts.values,
            marker=dict(
                colors=[colors.get(s, '#999') for s in statut_counts.index],
                line=dict(color='white', width=3)
            ),
            hole=0.5,
            textinfo='label+percent',
            textfont=dict(size=13, family='Poppins', color='white'),
            hovertemplate='<b>%{label}</b><br>%{value} véhicules<br>%{percent}<extra></extra>'
        )])
        fig.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True,
            paper_bgcolor='white',
            font=dict(family='Poppins', size=12),
            legend=dict(orientation='h', yanchor='bottom', y=-0.15, xanchor='center', x=0.5)
        )
        st.plotly_chart(fig, use_container_width=True)
        pct_ok = (statut_counts.get("OK", 0) / len(filtered) * 100) if len(filtered) > 0 else 0
        st.info(f"✅ {pct_ok:.1f}% de véhicules en statut OK")

with col_right:
    if "panne_type_simple" in filtered.columns:
        st.markdown("**🔧 Types de Pannes Anticipées**")
        st.caption("Quelles défaillances l'IA prédit le plus")
        panne_counts = filtered["panne_type_simple"].value_counts().sort_index()
        
        fig = px.bar(
            x=panne_counts.index,
            y=panne_counts.values,
            labels={'x': 'Type de Panne', 'y': 'Nombre'},
            color=panne_counts.values,
            color_continuous_scale=['#ffc107', '#ff5722']
        )
        fig.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family='Poppins', size=12),
            xaxis=dict(showgrid=False, tickangle=-45),
            yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)')
        )
        fig.update_traces(marker=dict(line=dict(width=0)))
        st.plotly_chart(fig, use_container_width=True)
        if len(panne_counts) > 0 and panne_counts.iloc[0] > 0:
            st.info(f"⚠️ Type principal: **{panne_counts.idxmax()}** ({panne_counts.max()} cas)")

st.markdown("---")

# Ligne 2: Distribution et corrélations
col_dist, col_corr = st.columns(2)

with col_dist:
    if "prob_panne" in filtered.columns:
        st.markdown("**📈 Distribution des Niveaux de Risque**")
        
        prob_bins = pd.cut(
            filtered["prob_panne"], 
            bins=[0, 0.3, 0.5, 0.7, 1.0],
            labels=["🟢 Faible", "🟡 Moyenne", "🟠 Élevée", "🔴 Critique"]
        )
        prob_dist = prob_bins.value_counts().sort_index()
        
        colors_map = {'🟢 Faible': '#4caf50', '🟡 Moyenne': '#ffc107', '🟠 Élevée': '#ff9800', '🔴 Critique': '#f44336'}
        fig = px.bar(
            x=prob_dist.index,
            y=prob_dist.values,
            labels={'x': 'Niveau de Risque', 'y': 'Nombre de Véhicules'},
            color=prob_dist.index,
            color_discrete_map=colors_map
        )
        fig.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family='Poppins', size=12),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)')
        )
        fig.update_traces(marker=dict(line=dict(color='white', width=2)))
        st.plotly_chart(fig, use_container_width=True)
        
        total = len(filtered)
        # Compter uniquement les vraies pannes critiques (exclure OK)
        critical = len(filtered[(filtered["prob_panne"] >= 0.7) & (filtered["type_panne_predit"] != 0)])
        if total > 0:
            st.info(
                f"📊 {critical}/{total} véhicules ({critical/total*100:.1f}%) en zone critique "
                "(probabilite >= 70% et type != OK)"
            )

with col_corr:
    if all(col in filtered.columns for col in ["temp_moteur", "voltage_batterie", "prob_panne"]):
        st.markdown("**🔬 Corrélation Données OBD vs Risque**")
        st.caption("Comparaison véhicules à risque élevé vs faible")
        
        with st.expander("ℹ️ Interprétation de ce graphique ?"):
            st.markdown("""
            Ce graphique compare les données moyennes des capteurs entre :
            - **Véhicules à risque élevé** (prob ≥ 50%)
            - **Véhicules à risque faible** (prob < 50%)
            
            **Observations utiles :**
            - Température plus élevée chez véhicules à risque → problème moteur
            - Voltage plus bas → problème batterie/alternateur
            - Différences marquées → capteurs fiables pour prédiction
            
            *Note: Valeurs mises à l'échelle pour visibilité*
            """)
        
        high_risk = filtered[filtered["prob_panne"] >= 0.5]
        low_risk = filtered[filtered["prob_panne"] < 0.5]
        
        if not high_risk.empty and not low_risk.empty:
            comparison = pd.DataFrame({
                "Risque Élevé": [
                    high_risk["temp_moteur"].mean(),
                    high_risk["voltage_batterie"].mean() * 10,
                    high_risk["regime_moteur"].mean() / 100
                ],
                "Risque Faible": [
                    low_risk["temp_moteur"].mean(),
                    low_risk["voltage_batterie"].mean() * 10,
                    low_risk["regime_moteur"].mean() / 100
                ]
            }, index=["Temp.(°C)", "Volt.(x10)", "RPM(/100)"])
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Risque Élevé', 
                x=comparison.index, 
                y=comparison['Risque Élevé'], 
                marker=dict(color='#f44336', line=dict(color='white', width=2))
            ))
            fig.add_trace(go.Bar(
                name='Risque Faible', 
                x=comparison.index, 
                y=comparison['Risque Faible'], 
                marker=dict(color='#4caf50', line=dict(color='white', width=2))
            ))
            fig.update_layout(
                height=280,
                margin=dict(l=0, r=0, t=10, b=0),
                barmode='group',
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(family='Poppins', size=12),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)'),
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 Données insuffisantes pour analyse comparative")

st.markdown("---")

# ============================================================================
# ANALYSE PAR MODÈLE - TABLEAU RÉCAPITULATIF
# ============================================================================

if "modele" in filtered.columns and "prob_panne" in filtered.columns:
    st.markdown("## 🏷️ Analyse Détaillée par Modèle de Véhicule")
    
    with st.expander("ℹ️ À quoi sert ce tableau ?", expanded=False):
        st.markdown("""
        Ce tableau synthétise les performances de chaque modèle de véhicule.
        
        **Utilisez-le pour :**
        - Identifier les modèles nécessitant plus de maintenance
        - Comparer fiabilité entre différents types de véhicules
        - Planifier le renouvellement du parc (remplacer modèles problématiques)
        - Négocier avec fournisseurs (garanties, SAV) basé sur données réelles
        
        **Colonnes :**
        - **Nb Véhicules** : Combien d'unités de ce modèle
        - **Prob. Moy.** : Probabilité moyenne de panne pour ce modèle
        - **Prob. Max** : Pire cas dans ce modèle
        - **Score Risque** : Évaluation globale du modèle
        - **Km Moyen** : Usage moyen du modèle
        """)
    
    model_analysis = filtered.groupby("modele").agg({
        "vin": "count",
        "prob_panne": ["mean", "max"],
        "score_risque": "mean",
        "km_actuel": "mean"
    }).round(2)
    
    model_analysis.columns = ["Nb Véhicules", "Prob. Moy.", "Prob. Max", "Score Risque", "Km Moyen"]
    model_analysis = model_analysis.sort_values("Prob. Moy.", ascending=False)
    
    # Formatage
    model_analysis["Prob. Moy."] = model_analysis["Prob. Moy."].apply(lambda x: f"{x:.1%}")
    model_analysis["Prob. Max"] = model_analysis["Prob. Max"].apply(lambda x: f"{x:.1%}")
    model_analysis["Km Moyen"] = model_analysis["Km Moyen"].apply(lambda x: f"{int(x):,}".replace(",", " "))
    
    st.dataframe(model_analysis, use_container_width=True, height=min(400, len(model_analysis) * 50 + 50))
    
    # Recommandation
    worst_model = model_analysis.index[0]
    st.warning(f"⚠️ **Recommandation :** Le modèle **{worst_model}** présente le plus de risques. Renforcez la surveillance de ces véhicules.")

st.markdown("---")

# ============================================================================
# INFORMATIONS SYSTÈME ET AIDE
# ============================================================================

st.markdown("## ℹ️ Informations Système")

col_info1, col_info2, col_info3 = st.columns(3)

with col_info1:
    st.markdown("**📊 Source de Données**")
    st.caption(f"Fichier: `{gold_parquet_path}`")
    st.caption(f"Dernière analyse: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    st.caption(f"Total véhicules: {len(df)}")

with col_info2:
    st.markdown("**🤖 Modèle d'Intelligence Artificielle**")
    st.caption("Algorithme: RandomForest Classifier")
    st.caption("Entraînement: 200+ historiques de pannes")
    st.caption("Features: Temp, Pression, RPM, Voltage, Km")

with col_info3:
    st.markdown("**🛠️ Support & Documentation**")
    st.caption("Version: 2.0")
    st.caption("Propulsé par: PySpark + scikit-learn")
    st.caption("Dashboard: Streamlit")

st.markdown("---")

# ============================================================================
# SECTION INFORMATIVE - SOURCES DE DONNÉES
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>🧭 Architecture de la Plateforme</h2>
    <p>Comprendre les différentes couches de traitement des données</p>
</div>
""", unsafe_allow_html=True)

col_info1, col_info2, col_info3 = st.columns(3)

with col_info1:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        padding: 2rem;
        border-radius: 20px;
        box-shadow: 0 8px 25px rgba(33, 150, 243, 0.2);
        border-left: 5px solid #2196F3;
        min-height: 450px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    ">
        <div style="font-size: 2.5rem; margin-bottom: 1rem; text-align: center;">📥</div>
        <h3 style="color: #1565c0; font-weight: 700; text-align: center; margin-bottom: 1rem;">Données Brutes</h3>
        <p style="color: #0d47a1; text-align: center; margin-bottom: 1.5rem; font-style: italic;">Sources primaires non traitées</p>
        <ul style="list-style: none; padding: 0; color: #1976d2; flex-grow: 1;">
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">📡 Télémétrie OBD</li>
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">🔧 Historique maintenance</li>
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">🚗 Données flotte</li>
        </ul>
        <p style="color: #0d47a1; font-size: 0.85rem; margin-top: 1rem; text-align: center;"><strong>Collecte pure sans transformation</strong></p>
    </div>
    """, unsafe_allow_html=True)

with col_info2:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%);
        padding: 2rem;
        border-radius: 20px;
        box-shadow: 0 8px 25px rgba(76, 175, 80, 0.2);
        border-left: 5px solid #4caf50;
        min-height: 450px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    ">
        <div style="font-size: 2.5rem; margin-bottom: 1rem; text-align: center;">🧮</div>
        <h3 style="color: #2e7d32; font-weight: 700; text-align: center; margin-bottom: 1rem;">Calculs Métier</h3>
        <p style="color: #1b5e20; text-align: center; margin-bottom: 1.5rem; font-style: italic;">Règles déterministes appliquées</p>
        <ul style="list-style: none; padding: 0; color: #388e3c; flex-grow: 1;">
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">📊 Score de risque</li>
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">📏 Calcul échéances</li>
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">⚙️ Actions requises</li>
        </ul>
        <p style="color: #1b5e20; font-size: 0.85rem; margin-top: 1rem; text-align: center;"><strong>Logique métier sans machine learning</strong></p>
    </div>
    """, unsafe_allow_html=True)

with col_info3:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
        padding: 2rem;
        border-radius: 20px;
        box-shadow: 0 8px 25px rgba(255, 152, 0, 0.2);
        border-left: 5px solid #ff9800;
        min-height: 450px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    ">
        <div style="font-size: 2.5rem; margin-bottom: 1rem; text-align: center;">🤖</div>
        <h3 style="color: #e65100; font-weight: 700; text-align: center; margin-bottom: 1rem;">Intelligence Artificielle</h3>
        <p style="color: #bf360c; text-align: center; margin-bottom: 1.5rem; font-style: italic;">Prédictions par Machine Learning</p>
        <ul style="list-style: none; padding: 0; color: #ef6c00; flex-grow: 1;">
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">🎯 Type de panne prédit</li>
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">📈 Probabilité estimée</li>
            <li style="padding: 0.5rem; background: rgba(255,255,255,0.5); margin: 0.5rem 0; border-radius: 8px;">🚨 Niveau d'alerte</li>
        </ul>
        <p style="color: #bf360c; font-size: 0.85rem; margin-top: 1rem; text-align: center;"><strong>Modèle RandomForest 200+ patterns</strong></p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# Pied de page artistique
st.markdown(f"""
<div style="
    text-align: center; 
    padding: 3rem 2rem; 
    background: linear-gradient(135deg, {current_theme['primary']} 0%, {current_theme['secondary']} 100%);
    border-radius: 20px;
    margin-top: 3rem;
    box-shadow: 0 10px 40px {current_theme['primary']}44;
    animation: fadeIn 0.8s ease-out;
">
    <h3 style="color: white; font-size: 1.8rem; font-weight: 600; margin: 0 0 1rem 0;">🚗 AutoStream</h3>
    <p style="color: rgba(255,255,255,0.95); font-size: 1.1rem; margin: 0.5rem 0;">Plateforme de Maintenance Prédictive Intelligente</p>
    <p style="color: rgba(255,255,255,0.85); font-size: 0.95rem; margin: 1rem 0 0 0;">Transformez vos données en décisions stratégiques · Optimisez vos coûts · Maximisez la disponibilité</p>
    <p style="color: rgba(255,255,255,0.7); font-size: 0.85rem; margin: 1.5rem 0 0 0; font-weight: 300;">✨ Propulsé par Intelligence Artificielle & Big Data ✨</p>
</div>
""", unsafe_allow_html=True)
