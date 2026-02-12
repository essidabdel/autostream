import os
import pandas as pd
import streamlit as st

from analytics import DATA_PATH, PANNE_DESCRIPTIONS, RECOMMANDATIONS, load_data

# ============================================================================
# CONFIGURATION DE LA PAGE
# ============================================================================

st.set_page_config(
    page_title="AutoStream - Maintenance Prédictive",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# EN-TÊTE DE L'APPLICATION
# ============================================================================

st.markdown("""
    <style>
    .main-header {
        text-align: center;
        padding: 1.5rem;
        background: linear-gradient(90deg, #1f77b4 0%, #2ca02c 100%);
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .main-header h1 {
        color: white;
        margin: 0;
        font-size: 2.5rem;
    }
    .main-header p {
        color: #e0e0e0;
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
    }
    .info-box {
        background-color: #e3f2fd;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #2196F3;
        margin: 1rem 0;
        color: #000000;
    }
    .info-box h3, .info-box h4, .info-box p, .info-box em, .info-box strong {
        color: #000000 !important;
    }
    .info-box ul, .info-box li {
        color: #000000 !important;
    }
    .warning-box {
        background-color: #fff3e0;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #ff9800;
        margin: 1rem 0;
        color: #000000;
    }
    .warning-box h3, .warning-box h4, .warning-box p, .warning-box em, .warning-box strong {
        color: #000000 !important;
    }
    .warning-box ul, .warning-box li {
        color: #000000 !important;
    }
    .success-box {
        background-color: #e8f5e9;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #4caf50;
        margin: 1rem 0;
        color: #000000;
    }
    .success-box h3, .success-box h4, .success-box p, .success-box em, .success-box strong {
        color: #000000 !important;
    }
    .success-box ul, .success-box li {
        color: #000000 !important;
    }
    .danger-box {
        background-color: #ffebee;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #f44336;
        margin: 1rem 0;
        color: #000000;
    }
    .danger-box ul, .danger-box li {
        color: #000000 !important;
    }
    .danger-box h3, .danger-box h4, .danger-box p, .danger-box em, .danger-box strong {
        color: #000000 !important;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
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

if not os.path.exists(DATA_PATH):
    st.error(f"❌ Fichier de données introuvable: {DATA_PATH}")
    st.info("💡 **Comment générer les données ?**")
    st.code("python creation_data/pipeline_gold.py", language="bash")
    st.stop()

# Chargement des données
df = load_data()

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
# RÉSUMÉ EXÉCUTIF - VUE D'ENSEMBLE RAPIDE
# ============================================================================

st.markdown("## 🎯 Résumé Exécutif - Vue d'ensemble de votre flotte")

with st.expander("ℹ️ Que signifie cette section ?", expanded=False):
    st.markdown("""
    Cette section vous donne **en un coup d'œil** les informations les plus critiques :
    - Le véhicule le plus à risque actuellement
    - La tendance globale des pannes dans votre flotte
    - Un score de santé général (comme une note sur 100)
    """)

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
                <div class="{box_class}">
                    <h3>{icon} VÉHICULE LE PLUS CRITIQUE</h3>
                    <p><strong>VIN :</strong> {top_critical['vin']}</p>
                    <p><strong>Type :</strong> {top_critical.get('panne_type_simple', 'N/A')}</p>
                    <p><strong>Probabilité :</strong> {top_critical['prob_panne']:.0%}</p>
                    <p><strong>Statut :</strong> {top_critical['statut']}</p>
                    <p><em>⚠️ Action immédiate requise !</em></p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class="success-box">
                    <h3>✅ STATUT EXCELLENT</h3>
                    <p>Aucune panne détectée par le modèle ML</p>
                    <p><em>Continuez la maintenance préventive</em></p>
                </div>
            """, unsafe_allow_html=True)
    
    with col_exec2:
        panne_counts = df[df["type_panne_predit"] != 0].groupby("panne_type_simple").size()
        if not panne_counts.empty:
            most_common = panne_counts.idxmax()
            count = panne_counts.max()
            st.markdown(f"""
                <div class="warning-box">
                    <h3>📈 TENDANCE PRINCIPALE</h3>
                    <p><strong>{count} véhicules</strong> risquent une panne</p>
                    <p><strong>Type :</strong> {most_common}</p>
                    <p><em>💡 Prévoir stock de pièces</em></p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class="info-box">
                    <h3>📉 TENDANCE</h3>
                    <p>Flotte en bon état général</p>
                    <p><em>Maintenance préventive efficace</em></p>
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
        
        st.markdown(f"""
            <div class="{box_class}">
                <h3>{icon} SANTÉ GLOBALE FLOTTE</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{health_percentage:.1f}%</p>
                <p><strong>{status}</strong></p>
                <p><em>{nb_ok} véhicules OK sur {nb_total}</em></p>
                <p><em>🔴 {nb_critiques} critiques | 🟠 {nb_alertes} alertes</em></p>
            </div>
        """, unsafe_allow_html=True)

st.markdown("---")

# ============================================================================
# SIDEBAR - FILTRES AVEC EXPLICATIONS
# ============================================================================

st.sidebar.markdown("# 🔍 Panneau de Filtres")
st.sidebar.markdown("*Affinez votre analyse en sélectionnant des critères*")
st.sidebar.markdown("---")

# Extraction des valeurs uniques
vin_values = sorted(df["vin"].dropna().unique()) if "vin" in df.columns else []
modele_values = sorted(df["modele"].dropna().unique()) if "modele" in df.columns else []
statut_values = sorted(df["statut"].dropna().unique()) if "statut" in df.columns else []
panne_values = sorted(df["panne_type_simple"].dropna().unique()) if "panne_type_simple" in df.columns else []

st.sidebar.subheader("🚗 Filtres par identificateur")
selected_vins = st.sidebar.multiselect(
    "VIN (Numéro d'identification)", 
    vin_values, 
    default=vin_values,
    help="Sélectionnez un ou plusieurs véhicules spécifiques"
)

selected_modeles = st.sidebar.multiselect(
    "Modèle de véhicule", 
    modele_values, 
    default=modele_values,
    help="Filtrer par type/modèle de véhicule"
)

st.sidebar.markdown("---")
st.sidebar.subheader("⚠️ Filtres par statut")
selected_statuts = st.sidebar.multiselect(
    "Statut opérationnel", 
    statut_values, 
    default=statut_values,
    help="OK, ALERTE ou CRITIQUE selon les seuils définis"
)

if panne_values:
    selected_pannes = st.sidebar.multiselect(
        "Type de panne prédit", 
        panne_values, 
        default=panne_values,
        help="Type de défaillance anticipée par l'IA"
    )
else:
    selected_pannes = None

st.sidebar.markdown("---")
st.sidebar.subheader("🎯 Seuil de Risque")
st.sidebar.markdown("*Ajustez pour filtrer par niveau de probabilité*")
prob_min = st.sidebar.slider(
    "Probabilité minimale de panne", 
    0.0, 1.0, 0.0, 0.05,
    help="Afficher uniquement les véhicules au-dessus de ce seuil"
)
st.sidebar.caption(f"🔍 Affiche les véhicules avec ≥ {prob_min:.0%} de risque")

st.sidebar.markdown("---")
st.sidebar.subheader("👁️ Vues Rapides")
vue_rapide = st.sidebar.radio(
    "Mode d'affichage",
    ["📊 Tous les véhicules", "🔴 Uniquement urgents (≥70%)", "🟡 En surveillance (≥30%)"],
    index=0,
    help="Sélectionnez une vue prédéfinie pour accès rapide"
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

st.sidebar.markdown("---")
st.sidebar.info(f"📌 **{len(filtered)}** véhicules affichés sur **{len(df)}** total")

# ============================================================================
# INDICATEURS CLÉS (KPIs) EXPLIQUÉS
# ============================================================================

st.markdown("## 📊 Indicateurs Clés de Performance (KPIs)")

with st.expander("ℹ️ Comment interpréter ces indicateurs ?", expanded=False):
    st.markdown("""
    Ces 5 indicateurs résument l'état de votre flotte :
    
    - **🚗 Véhicules** : Nombre de véhicules dans la sélection actuelle
    - **⚠️ Score Risque Moyen** : Score métier basé sur température et âge (informatif uniquement)
    - **🔴 Critiques** : Véhicules avec panne prédite ET probabilité ≥ 70% → **Action immédiate**
    - **🚨 Interventions** : Nombre TOTAL de véhicules nécessitant une intervention (panne prédite par le ML)
    - **📍 Km Moyen** : Kilométrage moyen de la flotte (indicateur d'usure)
    
    **Note :** Le statut (OK/ALERTE/CRITIQUE) est désormais basé sur la prédiction ML, pas sur le score risque métier.
    """)

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    nb_vehicules = int(filtered["vin"].nunique()) if "vin" in filtered.columns else 0
    total_vehicules = int(df["vin"].nunique())
    st.metric(
        label="🚗 Véhicules",
        value=nb_vehicules,
        help="Nombre de véhicules dans la sélection actuelle"
    )

with kpi2:
    avg_score = filtered["score_risque"].mean() if "score_risque" in filtered.columns else 0
    st.metric(
        label="⚠️ Score Risque (moyen)",
        value=f"{avg_score:.1f}",
        help="Indice de risque moyen calcule a partir des facteurs: km, age, maintenance"
    )

with kpi3:
    critical_count = int((filtered["statut"] == "CRITIQUE").sum()) if "statut" in filtered.columns else 0
    st.metric(
        label="🔴 Critiques",
        value=critical_count,
        help="Véhicules avec panne détectée et probabilité ≥ 70%"
    )

with kpi4:
    if "type_panne_predit" in filtered.columns:
        # Compter TOUS les véhicules avec panne prédite
        interventions_count = int((filtered["type_panne_predit"] != 0).sum())
        st.metric(
            label="🚨 Interventions",
            value=interventions_count,
            help="Nombre de véhicules nécessitant intervention (panne prédite par le ML)"
        )
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
    else:
        st.metric("📍 Km Moyen", "N/A")

st.markdown("---")

# ============================================================================
# SECTION MAINTENANCE PRIORITAIRE - EXPLICATIONS DÉTAILLÉES
# ============================================================================

st.markdown("## 🚨 Liste de Maintenance Prioritaire")

st.markdown("""
<div class="info-box">
    <h4>📋 À quoi sert cette section ?</h4>
    <p>Cette liste affiche <strong>TOUS les véhicules nécessitant attention</strong> :</p>
    <ul>
        <li>🔴 <strong>CRITIQUES</strong> : Probabilité ≥ 70% de panne détectée</li>
        <li>🟠 <strong>ALERTES</strong> : Probabilité 40-70% de panne détectée</li>
        <li>🟡 <strong>SURVEILLANCE</strong> : Probabilité &lt; 40% mais panne détectée</li>
    </ul>
    <p><strong>Note importante :</strong> Seuls les véhicules avec une panne détectée sont affichés (les véhicules "OK" sont exclus).</p>
    <p><strong>Utilisez-la pour :</strong></p>
    <ul>
        <li>📅 Planifier les rendez-vous atelier en priorité</li>
        <li>📦 Commander les pièces nécessaires à l'avance</li>
        <li>👥 Affecter les techniciens sur les cas urgents</li>
        <li>📞 Contacter les chauffeurs pour immobilisation préventive</li>
    </ul>
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
        
        st.markdown(f"### 🔴 {len(urgent)} véhicule(s) nécessitant attention")
        st.caption(f"Répartition : {nb_critiques} critiques 🔴 | {nb_alertes} alertes 🟠 | {nb_surveillance} surveillance 🟡")
        
        urgent_sorted = urgent.sort_values(["statut", "prob_panne"], ascending=[True, False])
        
        # Légende du tableau
        with st.expander("📖 Légende des colonnes du tableau", expanded=False):
            st.markdown("""
            - **🚦 Alerte** : Niveau d'urgence visuel (🔴=Urgent, 🟠=Attention, 🟡=Surveillance, 🟢=Normal)
            - **VIN** : Numéro d'identification unique du véhicule
            - **Modèle** : Type de véhicule (Transit, Sprinter, Jumper, etc.)
            - **🔧 Type** : Icône du type de panne prédit
            - **Panne** : Description du type de défaillance anticipée
            - **Probabilité** : Confiance de l'IA dans sa prédiction (plus élevé = plus certain)
            - **Échéance (km)** : Estimation en tranches de 250 km avant panne probable
            - **Km** : Kilométrage total actuel du véhicule
            - **Km depuis révision** : Distance parcourue depuis dernier entretien
            - **Action Requise** : Recommandation technique spécifique
            """)
        
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
            height=min(400, len(priority_table) * 50 + 50)
        )
        
        # Détail des actions recommandées
        st.markdown("### 🔧 Actions Recommandées Détaillées")
        
        for idx, row in urgent_sorted.iterrows():
            panne_type = row.get("panne_type_simple", "Inconnu")
            vin = row.get("vin", "N/A")
            prob = row.get("prob_panne", 0)
            
            with st.expander(f"{row.get('alerte_emoji', '🔴')} {vin} - {panne_type} ({prob:.0%})"):
                st.markdown(f"**📝 Description du problème :**")
                st.info(PANNE_DESCRIPTIONS.get(panne_type, "Description non disponible"))
                
                st.markdown(f"**🛠️ Liste des actions à effectuer :**")
                actions = RECOMMANDATIONS.get(panne_type, ["Diagnostic complet requis"])
                for action in actions:
                    st.markdown(f"- {action}")
                
                # Données OBD
                st.markdown("**📊 Données capteurs (OBD) :**")
                col_obd1, col_obd2 = st.columns(2)
                with col_obd1:
                    if "temp_moteur" in row:
                        temp_status = "🔥 ÉLEVÉE" if row["temp_moteur"] > 100 else "✅ Normale"
                        st.metric("🌡️ Température moteur", f"{row['temp_moteur']:.1f}°C")
                        st.caption(f"Statut: {temp_status}")
                    if "pression_huile" in row:
                        press_status = "⚠️ BASSE" if row["pression_huile"] < 2.5 else "✅ Normale"
                        st.metric("🛢️ Pression huile", f"{row['pression_huile']:.2f} bar")
                        st.caption(f"Statut: {press_status}")
                with col_obd2:
                    if "voltage_batterie" in row:
                        volt_status = "🔋 FAIBLE" if row["voltage_batterie"] < 12.0 else "✅ Normale"
                        st.metric("⚡ Voltage batterie", f"{row['voltage_batterie']:.2f}V")
                        st.caption(f"Statut: {volt_status}")
                    if "regime_moteur" in row:
                        st.metric("⚙️ Régime moteur", f"{int(row['regime_moteur']):,}".replace(",", " ") + " RPM")
        
        # Bouton export
        st.markdown("---")
        csv = urgent_sorted.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Télécharger le rapport d'intervention (CSV)",
            data=csv,
            file_name=f"maintenance_urgente_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            help="Exporter la liste pour impression ou partage avec l'équipe technique"
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

st.markdown("## 📋 Vue Détaillée - Tous les Véhicules")

with st.expander("ℹ️ Comment utiliser ce tableau ?", expanded=False):
    st.markdown("""
    Ce tableau présente **tous les véhicules de votre sélection** avec leurs données complètes.
    
    **Colonnes principales :**
    - **🚦 Alerte** : Code couleur d'urgence
    - **Panne & Probabilité** : Ce que l'IA prédit
    - **Échéance (km)** : Distance estimée avant la panne probable (tranches de 250 km)
    - **Données OBD** : Températures, pressions, voltages en temps réel
    - **Score Risque** : Évaluation globale du véhicule
    
    **💡 Astuce :** Cliquez sur les en-têtes de colonnes pour trier les données
    """)

# Préparation des données
display_data = filtered.copy()

core_cols = ["alerte_emoji", "vin", "modele", "statut", "panne_emoji", "panne_type_simple", "prob_panne"]
if "km_estime" in display_data.columns:
    core_cols.append("km_estime")
    
score_cols = ["score_risque"]
obd_cols = ["temp_moteur", "pression_huile", "regime_moteur", "voltage_batterie", "km_actuel", "km_depuis_revis"]

all_display_cols = core_cols + score_cols + obd_cols
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
    height=400
)

st.markdown("---")

# ============================================================================
# ANALYSES GRAPHIQUES - EXPLICATIONS
# ============================================================================

st.markdown("## 📈 Analyses Visuelles et Statistiques")

st.markdown("""
<div class="info-box">
    <h4>📊 Objectif des graphiques ci-dessous</h4>
    <p>Ces visualisations vous aident à identifier rapidement :</p>
    <ul>
        <li><strong>Les modèles problématiques</strong> qui nécessitent plus d'attention</li>
        <li><strong>La répartition des statuts</strong> dans votre flotte</li>
        <li><strong>Les types de pannes les plus fréquents</strong> pour anticiper les besoins</li>
        <li><strong>La distribution des risques</strong> pour prioriser les actions</li>
    </ul>
</div>
""", unsafe_allow_html=True)

# Ligne 1: Analyses principales
st.markdown("### 📊 Analyses par Catégorie")

col_left, col_middle, col_right = st.columns(3)

with col_left:
    if "modele" in filtered.columns and "score_risque" in filtered.columns:
        st.markdown("**🏷️ Score de Risque Moyen par Modèle**")
        st.caption("Plus la barre est haute, plus ce modèle nécessite attention")
        by_modele = filtered.groupby("modele", dropna=True)["score_risque"].mean().sort_values(ascending=False)
        st.bar_chart(by_modele, height=300)
        st.info(f"💡 Modèle le plus à risque: **{by_modele.idxmax()}** ({by_modele.max():.1f}/100)")

with col_middle:
    if "statut" in filtered.columns:
        st.markdown("**⚠️ Répartition des Statuts Opérationnels**")
        st.caption("Distribution OK / ALERTE / CRITIQUE")
        statut_counts = filtered["statut"].value_counts().sort_index()
        st.bar_chart(statut_counts, height=300)
        pct_ok = (statut_counts.get("OK", 0) / len(filtered) * 100) if len(filtered) > 0 else 0
        st.info(f"✅ {pct_ok:.1f}% de véhicules en statut OK")

with col_right:
    if "panne_type_simple" in filtered.columns:
        st.markdown("**🔧 Types de Pannes Anticipées**")
        st.caption("Quelles défaillances l'IA prédit le plus")
        panne_counts = filtered["panne_type_simple"].value_counts().sort_index()
        st.bar_chart(panne_counts, height=300)
        if len(panne_counts) > 0 and panne_counts.iloc[0] > 0:
            st.info(f"⚠️ Type principal: **{panne_counts.idxmax()}** ({panne_counts.max()} cas)")

st.markdown("---")

# Ligne 2: Distribution et corrélations
st.markdown("### 📊 Analyses Avancées")

col_dist, col_corr = st.columns(2)

with col_dist:
    if "prob_panne" in filtered.columns:
        st.markdown("**📈 Distribution des Niveaux de Risque**")
        st.caption("Combien de véhicules dans chaque catégorie de risque")
        
        with st.expander("ℹ️ Comment lire ce graphique ?"):
            st.markdown("""
            Ce graphique classe vos véhicules en 4 catégories :
            - **🟢 Faible** : Risque minimal, maintenance standard
            - **🟡 Moyenne** : À surveiller de près
            - **🟠 Élevée** : Planifier intervention prochaine
            - **🔴 Critique** : Action immédiate nécessaire
            
            **Objectif :** La majorité devrait être en vert/jaune, peu en orange/rouge
            """)
        
        prob_bins = pd.cut(
            filtered["prob_panne"], 
            bins=[0, 0.3, 0.5, 0.7, 1.0],
            labels=["🟢 Faible", "🟡 Moyenne", "🟠 Élevée", "🔴 Critique"]
        )
        prob_dist = prob_bins.value_counts().sort_index()
        
        st.bar_chart(prob_dist, height=300)
        
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
            
            st.bar_chart(comparison, height=300)
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
    st.caption(f"Fichier: `{DATA_PATH}`")
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

# Pied de page
st.markdown("""
<div style="text-align: center; padding: 2rem; color: #666;">
    <p><strong>AutoStream - Maintenance Prédictive Intelligente</strong></p>
    <p>Transformez vos données véhicules en décisions stratégiques · Économisez sur les coûts ·  Maximisez la disponibilité</p>
    <p><em>Propulsé par Intelligence Artificielle et Big Data</em></p>
</div>
""", unsafe_allow_html=True)
