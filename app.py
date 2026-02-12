import os
import pandas as pd
import streamlit as st

DATA_PATH = os.path.join("data", "gold", "reporting_final.csv")

# Mapping des codes de pannes
PANNE_LABELS = {
    0: "OK",
    1: "Batterie",
    2: "Moteur",
    3: "Freins",
    4: "Turbo"
}

# Fonction pour determiner la couleur selon la probabilite
def get_alert_icon(prob):
    if prob >= 0.7:
        return "URGENT"
    elif prob >= 0.5:
        return "ATTENTION"
    elif prob >= 0.3:
        return "SURVEILLANCE"
    else:
        return "NORMAL"

st.set_page_config(page_title="AutoStream Gold - Maintenance Predictive", layout="wide")

st.title("AutoStream - Maintenance Predictive")

if not os.path.exists(DATA_PATH):
    st.error("Fichier introuvable: data/gold/reporting_final.csv")
    st.stop()

df = pd.read_csv(DATA_PATH)

# Normalize columns
if "date_last_revis" in df.columns:
    df["date_last_revis"] = pd.to_datetime(df["date_last_revis"], errors="coerce")

# Ajouter les colonnes enrichies pour l'affichage
if "type_panne_predit" in df.columns:
    df["type_panne_label"] = df["type_panne_predit"].map(PANNE_LABELS)
if "prob_panne" in df.columns:
    df["alerte"] = df["prob_panne"].apply(get_alert_icon)

st.sidebar.header("Filtres")

vin_values = sorted(df["vin"].dropna().unique()) if "vin" in df.columns else []
modele_values = sorted(df["modele"].dropna().unique()) if "modele" in df.columns else []
statut_values = sorted(df["statut"].dropna().unique()) if "statut" in df.columns else []
panne_values = sorted(df["type_panne_label"].dropna().unique()) if "type_panne_label" in df.columns else []

selected_vins = st.sidebar.multiselect("VIN", vin_values, default=vin_values)
selected_modeles = st.sidebar.multiselect("Modele", modele_values, default=modele_values)
selected_statuts = st.sidebar.multiselect("Statut", statut_values, default=statut_values)

if panne_values:
    selected_pannes = st.sidebar.multiselect("Type de panne predit", panne_values, default=panne_values)
else:
    selected_pannes = None

# Filtre par seuil de probabilite
prob_min = 0.0
if "prob_panne" in df.columns:
    st.sidebar.subheader("Seuil de probabilite")
    prob_min = st.sidebar.slider("Probabilite minimale", 0.0, 1.0, 0.0, 0.05)

filtered = df.copy()
if selected_vins:
    filtered = filtered[filtered["vin"].isin(selected_vins)]
if selected_modeles:
    filtered = filtered[filtered["modele"].isin(selected_modeles)]
if selected_statuts:
    filtered = filtered[filtered["statut"].isin(selected_statuts)]
if selected_pannes and "type_panne_label" in filtered.columns:
    filtered = filtered[filtered["type_panne_label"].isin(selected_pannes)]
if "prob_panne" in filtered.columns:
    filtered = filtered[filtered["prob_panne"] >= prob_min]

st.subheader("KPIs")
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Nb vehicules", int(filtered["vin"].nunique()) if "vin" in filtered.columns else 0)
col2.metric("Nb lignes", int(len(filtered)))

avg_score = filtered["score_risque"].mean() if "score_risque" in filtered.columns else None
col3.metric("Score risque moyen", f"{avg_score:.2f}" if pd.notna(avg_score) else "N/A")

critical_count = 0
if "statut" in filtered.columns:
    critical_count = int((filtered["statut"] == "CRITIQUE").sum())
col4.metric("Critiques", critical_count)

# KPI de pannes urgentes
if "prob_panne" in filtered.columns and "type_panne_predit" in filtered.columns:
    urgent_count = int((filtered["prob_panne"] >= 0.7).sum())
    col5.metric("Pannes urgentes", urgent_count)
else:
    col5.metric("Pannes urgentes", "N/A")

# Section maintenance prioritaire
if "prob_panne" in filtered.columns and "type_panne_label" in filtered.columns:
    st.subheader("Maintenance Prioritaire (Probabilite >= 70%)")
    urgent = filtered[filtered["prob_panne"] >= 0.7].copy()
    if not urgent.empty:
        urgent_sorted = urgent.sort_values("prob_panne", ascending=False)
        
        # Colonnes a afficher pour la maintenance prioritaire
        priority_cols = ["alerte", "vin", "modele", "type_panne_label", "prob_panne", "km_actuel", "km_depuis_revis"]
        priority_display = urgent_sorted[[col for col in priority_cols if col in urgent_sorted.columns]]
        
        if "prob_panne" in priority_display.columns:
            priority_display["prob_panne"] = priority_display["prob_panne"].apply(lambda x: f"{x:.1%}")
        
        st.dataframe(priority_display, use_container_width=True, hide_index=True)
    else:
        st.success("Aucune maintenance urgente detectee!")
    
    st.divider()

st.subheader("Table des donnees")

# Selectionner les colonnes a afficher
display_cols = ["vin", "modele", "statut", "alerte", "type_panne_label", "prob_panne", 
                "score_risque", "km_actuel", "km_depuis_revis", "temp_moteur", 
                "pression_huile", "regime_moteur", "voltage_batterie"]
display_df = filtered[[col for col in display_cols if col in filtered.columns]].copy()

# Formatter la probabilite en pourcentage
if "prob_panne" in display_df.columns:
    display_df["prob_panne"] = display_df["prob_panne"].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")

st.dataframe(display_df, use_container_width=True, hide_index=True)

st.subheader("Analyses")

col_left, col_middle, col_right = st.columns(3)

with col_left:
    if "modele" in filtered.columns and "score_risque" in filtered.columns:
        by_modele = filtered.groupby("modele", dropna=True)["score_risque"].mean().sort_values(ascending=False)
        st.caption("Score risque moyen par modele")
        st.bar_chart(by_modele)

with col_middle:
    if "statut" in filtered.columns:
        statut_counts = filtered["statut"].value_counts().sort_index()
        st.caption("Repartition des statuts")
        st.bar_chart(statut_counts)

with col_right:
    if "type_panne_label" in filtered.columns:
        panne_counts = filtered["type_panne_label"].value_counts().sort_index()
        st.caption("Types de pannes predites")
        st.bar_chart(panne_counts)

# Graphique de distribution des probabilites
if "prob_panne" in filtered.columns:
    st.subheader("Distribution des probabilites de panne")
    
    # Creer des bins pour l'histogramme
    prob_bins = pd.cut(filtered["prob_panne"], bins=[0, 0.3, 0.5, 0.7, 1.0], 
                       labels=["Faible (0-30%)", "Moyenne (30-50%)", "Elevee (50-70%)", "Critique (70-100%)"])
    prob_dist = prob_bins.value_counts().sort_index()
    
    st.bar_chart(prob_dist)
