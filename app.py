import os
import pandas as pd
import streamlit as st

DATA_PATH = os.path.join("data", "gold", "reporting_final.csv")

st.set_page_config(page_title="AutoStream Gold", layout="wide")

st.title("AutoStream - Zone Gold")

if not os.path.exists(DATA_PATH):
    st.error("Fichier introuvable: data/gold/reporting_final.csv")
    st.stop()

df = pd.read_csv(DATA_PATH)

# Normalize columns
if "date_last_revis" in df.columns:
    df["date_last_revis"] = pd.to_datetime(df["date_last_revis"], errors="coerce")

st.sidebar.header("Filtres")

vin_values = sorted(df["vin"].dropna().unique()) if "vin" in df.columns else []
modele_values = sorted(df["modele"].dropna().unique()) if "modele" in df.columns else []
statut_values = sorted(df["statut"].dropna().unique()) if "statut" in df.columns else []

selected_vins = st.sidebar.multiselect("VIN", vin_values, default=vin_values)
selected_modeles = st.sidebar.multiselect("Modele", modele_values, default=modele_values)
selected_statuts = st.sidebar.multiselect("Statut", statut_values, default=statut_values)

filtered = df.copy()
if selected_vins:
    filtered = filtered[filtered["vin"].isin(selected_vins)]
if selected_modeles:
    filtered = filtered[filtered["modele"].isin(selected_modeles)]
if selected_statuts:
    filtered = filtered[filtered["statut"].isin(selected_statuts)]

st.subheader("KPIs")
col1, col2, col3, col4 = st.columns(4)

col1.metric("Nb vehicules", int(filtered["vin"].nunique()) if "vin" in filtered.columns else 0)
col2.metric("Nb lignes", int(len(filtered)))

avg_score = filtered["score_risque"].mean() if "score_risque" in filtered.columns else None
col3.metric("Score risque moyen", f"{avg_score:.2f}" if pd.notna(avg_score) else "N/A")

critical_count = 0
if "statut" in filtered.columns:
    critical_count = int((filtered["statut"] == "CRITIQUE").sum())
col4.metric("Critiques", critical_count)

st.subheader("Table des donnees")
st.dataframe(filtered, use_container_width=True)

st.subheader("Analyses")
left, right = st.columns(2)

with left:
    if "modele" in filtered.columns and "score_risque" in filtered.columns:
        by_modele = filtered.groupby("modele", dropna=True)["score_risque"].mean().sort_values(ascending=False)
        st.caption("Score risque moyen par modele")
        st.bar_chart(by_modele)

with right:
    if "statut" in filtered.columns:
        statut_counts = filtered["statut"].value_counts().sort_index()
        st.caption("Repartition des statuts")
        st.bar_chart(statut_counts)
