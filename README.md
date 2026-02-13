# 🚗 AutoStream

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

**Projet pédagogique de maintenance prédictive pour flotte automobile.**

AutoStream combine un Data Lake local, un pipeline ETL Spark et du Machine Learning pour prédire les risques de pannes, le tout visualisé sur un tableau de bord interactif.

---

### 🌐 Démo en ligne

Accédez à l'application déployée ici :

([https://LIEN_DE_TON_APP_STREAMLIT](https://autostream-ds8wtypmwgmgxgdov8r3gq.streamlit.app/))

---

## 🏗️ Architecture Medallion

Le projet suit un flux de données structuré de bout en bout :

* 🥉 **Zone Bronze** : Données brutes simulées (JSON, CSV, SQLite).
* 🥈 **Zone Silver** : Données nettoyées et standardisées via **Spark** (Parquet partitionné).
* 🥇 **Zone Gold** : Features métier, prédictions ML, agrégations temporelles et rapports de qualité.
* 📊 **Frontend** : Application **Streamlit** pour l'analyse et le monitoring.

*Le plan de stockage détaillé est disponible dans `data_catalog.md`.*

## ⚙️ Prérequis

* **Python 3.10+**
* **Java** (Requis pour la couche Spark)
* *Windows uniquement* : Environnement Hadoop/Winutils configuré (ex. `C:\hadoop`).

## 📦 Installation

1. Cloner le projet et installer les dépendances :
```bash
pip install -r requirements.txt
Si vous comptez exécuter les scripts Spark localement :

```bash
pip install pyspark
🚀 Démarrage Rapide
1. Entraîner le modèle
(À faire lors de la première utilisation ou mise à jour de l'historique)

```bash
python train_model.py
2. Lancer le pipeline complet (ETL Bronze → Gold)

```bash
python run_all.py
3. Lancer le tableau de bord

```bash
streamlit run app_glass.py
Variante Dark Mode : streamlit run app_dark.py

````
📂 Structure du Projet
Plaintext
AutoStream/
├── analytics.py             # Fonctions d'accès aux données & KPI
├── train_model.py           # Entraînement du RandomForest (génère model_pannes.pkl)
├── run_all.py               # Orchestrateur global
├── app_glass.py             # Interface Streamlit principale
├── data/                    # Data Lake Local (Bronze/Silver/Gold/Quality)
└── creation_data/
    ├── generator.py         # Simulation des sources de données
    ├── pipeline_spark.py    # Nettoyage et structuration (Silver)
    ├── pipeline_gold.py     # Feature Engineering & Agrégations (Gold)
    └── ml_inference.py      # Application du modèle & Scoring
📝 Notes d'utilisation
Reporting : Le dashboard lit automatiquement la dernière date disponible dans data/gold/parquet.

Performance : L'exécution des scripts Spark peut varier selon la puissance de la machine.

Dépannage : Si le fichier model_pannes.pkl est manquant, relancez train_model.py.

🎓 Auteur et Cadre
Ce projet académique a été réalisé pour illustrer un pipeline Data/ML complet, avec une attention particulière portée à la qualité des données et à la lisibilité des indicateurs décisionnels.


