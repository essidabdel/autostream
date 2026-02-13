# AutoStream

AutoStream est un projet pedagogique de maintenance predictive pour flotte automobile. Il combine un mini data lake local (bronze/silver/gold), un pipeline de preparation des donnees, un modele de classification, et un tableau de bord Streamlit pour l'analyse et le suivi des risques de panne.

L'objectif est de montrer un flux de bout en bout : generation de donnees, nettoyage, enrichissement, inference ML, puis visualisation.

## Architecture du projet

- Zone bronze : donnees brutes (JSON, CSV, SQLite) issues d'une simulation.
- Zone silver : donnees nettoyees et standardisees (CSV et Parquet partitionne).
- Zone gold : features metier, predictions ML, agregations temporelles, rapports de qualite.
- Applications Streamlit : plusieurs variantes d'interface pour l'exploration.

Le plan de stockage et les schemas attendus sont decrits dans data_catalog.md.

## Prerequis

- Python 3.10+ recommande
- pip
- Pour la couche Spark : Java + PySpark
- Sous Windows, les scripts Spark peuvent demander un environnement Hadoop/Winutils (ex. C:\hadoop)

## Installation

```bash
pip install -r requirements.txt
```

Si vous executez les scripts Spark :

```bash
pip install pyspark
```

## Demarrage rapide

1) Entrainer le modele (a faire une fois ou apres changement des donnees historiques)

```bash
python train_model.py
```

2) Lancer le pipeline complet (bronze -> silver -> gold)

```bash
python run_all.py
```

3) Lancer une interface Streamlit

```bash
streamlit run app_glass.py
```

Variantes d'interface :

```bash
streamlit run app_dark.py
streamlit run app_glass.py
```

## Scripts principaux

- train_model.py : entraine un RandomForest et cree model_pannes.pkl
- run_all.py : orchestre generator.py, pipeline_spark.py, pipeline_gold.py
- creation_data/generator.py : simule les sources (JSON/CSV/SQLite)
- creation_data/pipeline_spark.py : nettoie et structure la zone silver
- creation_data/pipeline_gold.py : jointures, indicateurs metier, predictions ML, agregations
- creation_data/ml_inference.py : applique le modele et calcule le statut de risque
- analytics.py : fonctions d'acces aux donnees et conventions de calcul

## Donnees et sorties

- data/bronze : sources brutes simulees
- data/silver : donnees nettoyees (CSV + Parquet)
- data/gold/parquet/run_date=YYYY-MM-DD/reporting_final.parquet : dataset final
- data/gold/aggregations : indicateurs journaliers/hebdo/mensuels
- data/quality : rapports de qualite silver et gold

## Notes d'utilisation

- Le tableau de bord lit la derniere date disponible dans data/gold/parquet.
- Les scripts Spark peuvent etre plus longs selon la machine.
- Si model_pannes.pkl est absent, relancer train_model.py.

## Auteur et cadre

Projet academique realise pour illustrer un pipeline data/ML complet, avec une attention particuliere a la qualite des donnees et a la lisibilite des indicateurs.

