import os
import shutil
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Config Spark
spark = SparkSession.builder \
    .appName("AutoStreamSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("🚀 Spark est prêt pour le traitement...")

RUN_DATE = datetime.now().strftime("%Y-%m-%d")

REQUIRED_COLUMNS = [
    "vin",
    "timestamp",
    "temp_moteur",
    "vitesse",
    "km_actuel",
    "pression_huile",
    "regime_moteur",
    "voltage_batterie",
    "temps_0_100",
    "conso_essence",
    "pression_injection"
]

EXPECTED_SCHEMA = {
    "vin": "string",
    "timestamp": "string",
    "temp_moteur": "double",
    "vitesse": "bigint",
    "km_actuel": "bigint",
    "pression_huile": "double",
    "regime_moteur": "bigint",
    "voltage_batterie": "double",
    "temps_0_100": "double",
    "conso_essence": "double",
    "pression_injection": "double"
}


def write_quality_report(df, output_path, required_columns, expected_schema):
    missing_cols = [col for col in required_columns if col not in df.columns]
    null_counts = df.select(
        [F.sum(F.col(col).isNull().cast("int")).alias(col) for col in df.columns]
    ).toPandas()

    total_rows = df.count()
    null_pct = {
        col: (float(null_counts.iloc[0][col]) / total_rows * 100) if total_rows else 0.0
        for col in null_counts.columns
    }

    actual_schema = dict(df.dtypes)
    schema_issues = []
    for col, expected_type in expected_schema.items():
        actual_type = actual_schema.get(col)
        if actual_type is None:
            schema_issues.append(f"missing:{col}")
        elif actual_type != expected_type:
            schema_issues.append(f"{col}:{actual_type}!=expected:{expected_type}")

    report = {
        "run_date": RUN_DATE,
        "rows": [total_rows],
        "missing_columns": [", ".join(missing_cols)],
        "schema_issues": [", ".join(schema_issues)]
    }
    report_df = pd.DataFrame(report)
    for col in null_counts.columns:
        report_df[f"nulls_{col}"] = null_counts.iloc[0][col]
        report_df[f"null_pct_{col}"] = round(null_pct[col], 2)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    report_df.to_csv(output_path, index=False)


def write_parquet_partitioned(df_pandas, base_path, partition_col):
    os.makedirs(base_path, exist_ok=True)
    if partition_col not in df_pandas.columns:
        df_pandas.to_parquet(os.path.join(base_path, "data.parquet"), index=False)
        return

    for value, group in df_pandas.groupby(partition_col):
        if pd.isna(value):
            part_value = "unknown"
        else:
            part_value = str(value)
        part_dir = os.path.join(base_path, f"{partition_col}={part_value}")
        os.makedirs(part_dir, exist_ok=True)
        group.drop(columns=[partition_col]).to_parquet(os.path.join(part_dir, "data.parquet"), index=False)

def run_silver_layer():
    # Chemins
    bronze_path = os.path.join(os.getcwd(), "data", "bronze", "json")
    silver_path = os.path.join(os.getcwd(), "data", "silver", "telemetry")
    silver_parquet_path = os.path.join(os.getcwd(), "data", "silver", "telemetry_parquet", f"run_date={RUN_DATE}")
    quality_path = os.path.join(os.getcwd(), "data", "quality", f"silver_quality_{RUN_DATE}.csv")

    print("--- 📥 PHASE 1 : Ingestion Spark ---")
    try:
        df_spark = spark.read.json(f"{bronze_path}/*.json")
        print(f"✅ Données lues par Spark : {df_spark.count()} lignes")
        
        # --- Nettoyage Spark ---
        df_clean = (
            df_spark.dropna(subset=["vin"])
            .fillna({"temp_moteur": 90.0, "vitesse": 0})
            .withColumn("event_ts", F.to_timestamp("timestamp"))
            .withColumn("event_date", F.to_date("event_ts"))
        )

        write_quality_report(df_clean, quality_path, REQUIRED_COLUMNS, EXPECTED_SCHEMA)
        
        print("--- 💾 PHASE 3 : Sauvegarde (Bypass Hadoop) ---")
        
        # Transformation en Pandas (pour compatibilite avec la suite)
        df_pandas = df_clean.toPandas()

        # Sauvegarde Parquet partitionnee par date (Data Lake local)
        write_parquet_partitioned(df_pandas, silver_parquet_path, "event_date")
        
        # Création du dossier Silver
        if os.path.exists(silver_path):
            shutil.rmtree(silver_path)
        os.makedirs(silver_path, exist_ok=True)
        
        # Sauvegarde en CSV
        output_file = os.path.join(silver_path, "telemetry_clean.csv")
        df_pandas.to_csv(output_file, index=False)
        
        print(f"✨ ZONE SILVER TERMINÉE !")
        print(f"📁 Fichier créé ici : {output_file}")
        print(f"📁 Parquet partitionne ici : {silver_parquet_path}")
        print(f"📁 Rapport qualite ici : {quality_path}")
        print(df_pandas.head())

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    run_silver_layer()
    spark.stop()