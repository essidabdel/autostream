import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import pandas as pd

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from ml_inference import add_ml_predictions

# Config Spark 
BASE_PATH = os.path.abspath(os.getcwd()).replace("\\", "/")
FINAL_PATH = f"file:///{BASE_PATH}"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("hadoop.home.dir", os.environ["HADOOP_HOME"])
if os.path.join(os.environ["HADOOP_HOME"], "bin") not in os.environ.get("PATH", ""):
    os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

spark = SparkSession.builder \
    .appName("AutoStreamGold") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.io.nativeio.enabled", "false") \
    .config("spark.hadoop.fs.permissions.enabled", "false") \
    .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
    .config("spark.executor.extraLibraryPath", r"C:\hadoop\bin") \
    .config("spark.hadoop.hadoop.home.dir", os.environ["HADOOP_HOME"]) \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("io.nativeio.enabled", "false")
spark.sparkContext._jsc.hadoopConfiguration().set("io.native.lib.available", "false")
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.home.dir", os.environ["HADOOP_HOME"])

spark.sparkContext.setLogLevel("ERROR")

RUN_DATE = datetime.now().strftime("%Y-%m-%d")

GOLD_OUTPUT_DIR = os.path.join("data", "gold")
GOLD_PARQUET_DIR = os.path.join(GOLD_OUTPUT_DIR, "parquet", f"run_date={RUN_DATE}")
GOLD_AGG_DIR = os.path.join(GOLD_OUTPUT_DIR, "aggregations")
QUALITY_DIR = os.path.join("data", "quality")

REQUIRED_GOLD_COLUMNS = [
    "vin",
    "modele",
    "temp_moyenne",
    "km_actuel",
    "pression_huile",
    "regime_moteur",
    "voltage_batterie",
    "km_depuis_revis",
    "score_risque",
    "statut",
    "type_panne_predit",
    "prob_panne"
]

EXPECTED_GOLD_SCHEMA = {
    "vin": "object",
    "modele": "object",
    "temp_moyenne": "float64",
    "km_actuel": "float64",
    "pression_huile": "float64",
    "regime_moteur": "float64",
    "voltage_batterie": "float64",
    "km_depuis_revis": "float64",
    "score_risque": "float64",
    "statut": "object",
    "type_panne_predit": "int64",
    "prob_panne": "float64"
}


def write_quality_report(df, output_path, required_columns, expected_schema):
    missing_cols = [col for col in required_columns if col not in df.columns]
    null_counts = df.isna().sum().to_dict()
    total_rows = len(df)
    null_pct = {col: (count / total_rows * 100) if total_rows else 0.0 for col, count in null_counts.items()}

    schema_issues = []
    for col, expected_type in expected_schema.items():
        actual_type = str(df[col].dtype) if col in df.columns else None
        if actual_type is None:
            schema_issues.append(f"missing:{col}")
        elif actual_type != expected_type:
            schema_issues.append(f"{col}:{actual_type}!=expected:{expected_type}")

    report = {
        "run_date": RUN_DATE,
        "rows": len(df),
        "missing_columns": ", ".join(missing_cols),
        "schema_issues": ", ".join(schema_issues)
    }
    for col, count in null_counts.items():
        report[f"nulls_{col}"] = int(count)
        report[f"null_pct_{col}"] = round(null_pct[col], 2)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pd.DataFrame([report]).to_csv(output_path, index=False)


def write_parquet(df_pandas, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_pandas.to_parquet(output_path, index=False)

def run_gold_layer():
    print("--- 🔗 ZONE GOLD : Jointures et Indicateurs Métiers ---")

    # Charger la Télémétrie
    df_telemetry = spark.read.csv("data/silver/telemetry/telemetry_clean.csv", header=True, inferSchema=True)
    df_telemetry = (
        df_telemetry.withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
    )
    
    # Agrégation Spark
    df_temp_avg = df_telemetry.groupBy("vin").agg(
        F.avg("temp_moteur").alias("temp_moyenne"),
        F.avg("km_actuel").alias("km_actuel"),
        F.avg("pression_huile").alias("pression_huile"),
        F.avg("regime_moteur").alias("regime_moteur"),
        F.avg("voltage_batterie").alias("voltage_batterie"),
        F.avg("temps_0_100").alias("temps_0_100"),
        F.avg("conso_essence").alias("conso_essence"),
        F.avg("pression_injection").alias("pression_injection")
    )

    # Charger la Maintenance
    df_maint_raw = spark.read.csv("data/bronze/csv/maintenance.csv", header=True, inferSchema=True)
    
    # Calcul Spark
    df_maint = df_maint_raw.withColumn("jours_revis", 
        F.datediff(F.current_date(), F.col("date_last_revis")))

    # Charger la Flotte
    conn = sqlite3.connect('data/bronze/sql/flotte.db')
    pdf_flotte = pd.read_sql_query("SELECT * FROM vehicules", conn)
    conn.close()
    df_flotte = spark.createDataFrame(pdf_flotte)

    # --- LES JOINTURES SPARK ---
    df_gold = df_temp_avg.join(df_maint, "vin").join(df_flotte, "vin")

    # --- CALCUL DU SCORE S ---
    df_gold = df_gold.withColumn("age_score", (F.col("jours_revis") / 365) * 100)
    df_gold = df_gold.withColumn(
        "score_risque",
        (F.col("temp_moyenne") * 0.6) + (F.col("age_score") * 0.4)
    )
    df_gold = df_gold.withColumn(
        "score_risque",
        F.when(F.col("score_risque") > 100, 100).otherwise(F.col("score_risque"))
    )

    # Segmentation
    df_gold = df_gold.withColumn("statut", 
        F.when(F.col("score_risque") > 100, "CRITIQUE")
        .when(F.col("score_risque") > 80, "ALERTE")
        .otherwise("OK"))

    print("✅ Agrégations et jointures Spark terminées.")

    # Ajouter km_depuis_revis
    df_gold = df_gold.withColumn("km_depuis_revis", 
        F.col("km_actuel") - F.col("km_derniere_revis"))

    # Convertir en pandas pour ML
    df_gold_pandas = df_gold.toPandas()

    # Prédictions ML (partie IA separée)
    df_gold_pandas = add_ml_predictions(df_gold_pandas, model_path="model_pannes.pkl")
    
    # --- AGRÉGATIONS TEMPORELLES ---
    df_predictions = spark.createDataFrame(df_gold_pandas[["vin", "type_panne_predit", "prob_panne"]])
    df_pannes_daily = (
        df_telemetry.join(df_predictions, on="vin", how="left")
        .groupBy("event_date")
        .agg(
            F.countDistinct("vin").alias("vehicules_actifs"),
            F.sum(F.when(F.col("type_panne_predit") != 0, 1).otherwise(0)).alias("pannes_predites"),
            F.avg("prob_panne").alias("prob_panne_moy"),
            F.avg("temp_moteur").alias("temp_moy"),
            F.avg("voltage_batterie").alias("voltage_moy")
        )
        .orderBy("event_date")
    )

    df_pannes_weekly = (
        df_telemetry.join(df_predictions, on="vin", how="left")
        .withColumn("week_start", F.date_trunc("week", F.col("event_date")))
        .groupBy("week_start")
        .agg(
            F.countDistinct("vin").alias("vehicules_actifs"),
            F.sum(F.when(F.col("type_panne_predit") != 0, 1).otherwise(0)).alias("pannes_predites"),
            F.avg("prob_panne").alias("prob_panne_moy"),
            F.avg("temp_moteur").alias("temp_moy"),
            F.avg("voltage_batterie").alias("voltage_moy")
        )
        .orderBy("week_start")
    )

    df_pannes_monthly = (
        df_telemetry.join(df_predictions, on="vin", how="left")
        .withColumn("month_start", F.date_trunc("month", F.col("event_date")))
        .groupBy("month_start")
        .agg(
            F.countDistinct("vin").alias("vehicules_actifs"),
            F.sum(F.when(F.col("type_panne_predit") != 0, 1).otherwise(0)).alias("pannes_predites"),
            F.avg("prob_panne").alias("prob_panne_moy"),
            F.avg("temp_moteur").alias("temp_moy"),
            F.avg("voltage_batterie").alias("voltage_moy")
        )
        .orderBy("month_start")
    )

    # --- SAUVEGARDE FINALE ---
    os.makedirs(GOLD_OUTPUT_DIR, exist_ok=True)
    os.makedirs(GOLD_PARQUET_DIR, exist_ok=True)
    os.makedirs(GOLD_AGG_DIR, exist_ok=True)

    final_path = os.path.join(GOLD_OUTPUT_DIR, "reporting_final.csv")
    df_gold_pandas.to_csv(final_path, index=False)

    write_parquet(df_gold_pandas, os.path.join(GOLD_PARQUET_DIR, "reporting_final.parquet"))

    agg_day_csv = os.path.join(GOLD_AGG_DIR, f"pannes_par_jour_{RUN_DATE}.csv")
    agg_day_parquet = os.path.join(GOLD_AGG_DIR, f"pannes_par_jour_{RUN_DATE}.parquet")
    df_pannes_daily_pd = df_pannes_daily.toPandas()
    df_pannes_daily_pd.to_csv(agg_day_csv, index=False)
    write_parquet(df_pannes_daily_pd, agg_day_parquet)

    agg_week_csv = os.path.join(GOLD_AGG_DIR, f"pannes_par_semaine_{RUN_DATE}.csv")
    agg_week_parquet = os.path.join(GOLD_AGG_DIR, f"pannes_par_semaine_{RUN_DATE}.parquet")
    df_pannes_weekly_pd = df_pannes_weekly.toPandas()
    df_pannes_weekly_pd.to_csv(agg_week_csv, index=False)
    write_parquet(df_pannes_weekly_pd, agg_week_parquet)

    agg_month_csv = os.path.join(GOLD_AGG_DIR, f"pannes_par_mois_{RUN_DATE}.csv")
    agg_month_parquet = os.path.join(GOLD_AGG_DIR, f"pannes_par_mois_{RUN_DATE}.parquet")
    df_pannes_monthly_pd = df_pannes_monthly.toPandas()
    df_pannes_monthly_pd.to_csv(agg_month_csv, index=False)
    write_parquet(df_pannes_monthly_pd, agg_month_parquet)

    quality_path = os.path.join(QUALITY_DIR, f"gold_quality_{RUN_DATE}.csv")
    write_quality_report(df_gold_pandas, quality_path, REQUIRED_GOLD_COLUMNS, EXPECTED_GOLD_SCHEMA)

    print("✨ Rapport final généré dans data/gold/reporting_final.csv")
    print(f"📁 Parquet gold ici : {GOLD_PARQUET_DIR}")
    print(f"📁 Agrégations ici : {GOLD_AGG_DIR}")
    print(f"📁 Rapport qualite ici : {quality_path}")
    print(df_gold_pandas[["vin", "modele", "score_risque", "statut", "type_panne_predit", "prob_panne"]].head(5))

if __name__ == "__main__":
    run_gold_layer()
    spark.stop()