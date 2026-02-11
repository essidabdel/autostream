import os
import sys
import glob
import shutil
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import pandas as pd

# 1. Config Spark avec le bypass pour l'écriture Windows
BASE_PATH = os.path.abspath(os.getcwd()).replace("\\", "/")
FINAL_PATH = f"file:///{BASE_PATH}"

# Force Spark to use the same Python executable as this script (avoids Windows Store python shim)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ensure Hadoop tools are visible to the JVM on Windows
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

# Reinforce Hadoop native IO setting in the live Hadoop config
spark.sparkContext._jsc.hadoopConfiguration().set("io.nativeio.enabled", "false")
spark.sparkContext._jsc.hadoopConfiguration().set("io.native.lib.available", "false")
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.home.dir", os.environ["HADOOP_HOME"])

spark.sparkContext.setLogLevel("ERROR")

def run_gold_layer():
    print("--- 🔗 ZONE GOLD : Jointures et Indicateurs Métiers ---")

    # A. Charger la Télémétrie (Silver) - Nettoyée par Spark précédemment
    df_telemetry = spark.read.csv(f"data/silver/telemetry/telemetry_clean.csv", header=True, inferSchema=True)
    
    # Agrégation Spark : Température moyenne par VIN
    df_temp_avg = df_telemetry.groupBy("vin").agg(
        F.avg("temp_moteur").alias("temp_moyenne"),
        F.avg("km_actuel").alias("km_actuel"),
        F.avg("pression_huile").alias("pression_huile"),
        F.avg("regime_moteur").alias("regime_moteur"),
        F.avg("voltage_batterie").alias("voltage_batterie")
    )

    # B. Charger la Maintenance (Bronze CSV)
    df_maint_raw = spark.read.csv(f"data/bronze/csv/maintenance.csv", header=True, inferSchema=True)
    
    # Calcul Spark : Jours depuis la dernière révision
    df_maint = df_maint_raw.withColumn("jours_revis", 
        F.datediff(F.current_date(), F.col("date_last_revis")))

    # C. Charger la Flotte (Bronze SQL) 
    # Note : Spark a besoin d'un driver pour SQL, on passe par Pandas juste pour le LOAD
    conn = sqlite3.connect('data/bronze/sql/flotte.db')
    pdf_flotte = pd.read_sql_query("SELECT * FROM vehicules", conn)
    conn.close()
    df_flotte = spark.createDataFrame(pdf_flotte)

    # --- 🔄 LES JOINTURES SPARK (Obligatoire) ---
    # On marie les 3 sources sur le 'vin' 
    df_gold = df_temp_avg.join(df_maint, "vin").join(df_flotte, "vin")

    # --- 📈 CALCUL DU SCORE S (Indicateur Métier) ---
    # Formule : (Temp * 0.6) + ((Jours/365 * 100) * 0.4)
    df_gold = df_gold.withColumn("age_score", (F.col("jours_revis") / 365) * 100)
    df_gold = df_gold.withColumn("score_risque", 
        (F.col("temp_moyenne") * 0.6) + (F.col("age_score") * 0.4))

    # Segmentation
    df_gold = df_gold.withColumn("statut", 
        F.when(F.col("score_risque") > 100, "CRITIQUE")
        .when(F.col("score_risque") > 80, "ALERTE")
        .otherwise("OK"))

    print("✅ Agrégations et jointures Spark terminées.")
    
    # --- 💾 SAUVEGARDE FINALE ---
    # Ecriture en Python pour contourner les problemes Hadoop NativeIO sur Windows
    os.makedirs("data/gold", exist_ok=True)
    final_path = "data/gold/reporting_final.csv"

    with open(final_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df_gold.columns)
        for row in df_gold.toLocalIterator():
            writer.writerow(list(row))
    
    print("✨ Rapport final généré dans data/gold/reporting_final.csv")
    df_gold.select("vin", "modele", "score_risque", "statut").show(5)

if __name__ == "__main__":
    run_gold_layer()
    spark.stop()