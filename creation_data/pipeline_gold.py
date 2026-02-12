import os
import sys
import pickle
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import pandas as pd

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

def run_gold_layer():
    print("--- 🔗 ZONE GOLD : Jointures et Indicateurs Métiers ---")

    # Charger la Télémétrie
    df_telemetry = spark.read.csv(f"data/silver/telemetry/telemetry_clean.csv", header=True, inferSchema=True)
    
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
    df_maint_raw = spark.read.csv(f"data/bronze/csv/maintenance.csv", header=True, inferSchema=True)
    
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
    df_gold = df_gold.withColumn("score_risque", 
        (F.col("temp_moyenne") * 0.6) + (F.col("age_score") * 0.4))

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

    # Charger le modèle
    with open('model_pannes.pkl', 'rb') as f:
        model = pickle.load(f)

    # Prédictions
    features = df_gold_pandas[['temp_moyenne', 'pression_huile', 'regime_moteur', 'voltage_batterie', 'km_actuel', 'km_depuis_revis', 'temps_0_100', 'conso_essence', 'pression_injection']]
    features = features.rename(columns={'temp_moyenne': 'temp_moteur'})
    predictions = model.predict(features)
    proba_all = model.predict_proba(features)
    
    # Calculer prob_panne
    prob_panne = []
    for i, pred in enumerate(predictions):
        if pred == 0:
            prob_panne.append(proba_all[i, 1:].max())
        else: 
            prob_panne.append(proba_all[i, pred])

    # Ajouter au dataframe
    df_gold_pandas['type_panne_predit'] = predictions
    df_gold_pandas['prob_panne'] = prob_panne
    
    # Recalculer le statut basé sur la prédiction ML
    def compute_ml_status(row):
        type_panne = row['type_panne_predit']
        prob = row['prob_panne']
        
        if int(type_panne) == 0:
            return "OK"
        
        
        if prob >= 0.7:
            return "CRITIQUE"
        elif prob >= 0.4:
            return "ALERTE"
        else:
            return "SURVEILLANCE"
    
    df_gold_pandas['statut'] = df_gold_pandas.apply(compute_ml_status, axis=1)
    
    # --- SAUVEGARDE FINALE ---
    os.makedirs("data/gold", exist_ok=True)
    final_path = "data/gold/reporting_final.csv"

    df_gold_pandas.to_csv(final_path, index=False)
    
    print("✨ Rapport final généré dans data/gold/reporting_final.csv")
    print(df_gold_pandas[["vin", "modele", "score_risque", "statut", "type_panne_predit", "prob_panne"]].head(5))

if __name__ == "__main__":
    run_gold_layer()
    spark.stop()