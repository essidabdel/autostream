import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession

# Config Spark
spark = SparkSession.builder \
    .appName("AutoStreamSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("🚀 Spark est prêt pour le traitement...")

def run_silver_layer():
    # Chemins
    bronze_path = os.path.join(os.getcwd(), "data", "bronze", "json")
    silver_path = os.path.join(os.getcwd(), "data", "silver", "telemetry")

    print("--- 📥 PHASE 1 : Ingestion Spark ---")
    try:
        df_spark = spark.read.json(f"data/bronze/json/*.json")
        print(f"✅ Données lues par Spark : {df_spark.count()} lignes")
        
        # --- Nettoyage Spark ---
        df_clean = df_spark.dropna(subset=["vin"]).fillna({'temp_moteur': 90.0, 'vitesse': 0})
        
        print("--- 💾 PHASE 3 : Sauvegarde (Bypass Hadoop) ---")
        
        # Transformation en Pandas
        df_pandas = df_clean.toPandas()
        
        # Création du dossier Silver
        if os.path.exists(silver_path):
            shutil.rmtree(silver_path)
        os.makedirs(silver_path, exist_ok=True)
        
        # Sauvegarde en CSV
        output_file = os.path.join(silver_path, "telemetry_clean.csv")
        df_pandas.to_csv(output_file, index=False)
        
        print(f"✨ ZONE SILVER TERMINÉE !")
        print(f"📁 Fichier créé ici : {output_file}")
        print(df_pandas.head())

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    run_silver_layer()
    spark.stop()