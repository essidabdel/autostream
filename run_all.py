import os
import subprocess
import sys

def run_script(script_name):
    print(f"--- Exécution de {script_name} ---")
    try:
        subprocess.run([sys.executable, script_name], check=True)
        print(f"✅ {script_name} terminé avec succès.\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {script_name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("🚀 DÉMARRAGE DU PIPELINE AUTOSTREAM\n")
    
    # Simulation des sources (Bronze)
    run_script("creation_data/generator.py")
    
    # Nettoyage Spark (Silver)
    run_script("creation_data/pipeline_spark.py")
    
    # Calcul métier et Score S (Gold)
    run_script("creation_data/pipeline_gold.py")
    
    print("✨ TOUTES LES ÉTAPES SONT TERMINÉES.")
    print("📊 Les résultats finaux sont dans : data/gold/reporting_final.csv")