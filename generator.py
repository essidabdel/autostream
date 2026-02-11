import json
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import random

# Création des dossiers si besoin
for path in ['data/bronze/json', 'data/bronze/csv', 'data/bronze/sql']:
    os.makedirs(path, exist_ok=True)

vins = [f'VIN-75{1000+i}' for i in range(50)]

modeles = ['Master', 'Expert', 'Daily', 'Sprinter', 'Transit']
annees = list(range(2018, 2025))

# 1. SQL : Le Référentiel Flotte (La base stable)
conn = sqlite3.connect('data/bronze/sql/flotte.db')
df_flotte = pd.DataFrame({
    'vin': vins,
    'modele': [random.choice(modeles) for _ in vins],
    'annee': [random.choice(annees) for _ in vins]
})
df_flotte.to_sql('vehicules', conn, if_exists='replace', index=False)
conn.close()

# 2. CSV : Historique Maintenance (Avec des données sales !)
maintenance = []
types_maint = ['Vidange', 'Freins', 'Pneus', 'Moteur', 'Batterie']
for vin in vins:
    has_date = random.random() > 0.15  # 85% ont une date
    if has_date:
        date = (datetime.now() - timedelta(days=random.randint(30, 1200))).strftime('%Y-%m-%d')
    else:
        date = None
    maintenance.append({
        'vin': vin,
        'date_last_revis': date,
        'type': random.choice(types_maint),
        'km_derniere_revis': random.randint(5000, 20000)
    })
pd.DataFrame(maintenance).to_csv('data/bronze/csv/maintenance.csv', index=False)

# 3. JSON : Flux IoT Télémétrie (Simule 50 messages pour les 50 véhicules)
for i in range(50):
    telemetry = {
        'vin': vins[i],  # Un message par véhicule
        'timestamp': (datetime.now() - timedelta(minutes=i)).isoformat(),
        'temp_moteur': random.uniform(85, 115), # On simule des surchauffes
        'vitesse': random.randint(70, 110),
        'km_actuel': random.randint(10000, 250000), 
        'pression_huile': random.uniform(2.5, 5.0),
        'regime_moteur': random.randint(1000, 4000),
        'voltage_batterie': random.uniform(11.5, 14.5)
    }
    with open(f'data/bronze/json/msg_{i}.json', 'w') as f:
        json.dump(telemetry, f)

print("✅ Étape 1 terminée : Les sources Bronze sont prêtes !")