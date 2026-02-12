import json
import os
import random
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

# Création des dossiers si besoin
RUN_DATE = datetime.now().strftime("%Y-%m-%d")
BRONZE_ROOT = "data/bronze"
BRONZE_DATE_DIR = os.path.join(BRONZE_ROOT, f"run_date={RUN_DATE}")

for path in [
    os.path.join(BRONZE_ROOT, "json"),
    os.path.join(BRONZE_ROOT, "csv"),
    os.path.join(BRONZE_ROOT, "sql"),
    os.path.join(BRONZE_DATE_DIR, "json"),
    os.path.join(BRONZE_DATE_DIR, "csv"),
    os.path.join(BRONZE_DATE_DIR, "sql")
]:
    os.makedirs(path, exist_ok=True)

vins = [f'VIN-75{1000+i}' for i in range(50)]

modeles = ['Master', 'Expert', 'Daily', 'Sprinter', 'Transit']
annees = list(range(2018, 2025))

# Repartition cible des profils de pannes (sur 50 vehicules)
profile_counts = {
    'OK': 34,
    'Batterie': 3,
    'Moteur': 3,
    'Freins': 4,
    'Turbo': 3,
    'Injecteur': 3
}
profiles = []
for label, count in profile_counts.items():
    profiles.extend([label] * count)
random.shuffle(profiles)

profile_by_vin = {vin: profiles[i] for i, vin in enumerate(vins)}

def generate_obd_for_profile(profile):
    if profile == 'OK':
        temp_moteur = random.uniform(85, 100)
        pression_huile = random.uniform(3.0, 5.0)
        regime_moteur = random.randint(800, 3000)
        voltage_batterie = random.uniform(12.2, 14.5)
        km_actuel = random.randint(10000, 180000)
        km_depuis_revis = random.randint(1000, 20000)
        temps_0_100 = random.uniform(8.0, 10.0)
        conso_essence = random.uniform(6.0, 9.0)
        pression_injection = random.uniform(280, 350)
    elif profile == 'Batterie':
        temp_moteur = random.uniform(85, 105)
        pression_huile = random.uniform(2.8, 4.5)
        regime_moteur = random.randint(800, 3200)
        voltage_batterie = random.uniform(10.5, 11.8)
        km_actuel = random.randint(20000, 220000)
        km_depuis_revis = random.randint(5000, 35000)
        temps_0_100 = random.uniform(10.0, 12.0)
        conso_essence = random.uniform(8.0, 12.0)
        pression_injection = random.uniform(270, 340)
    elif profile == 'Moteur':
        temp_moteur = random.uniform(110, 125)
        pression_huile = random.uniform(1.8, 2.8)
        regime_moteur = random.randint(2000, 4500)
        voltage_batterie = random.uniform(11.5, 14.0)
        km_actuel = random.randint(60000, 250000)
        km_depuis_revis = random.randint(20000, 50000)
        temps_0_100 = random.uniform(14.0, 16.0)
        conso_essence = random.uniform(11.0, 15.0)
        pression_injection = random.uniform(260, 330)
    elif profile == 'Freins':
        temp_moteur = random.uniform(85, 105)
        pression_huile = random.uniform(2.5, 4.5)
        regime_moteur = random.randint(800, 3500)
        voltage_batterie = random.uniform(11.8, 14.5)
        km_actuel = random.randint(40000, 250000)
        km_depuis_revis = random.randint(35000, 50000)
        temps_0_100 = random.uniform(9.0, 11.0)
        conso_essence = random.uniform(7.0, 11.0)
        pression_injection = random.uniform(275, 345)
    elif profile == 'Turbo':
        temp_moteur = random.uniform(95, 115)
        pression_huile = random.uniform(2.0, 3.2)
        regime_moteur = random.randint(3800, 5200)
        voltage_batterie = random.uniform(11.8, 14.5)
        km_actuel = random.randint(50000, 240000)
        km_depuis_revis = random.randint(15000, 45000)
        temps_0_100 = random.uniform(13.0, 15.0)
        conso_essence = random.uniform(12.0, 16.0)
        pression_injection = random.uniform(250, 320)
    else:  # Injecteur
        temp_moteur = random.uniform(88, 108)
        pression_huile = random.uniform(2.6, 4.2)
        regime_moteur = random.randint(1000, 3500)
        voltage_batterie = random.uniform(11.9, 14.2)
        km_actuel = random.randint(40000, 230000)
        km_depuis_revis = random.randint(25000, 50000)
        temps_0_100 = random.uniform(11.0, 13.0)
        conso_essence = random.uniform(13.0, 17.0)
        pression_injection = random.uniform(200, 260)

    return {
        'temp_moteur': temp_moteur,
        'pression_huile': pression_huile,
        'regime_moteur': regime_moteur,
        'voltage_batterie': voltage_batterie,
        'km_actuel': km_actuel,
        'km_depuis_revis': km_depuis_revis,
        'temps_0_100': temps_0_100,
        'conso_essence': conso_essence,
        'pression_injection': pression_injection
    }

# SQL
conn = sqlite3.connect('data/bronze/sql/flotte.db')
df_flotte = pd.DataFrame({
    'vin': vins,
    'modele': [random.choice(modeles) for _ in vins],
    'annee': [random.choice(annees) for _ in vins]
})
df_flotte.to_sql('vehicules', conn, if_exists='replace', index=False)
conn.close()

# Copie dans le dossier date (bronze)
conn = sqlite3.connect(os.path.join(BRONZE_DATE_DIR, "sql", "flotte.db"))
df_flotte.to_sql('vehicules', conn, if_exists='replace', index=False)
conn.close()

# CSV
maintenance = []
types_maint = ['Vidange', 'Freins', 'Pneus', 'Moteur', 'Batterie']

# Pre-calcul
obd_by_vin = {vin: generate_obd_for_profile(profile_by_vin[vin]) for vin in vins}

for vin in vins:
    profile = profile_by_vin[vin]
    obd = obd_by_vin[vin]

    has_date = random.random() > 0.15
    if has_date:
        # Approximation de la date
        days_since = int(obd['km_depuis_revis'] / 50)
        days_since = max(30, min(days_since, 1200))
        date = (datetime.now() - timedelta(days=days_since)).strftime('%Y-%m-%d')
    else:
        date = None

    if profile in ['Freins', 'Moteur', 'Batterie', 'Turbo', 'Injecteur']:
        if profile == 'Turbo':
            type_maint = 'Moteur'
        elif profile == 'Injecteur':
            type_maint = 'Moteur'
        else:
            type_maint = profile
    else:
        type_maint = random.choice(types_maint)

    km_derniere_revis = max(0, int(obd['km_actuel'] - obd['km_depuis_revis']))

    maintenance.append({
        'vin': vin,
        'date_last_revis': date,
        'type': type_maint,
        'km_derniere_revis': km_derniere_revis
    })

pd.DataFrame(maintenance).to_csv('data/bronze/csv/maintenance.csv', index=False)
pd.DataFrame(maintenance).to_csv(os.path.join(BRONZE_DATE_DIR, "csv", "maintenance.csv"), index=False)

# JSON
for i in range(50):
    vin = vins[i]
    obd = obd_by_vin[vin]
    telemetry = {
        'vin': vin,
        'timestamp': (datetime.now() - timedelta(minutes=i)).isoformat(),
        'temp_moteur': obd['temp_moteur'],
        'vitesse': random.randint(70, 110),
        'km_actuel': obd['km_actuel'],
        'pression_huile': obd['pression_huile'],
        'regime_moteur': obd['regime_moteur'],
        'voltage_batterie': obd['voltage_batterie'],
        'temps_0_100': obd['temps_0_100'],
        'conso_essence': obd['conso_essence'],
        'pression_injection': obd['pression_injection']
    }
    with open(f'data/bronze/json/msg_{i}.json', 'w') as f:
        json.dump(telemetry, f)
    with open(os.path.join(BRONZE_DATE_DIR, "json", f"msg_{i}.json"), 'w') as f:
        json.dump(telemetry, f)

# CSV : Historique des Pannes
TARGET_COUNTS_HIST = {
    0: 163,  # OK
    1: 15,   # Batterie
    2: 15,   # Moteur
    3: 17,   # Freins
    4: 15,   # Turbo
    5: 15    # Injecteur
}

def generate_history_row(vin, type_panne):
    """Génère une ligne d'historique de panne avec OBD cohérent"""
    if type_panne == 0:  # OK
        temp_moteur = random.uniform(85, 100)
        pression_huile = random.uniform(3.0, 5.0)
        regime_moteur = random.randint(800, 3000)
        voltage_batterie = random.uniform(12.2, 14.5)
        km_actuel = random.randint(10000, 180000)
        km_depuis_revis = random.randint(100, 20000)
        temps_0_100 = random.uniform(8.0, 10.0)
        conso_essence = random.uniform(6.0, 9.0)
        pression_injection = random.uniform(280, 350)
    elif type_panne == 1:  # Batterie
        temp_moteur = random.uniform(85, 105)
        pression_huile = random.uniform(2.8, 4.5)
        regime_moteur = random.randint(800, 3200)
        voltage_batterie = random.uniform(10.5, 11.8)
        km_actuel = random.randint(20000, 220000)
        km_depuis_revis = random.randint(5000, 35000)
        temps_0_100 = random.uniform(10.0, 12.0)
        conso_essence = random.uniform(8.0, 12.0)
        pression_injection = random.uniform(270, 340)
    elif type_panne == 2:  # Moteur
        temp_moteur = random.uniform(110, 125)
        pression_huile = random.uniform(1.8, 2.8)
        regime_moteur = random.randint(2000, 4500)
        voltage_batterie = random.uniform(11.5, 14.0)
        km_actuel = random.randint(60000, 250000)
        km_depuis_revis = random.randint(20000, 50000)
        temps_0_100 = random.uniform(14.0, 16.0)
        conso_essence = random.uniform(11.0, 15.0)
        pression_injection = random.uniform(260, 330)
    elif type_panne == 3:  # Freins
        temp_moteur = random.uniform(85, 105)
        pression_huile = random.uniform(2.5, 4.5)
        regime_moteur = random.randint(800, 3500)
        voltage_batterie = random.uniform(11.8, 14.5)
        km_actuel = random.randint(40000, 250000)
        km_depuis_revis = random.randint(35000, 50000)
        temps_0_100 = random.uniform(9.0, 11.0)
        conso_essence = random.uniform(7.0, 11.0)
        pression_injection = random.uniform(275, 345)
    elif type_panne == 4:  # Turbo
        temp_moteur = random.uniform(95, 115)
        pression_huile = random.uniform(2.0, 3.2)
        regime_moteur = random.randint(3800, 5200)
        voltage_batterie = random.uniform(11.8, 14.5)
        km_actuel = random.randint(50000, 240000)
        km_depuis_revis = random.randint(15000, 45000)
        temps_0_100 = random.uniform(13.0, 15.0)
        conso_essence = random.uniform(12.0, 16.0)
        pression_injection = random.uniform(250, 320)
    else:  # Injecteur
        temp_moteur = random.uniform(88, 108)
        pression_huile = random.uniform(2.6, 4.2)
        regime_moteur = random.randint(1000, 3500)
        voltage_batterie = random.uniform(11.9, 14.2)
        km_actuel = random.randint(40000, 230000)
        km_depuis_revis = random.randint(25000, 50000)
        temps_0_100 = random.uniform(11.0, 13.0)
        conso_essence = random.uniform(13.0, 17.0)
        pression_injection = random.uniform(200, 260)

    return {
        'vin': vin,
        'temp_moteur': temp_moteur,
        'pression_huile': pression_huile,
        'regime_moteur': regime_moteur,
        'voltage_batterie': voltage_batterie,
        'km_actuel': km_actuel,
        'km_depuis_revis': km_depuis_revis,
        'temps_0_100': temps_0_100,
        'conso_essence': conso_essence,
        'pression_injection': pression_injection,
        'type_panne': type_panne
    }

# Génération de l'historique
historique = []
index = 0
for type_panne, count in TARGET_COUNTS_HIST.items():
    for _ in range(count):
        vin = f'VIN-HIST-{index}'
        historique.append(generate_history_row(vin, type_panne))
        index += 1

# Mélanger
random.shuffle(historique)

df_historique = pd.DataFrame(historique)
df_historique.to_csv('data/bronze/csv/data_historique_pannes.csv', index=False)
df_historique.to_csv(os.path.join(BRONZE_DATE_DIR, "csv", "data_historique_pannes.csv"), index=False)

# CSV : Durée de vie des pièces
type_map = {1: 'Batterie', 2: 'Moteur', 3: 'Freins', 4: 'Turbo', 5: 'Injecteur'}
df_hist_pannes = df_historique[df_historique['type_panne'].isin(type_map)].copy()
df_hist_pannes['piece'] = df_hist_pannes['type_panne'].map(type_map)

stats_lifetime = (
    df_hist_pannes.groupby('piece')['km_depuis_revis']
    .agg(km_median='median', km_min='min', km_max='max')
    .reset_index()
)
stats_lifetime['km_median'] = stats_lifetime['km_median'].round().astype(int)
stats_lifetime['km_min'] = stats_lifetime['km_min'].round().astype(int)
stats_lifetime['km_max'] = stats_lifetime['km_max'].round().astype(int)
stats_lifetime = stats_lifetime[['piece', 'km_median', 'km_min', 'km_max']]
stats_lifetime.to_csv('data/bronze/csv/piece_lifetime.csv', index=False)
stats_lifetime.to_csv(os.path.join(BRONZE_DATE_DIR, "csv", "piece_lifetime.csv"), index=False)

print("✅ Étape 1 terminée : Les sources Bronze sont prêtes !")
print(f"   - Flotte : {len(df_flotte)} véhicules")
print(f"   - Maintenance : {len(maintenance)} enregistrements")
print(f"   - Télémétrie : 50 messages JSON")
print(f"   - Historique pannes : {len(df_historique)} véhicules")
print(f"   - Durée de vie pièces : {len(stats_lifetime)} types")
print(f"   - Bronze daté : {BRONZE_DATE_DIR}")