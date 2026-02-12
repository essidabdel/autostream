import pandas as pd
import random

# Simulation d'un historique de 200 véhicules avec pannes
historique = []

for i in range(200):
    vin = f'VIN-HIST-{i}'
    
    # Données OBD simulées
    temp_moteur = random.uniform(85, 120)
    pression_huile = random.uniform(2.0, 5.0)
    regime_moteur = random.randint(800, 4500)
    voltage_batterie = random.uniform(11.0, 14.5)
    km_actuel = random.randint(10000, 250000)
    km_depuis_revis = random.randint(100, 50000)
    
    # Règles pour déterminer le type de panne
    type_panne = 0  # par défaut OK
    
    if voltage_batterie < 11.8:  # Batterie faible
        type_panne = 1
    elif temp_moteur > 115 and km_depuis_revis > 30000:  # Moteur surchauffé
        type_panne = 2
    elif pression_huile < 2.5:  # Huile/moteur
        type_panne = 2
    elif regime_moteur > 4200 and random.random() > 0.7:  # Turbo à risque
        type_panne = 4
    elif km_depuis_revis > 40000:  # Freins usés
        type_panne = 3
    
    historique.append({
        'vin': vin,
        'temp_moteur': temp_moteur,
        'pression_huile': pression_huile,
        'regime_moteur': regime_moteur,
        'voltage_batterie': voltage_batterie,
        'km_actuel': km_actuel,
        'km_depuis_revis': km_depuis_revis,
        'type_panne': type_panne  # 0=OK, 1=Batterie, 2=Moteur, 3=Freins, 4=Turbo
    })

df = pd.DataFrame(historique)
df.to_csv('data_historique_pannes.csv', index=False)
print(f"✅ Historique généré : {len(df)} véhicules")
print(df.head())