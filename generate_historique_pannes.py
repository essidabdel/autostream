import pandas as pd
import random

# Repartition cible (200 lignes)
# 40% OK, 15% Batterie, 15% Moteur, 20% Freins, 10% Turbo
TARGET_COUNTS = {
    0: 80,   # OK
    1: 30,   # Batterie
    2: 30,   # Moteur
    3: 40,   # Freins
    4: 20    # Turbo
}

random.seed(42)

def generate_row(vin, type_panne):
    # Donnees OBD simulees par type de panne pour rendre les classes separables
    if type_panne == 0:  # OK
        temp_moteur = random.uniform(85, 100)
        pression_huile = random.uniform(3.0, 5.0)
        regime_moteur = random.randint(800, 3000)
        voltage_batterie = random.uniform(12.2, 14.5)
        km_actuel = random.randint(10000, 180000)
        km_depuis_revis = random.randint(100, 20000)
    elif type_panne == 1:  # Batterie
        temp_moteur = random.uniform(85, 105)
        pression_huile = random.uniform(2.8, 4.5)
        regime_moteur = random.randint(800, 3200)
        voltage_batterie = random.uniform(10.5, 11.8)
        km_actuel = random.randint(20000, 220000)
        km_depuis_revis = random.randint(5000, 35000)
    elif type_panne == 2:  # Moteur
        temp_moteur = random.uniform(110, 125)
        pression_huile = random.uniform(1.8, 2.8)
        regime_moteur = random.randint(2000, 4500)
        voltage_batterie = random.uniform(11.5, 14.0)
        km_actuel = random.randint(60000, 250000)
        km_depuis_revis = random.randint(20000, 50000)
    elif type_panne == 3:  # Freins
        temp_moteur = random.uniform(85, 105)
        pression_huile = random.uniform(2.5, 4.5)
        regime_moteur = random.randint(800, 3500)
        voltage_batterie = random.uniform(11.8, 14.5)
        km_actuel = random.randint(40000, 250000)
        km_depuis_revis = random.randint(35000, 50000)
    else:  # Turbo
        temp_moteur = random.uniform(95, 115)
        pression_huile = random.uniform(2.0, 3.2)
        regime_moteur = random.randint(3800, 5200)
        voltage_batterie = random.uniform(11.8, 14.5)
        km_actuel = random.randint(50000, 240000)
        km_depuis_revis = random.randint(15000, 45000)

    return {
        'vin': vin,
        'temp_moteur': temp_moteur,
        'pression_huile': pression_huile,
        'regime_moteur': regime_moteur,
        'voltage_batterie': voltage_batterie,
        'km_actuel': km_actuel,
        'km_depuis_revis': km_depuis_revis,
        'type_panne': type_panne  # 0=OK, 1=Batterie, 2=Moteur, 3=Freins, 4=Turbo
    }

# Simulation d'un historique de 200 vehicules avec repartition controlee
historique = []
index = 0
for type_panne, count in TARGET_COUNTS.items():
    for _ in range(count):
        vin = f'VIN-HIST-{index}'
        historique.append(generate_row(vin, type_panne))
        index += 1

# Melanger pour eviter les blocs par classe
random.shuffle(historique)

df = pd.DataFrame(historique)
df.to_csv('data_historique_pannes.csv', index=False)
print(f"✅ Historique généré : {len(df)} véhicules")
print(df.head())