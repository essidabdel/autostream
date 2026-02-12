import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pickle

df = pd.read_csv('data_historique_pannes.csv')

x = df[['temp_moteur', 'pression_huile', 'regime_moteur', 'voltage_batterie', 'km_actuel', 'km_depuis_revis']]
y = df['type_panne']

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(x_train, y_train)

score = model.score(x_test, y_test)
print(f"✅ Modèle entraîné avec une précision de {score:.2f}")

with open('model_pannes.pkl', 'wb') as f:
    pickle.dump(model, f)
print("✅ Modèle sauvegardé dans model_pannes.pkl")