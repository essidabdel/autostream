import pickle


def _compute_ml_status(row):
    type_panne = row["type_panne_predit"]
    prob = row["prob_panne"]

    if int(type_panne) == 0:
        return "OK"

    if prob >= 0.7:
        return "CRITIQUE"
    if prob >= 0.4:
        return "ALERTE"
    return "SURVEILLANCE"


def add_ml_predictions(df_gold_pandas, model_path="model_pannes.pkl"):
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    features = df_gold_pandas[[
        "temp_moyenne",
        "pression_huile",
        "regime_moteur",
        "voltage_batterie",
        "km_actuel",
        "km_depuis_revis",
        "temps_0_100",
        "conso_essence",
        "pression_injection"
    ]]
    features = features.rename(columns={"temp_moyenne": "temp_moteur"})

    predictions = model.predict(features)
    proba_all = model.predict_proba(features)

    prob_panne = []
    for i, pred in enumerate(predictions):
        if pred == 0:
            prob_panne.append(proba_all[i, 1:].max())
        else:
            prob_panne.append(proba_all[i, pred])

    df_gold_pandas["type_panne_predit"] = predictions
    df_gold_pandas["prob_panne"] = prob_panne
    df_gold_pandas["statut"] = df_gold_pandas.apply(_compute_ml_status, axis=1)

    return df_gold_pandas
