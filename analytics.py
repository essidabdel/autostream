import os
import pandas as pd
import streamlit as st

# ============================================================================
# CONSTANTES ET MAPPINGS
# ============================================================================

DATA_PATH = os.path.join("data", "gold", "reporting_final.csv")
HISTORY_PATH = os.path.join("data_historique_pannes.csv")
LIFETIME_PATH = os.path.join("data", "bronze", "csv", "piece_lifetime.csv")

PANNE_LABELS = {
    0: "OK - Aucune panne détectée",
    1: "Panne Batterie",
    2: "Panne Moteur",
    3: "Panne Freins",
    4: "Panne Turbo",
    5: "Panne Injecteur"
}

PANNE_EMOJIS = {
    0: "✅",
    1: "🔋",
    2: "🔥",
    3: "🛑",
    4: "⚙️",
    5: "🧪"
}

PANNE_DESCRIPTIONS = {
    "Batterie": "Défaillance du système électrique - Problème de charge ou batterie usée",
    "Moteur": "Surchauffe ou problème mécanique majeur - Risque d'immobilisation",
    "Freins": "Usure des plaquettes ou problème hydraulique - Sécurité compromise",
    "Turbo": "Problème de suralimentation - Perte de puissance et consommation",
    "Injecteur": "Pression d'injection faible - Surconsommation et perte de puissance",
    "OK": "Tous les systèmes fonctionnent normalement"
}

RECOMMANDATIONS = {
    "Batterie": [
        "🔧 Remplacer la batterie immédiatement",
        "⚡ Vérifier l'alternateur et le système de charge",
        "🧹 Nettoyer les bornes et connexions",
        "📊 Tester la tension à froid et à chaud"
    ],
    "Moteur": [
        "🔥 Vérifier le niveau d'huile moteur",
        "💧 Contrôler le système de refroidissement",
        "🔍 Effectuer un diagnostic électronique complet",
        "🛠️ Inspecter les joints et courroies"
    ],
    "Freins": [
        "🛑 Remplacer plaquettes et disques si nécessaire",
        "💧 Purger et remplacer le liquide de frein",
        "⚙️ Vérifier l'étrier et les pistons",
        "🚨 Test de freinage d'urgence obligatoire"
    ],
    "Turbo": [
        "⚙️ Inspecter le turbocompresseur",
        "🛢️ Vérifier pression et qualité de l'huile",
        "🌬️ Nettoyer le système d'admission d'air",
        "📈 Contrôler les durites et collecteurs"
    ],
    "Injecteur": [
        "💉 Nettoyer ou remplacer les injecteurs",
        "🔧 Vérifier la pression de la rampe d'injection",
        "🧪 Tester la qualité du carburant",
        "🔍 Contrôler les capteurs de débit et pression"
    ],
    "OK": [
        "✅ Continuer la maintenance préventive standard",
        "📅 Respecter les intervalles de révision",
        "👀 Surveiller les indicateurs OBD régulièrement"
    ]
}

# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================


def get_alert_level(prob):
    """Détermine le niveau d'alerte basé sur la probabilité de panne"""
    if prob >= 0.7:
        return "URGENT"
    if prob >= 0.5:
        return "ATTENTION"
    if prob >= 0.3:
        return "SURVEILLANCE"
    return "NORMAL"


def get_alert_emoji(level):
    """Retourne l'emoji correspondant au niveau d'alerte"""
    emojis = {
        "URGENT": "🔴",
        "ATTENTION": "🟠",
        "SURVEILLANCE": "🟡",
        "NORMAL": "🟢"
    }
    return emojis.get(level, "⚪")


def estimate_km_to_failure(row, lifetime_by_piece, type_map):
    """
    Estime l'echeance avant panne probable en kilometres.
    Basee sur la duree de vie des pieces (piece_lifetime.csv) ET la probabilite de panne ML.
    Pour les vehicules OK (type_panne=0), retourne 30000 km (prochaine revision).
    Pour les vehicules avec panne, ajuste l'echeance selon la probabilite :
    - Prob >= 70% (CRITIQUE) : max 2000 km
    - Prob 40-70% (ALERTE) : 30-50% de l'echeance normale
    - Prob < 40% (SURVEILLANCE) : 60-80% de l'echeance normale
    """
    km_depuis_revis = row.get("km_depuis_revis")
    type_panne = row.get("type_panne_predit")
    prob_panne = row.get("prob_panne")
    type_maint = row.get("type")

    if pd.isna(km_depuis_revis):
        return None

    # Si prediction OK (type_panne=0), échéance = prochaine révision (30 000 km)
    if pd.notna(type_panne) and int(type_panne) == 0:
        return 30000

    if lifetime_by_piece is None:
        return None

    # Priorite au type de panne predit si disponible, sinon type maintenance
    piece = None
    if pd.notna(type_panne) and int(type_panne) in type_map:
        piece = type_map[int(type_panne)]
    elif pd.notna(type_maint):
        piece = str(type_maint).strip()

    if not piece:
        return None

    km_median = lifetime_by_piece.get(piece)
    if km_median is None or pd.isna(km_median):
        return None

    km_restant = int(km_median - km_depuis_revis)

    # Si la pièce a dépassé sa durée de vie: intervention IMMEDIATE (0 km)
    if km_restant <= 0:
        return 0

    # Ajuster l'echeance selon la probabilite de panne
    if pd.notna(prob_panne):
        if prob_panne >= 0.7:
            # CRITIQUE : max 2000 km d'echeance
            km_restant = min(km_restant, 2000)
        elif prob_panne >= 0.4:
            # ALERTE : 30-50% de l'echeance normale
            km_restant = int(km_restant * 0.4)
        else:
            # SURVEILLANCE : 60-80% de l'echeance normale
            km_restant = int(km_restant * 0.7)

    # Arrondir a la tranche de 250 km superieure
    tranche = int(((km_restant + 249) // 250) * 250)
    return max(tranche, 0)


def load_piece_lifetime():
    """Charge la duree de vie des pieces (km_median) depuis le CSV."""
    if not os.path.exists(LIFETIME_PATH):
        return None

    df_life = pd.read_csv(LIFETIME_PATH)
    if "piece" not in df_life.columns or "km_median" not in df_life.columns:
        return None

    return dict(zip(df_life["piece"], df_life["km_median"]))


def get_health_score(prob_panne):
    """Calcule un score de sante (0-100) inverse a la probabilite de panne"""
    return int((1 - prob_panne) * 100)

# ============================================================================
# CHARGEMENT ET ENRICHISSEMENT DES DONNEES
# ============================================================================


@st.cache_data
def load_data():
    """Charge et enrichit les donnees avec cache pour performance"""
    df = pd.read_csv(DATA_PATH)
    lifetime_by_piece = load_piece_lifetime()
    type_map = {
        1: "Batterie",
        2: "Moteur",
        3: "Freins",
        4: "Turbo",
        5: "Injecteur"
    }

    # Normalisation des dates
    if "date_last_revis" in df.columns:
        df["date_last_revis"] = pd.to_datetime(df["date_last_revis"], errors="coerce")

    # Enrichissement des colonnes
    if "type_panne_predit" in df.columns:
        df["panne_label"] = df["type_panne_predit"].map(PANNE_LABELS)
        df["panne_emoji"] = df["type_panne_predit"].map(PANNE_EMOJIS)
        # Extraire juste le type sans "Panne"
        df["panne_type_simple"] = df["panne_label"].str.replace("Panne ", "").str.replace(" - Aucune panne détectée", "")

    if "prob_panne" in df.columns:
        df["alerte"] = df["prob_panne"].apply(get_alert_level)
        df["alerte_emoji"] = df["alerte"].apply(get_alert_emoji)
        df["sante"] = df["prob_panne"].apply(get_health_score)

    if "km_depuis_revis" in df.columns:
        df["km_estime"] = df.apply(
            lambda row: estimate_km_to_failure(row, lifetime_by_piece, type_map),
            axis=1
        )

    return df
