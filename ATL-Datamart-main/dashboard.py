import streamlit as st
import psycopg2
import pandas as pd
import json
from sqlalchemy import create_engine

# Connexion à la base PostgreSQL
def connect_to_db():
    engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:15432/nyc_warehouse")
    return engine

# Charger les logs depuis la base
def fetch_logs():
    engine = connect_to_db()
    query = "SELECT scan_date, log_content FROM soda_logs ORDER BY scan_date DESC;"
    logs = pd.read_sql(query, engine)
    return logs

# Extraire les checks et leurs statuts depuis log_content
def extract_checks(log_content):
    try:
        # Si log_content est déjà un dictionnaire, pas besoin de le charger
        if isinstance(log_content, dict):
            log_data = log_content
        else:
            log_data = json.loads(log_content)  # Charger le contenu JSON

        # Vérifier que "checks" est présent
        if "checks" not in log_data:
            return [{"Nom du Check": "Aucun check trouvé", "Statut": "Non disponible", "Table": "-", "Colonne": "-"}]

        checks = log_data.get("checks", [])
        extracted_data = []
        for check in checks:
            extracted_data.append({
                "Nom du Check": check.get("name", "Non spécifié"),
                "Statut": check.get("outcome", "Non évalué"),
                "Table": check.get("table", "Non spécifié"),
                "Colonne": check.get("column", "Non spécifié")
            })
        return extracted_data
    except json.JSONDecodeError:
        return [{"Nom du Check": "Erreur JSON", "Statut": "Format JSON invalide", "Table": "-", "Colonne": "-"}]
    except Exception as e:
        return [{"Nom du Check": "Erreur", "Statut": str(e), "Table": "-", "Colonne": "-"}]

# Streamlit : Interface utilisateur
st.title("Dashboard de Qualité de Données")
st.markdown("Ce tableau de bord affiche les résultats des scans Soda Core pour suivre les indicateurs de qualité des données.")

# Charger les logs
try:
    logs = fetch_logs()
    st.success("Données chargées avec succès !")

    # Afficher les logs dans un tableau interactif
    st.subheader("Logs de Qualité des Données")
    st.dataframe(logs)

    # Parcourir chaque log pour afficher les résultats des checks
    st.subheader("Indicateurs de Performance")
    for index, row in logs.iterrows():
        scan_date = row["scan_date"]
        log_content = row["log_content"]

        st.markdown(f"### Résultats du scan du {scan_date}")
        checks_data = extract_checks(log_content)

        # Convertir en DataFrame pour affichage dans Streamlit
        checks_df = pd.DataFrame(checks_data)
        st.dataframe(checks_df)
except Exception as e:
    st.error(f"Erreur lors du chargement des données : {e}")
