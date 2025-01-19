import psycopg2
import json
from datetime import datetime

# Connexion à la base de données cible
conn = psycopg2.connect(
    host="localhost",
    port=15432,
    database="nyc_warehouse",
    user="postgres",
    password="admin"
)
cursor = conn.cursor()

# Lire le fichier de log JSON
with open('logs.json', 'r') as file:
    logs = json.load(file)

# Insérer les logs dans la table soda_logs
insert_query = """
INSERT INTO soda_logs (scan_date, log_content)
VALUES (%s, %s)
"""
cursor.execute(insert_query, (datetime.now(), json.dumps(logs)))

# Valider et fermer la connexion
conn.commit()
cursor.close()
conn.close()

print("Logs insérés avec succès dans la base de données !")
