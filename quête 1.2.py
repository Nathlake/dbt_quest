import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pymysql

# Paramètres de connexion.
username = "root"
password = ""
host = "localhost"
port = 3306
database = "my_dbt_db"

# On créée la connexion vers la base de données.
DATABASE_URI = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'

# Test de la connexion
try:
    engine = create_engine(DATABASE_URI)
    with engine.connect() as conn:
        print("Connexion réussie à la base de données!")
except pymysql.MySQLError as e:
    print(f"Erreur de connexion : {e}")
    exit(1)

# Vérification de l'existence de la base de données et création si nécessaire
if not database_exists(engine.url):
    try:
        create_database(engine.url)
        print(f"La base de données '{database}' a été créée.")
    except Exception as e:
        print(f"Erreur lors de la création de la base de données : {e}")
else:
    print(f"La base de données '{database}' existe déjà.")

# On crée un DataFrame pour chaque fichier CSV de la base de données.
liste_tables = ["customers", "items", "orders", "products", "stores", "supplies"]

for table in liste_tables:
    csv_url = f"https://raw.githubusercontent.com/dsteddy/jaffle_shop_data/main/raw_{table}.csv"
    try:
        df = pd.read_csv(csv_url)
        print(f"Fichier {table} chargé avec succès!")
    except Exception as e:
        print(f"Erreur de téléchargement du fichier CSV {table}: {e}")
        continue

    # On ajoute les informations du dataframe à la table dans MySQL.
    try:
        df.to_sql(f"raw_{table}", engine, if_exists="replace", index=False)
        print(f"Table '{table}' ajoutée avec succès à MySQL!")
    except Exception as e:
        print(f"Erreur lors de l'ajout de la table {table} à MySQL: {e}")

print("Exécution du script terminée.")
