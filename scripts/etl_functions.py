import logging
import pandas as pd
from sqlalchemy import create_engine



def extract(file_path : str):
    # Configuration de la journalisation
    logging.basicConfig(filename='log/extraction.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        data = pd.read_parquet(file_path)
        # Enregistrement d'un message d'information dans le journal
        logging.info(f"Extraction réussie depuis {file_path}")
        return data
    except Exception as e:
        # Enregistrement d'une erreur dans le journal
        logging.error(f"Erreur lors de l'extraction depuis {file_path}: {str(e)}")
        return None


def transform(df):
    logging.basicConfig(filename='log/transform.log', level=logging.INFO, 
                        format='%(asctime)s:%(levelname)s:%(message)s')
    try:
        # Suppression des observations avec des valeurs manquantes dans les colonnes spécifiées
        df_filtered = df.dropna(axis=0, subset=['passenger_count', 'total_amount'])

        #Suppression des colonnes airport_fee et congestion_surcharge car ses colonne contiennent trop de valeurs manquantes
        # df_filtered = df_filtered.drop(['congestion_surcharge', 'airport_fee'], axis =1)
        
        df_filtered.rename(columns=str.lower, inplace=True)
        
        # Log du message de succès
        logging.info("La transformation a été effectuée avec succès.")
        
        return df_filtered
    except Exception as e:
        logging.error("Erreur lors de la transformation: {}".format(e))
        
        return None


def load(df, table_name, connection_string):
    logging.basicConfig(filename='log/load.log', level=logging.INFO, 
                        format='%(asctime)s:%(levelname)s:%(message)s')
    
    try:
        # Création de l'engine de base de données
        engine = create_engine(connection_string)

        print("Debut de l'insertion")
        
        # Chargement du DataFrame dans la table PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print("Fin de l'insertion")
        
        # Fermeture de la connexion à la base de données
        engine.dispose()
        
        logging.info("Les données ont été chargées avec succès dans la table '{}'.".format(table_name))
        
        return True
    except Exception as e:
        logging.error("Une erreur est survenue lors du chargement des données : {}".format(e))
        return False
