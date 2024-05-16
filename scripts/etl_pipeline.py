import os
import logging
from dotenv import load_dotenv
import etl_functions

# Configuration de la journalisation
logging.basicConfig(filename='log/run_pipeline.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Charger les variables d'environnement du fichier .env
load_dotenv('./.env')

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# Initialisation des variables
path_to_histo_data_folder = r'D:\Lab\projet ETL\data'
connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Définition de la fonction run_pipeline
def run_pipeline(path_to_histo_data_folder, connection_string):
    try:
        # Liste tous les fichiers Parquet dans le dossier spécifié
        parquet_files = [f for f in os.listdir(path_to_histo_data_folder) if f.endswith('.parquet')]
        
        for file in parquet_files:
            try:
                # Construit le chemin complet du fichier
                full_file_path = os.path.join(path_to_histo_data_folder, file)
                
                # Étape d'extraction
                print('Extraction')
                data = etl_functions.extract(full_file_path)
                
                if data is None:
                    logging.error(f'Échec de l\'extraction pour le fichier {file}')
                    continue

                # Étape de transformation
                print('Transformation')
                transformed_data = etl_functions.transform(data)
                
                if transformed_data is None:
                    logging.error(f'Échec de la transformation pour le fichier {file}')
                    continue
                
                # Étape de chargement
                print('Chargement dans la base de données')
                load_success = etl_functions.load(transformed_data, 'ytaxi_histo', connection_string)
                if load_success:
                    logging.info(f'Chargement réussi pour le fichier {file}')
                else:
                    logging.error(f'Échec du chargement pour le fichier {file}')
            except Exception as e:
                logging.error(f'Erreur lors du traitement du fichier {file}: {e}')
    except Exception as e:
        logging.error(f'Exception survenue lors de l\'exécution du pipeline : {e}')

if __name__ == '__main__':
    run_pipeline(path_to_histo_data_folder, connection_string)
