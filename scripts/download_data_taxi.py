import os
import requests
import time
import random
import datetime
from fake_useragent import UserAgent

def download_histo_data(path_to_histo_data_folder):
    ua = UserAgent()
    headers = {'User-Agent': ua.random}

    try:
        # Creation du dossier pour le data s'il n'existe pas
        os.makedirs(path_to_histo_data_folder, exist_ok=True)

        # Recuperer l'année en cours
        current_year = datetime.datetime.now().year
        for year in range(current_year, 2017, -1):
            for month in range(1, 13):
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02}.parquet"
                print(url)
                file_path = os.path.join(path_to_histo_data_folder, f"yellow_tripdata_{year}-{month:02}.parquet")
                if os.path.exists(file_path):
                    print(f"Le fichier {file_path} existe déjà. Passage au fichier suivant.")
                    continue

                try:
                    with requests.Session() as session:
                        response = session.get(url, headers=headers)
                        response.raise_for_status()
                        with open(file_path, 'wb') as file:
                            file.write(response.content)
                            print(f"Fichier téléchargé : {file_path}")
                except requests.exceptions.HTTPError as http_err:
                    print(f"Erreur HTTP : {http_err}")
                except requests.exceptions.ConnectionError as conn_err:
                    print(f"Erreur de connexion : {conn_err}")
                except requests.exceptions.Timeout as timeout_err:
                    print(f"Erreur de délai d'attente : {timeout_err}")
                except requests.exceptions.RequestException as req_err:
                    print(f"Erreur de requête : {req_err}")

                time.sleep(random.uniform(1, 3))
    except OSError as os_err:
        print(f"Erreur OS : {os_err}")
    finally:
        print("Téléchargement des données historiques terminé.")
        print("Date de téléchargement :", datetime.datetime.now().strftime("%d-%m-%y"))
