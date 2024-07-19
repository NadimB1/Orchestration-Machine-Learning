import os
import pandas as pd
import numpy as np
from collections import defaultdict

from os import (
    listdir,
    path,
)

from bs4 import BeautifulSoup
from pandas import (
    DataFrame,
    concat,
    read_csv,
)

from requests import (
    get,
)

##################################################################################################################
class GazsData:
    # Initialisation de la classe avec le dossier de téléchargement des fichiers CSV.
    def __init__(self, download_folder):
        self.download_folder = download_folder
        self.url = "https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/"

    # Télécharge les fichiers CSV depuis une URL donnée.
    def download_csv_files(self, target_year="2024"):
        downloaded_files = listdir(self.download_folder)
        response = get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Parcoure les dossiers de chaque année sur la page web.
        for year_folder in soup.select('a[href$="/"]'):
            year = year_folder['href'].strip('/')
            if year == target_year:
                year_url = self.url + year_folder['href']
                response_year = get(year_url)
                soup_year = BeautifulSoup(response_year.text, 'html.parser')

                # Parcoure les fichiers CSV dans chaque dossier annuel.
                for csv_file in soup_year.select('a[href$=".csv"]'):
                    csv_url = year_url + csv_file['href']
                    csv_filename = path.join(self.download_folder, csv_file['href'])

                    # Si le fichier CSV n'a pas encore été téléchargé, le télécharge.
                    if csv_file['href'] not in downloaded_files:
                        with open(csv_filename, 'wb') as f:
                            f.write(get(csv_url).content)

                            print(f"Le fichier {csv_file['href']} a été téléchargé avec succès.")

                    downloaded_files.append(csv_file['href'])

    # Lit un fichier CSV spécifique et renvoie un DataFrame pandas.
    def read_csv(self, f):
        file_path = path.join(self.download_folder, f)

        if path.getsize(file_path) > 0:  # Vérifie si le fichier n'est pas vide
            df = read_csv(file_path, sep=';')
            return df
        else:
            print(f'Le fichier {f} est vide et ne peut pas être lu.')
            return DataFrame()


if __name__ == "__main__":
    # Chemin où les fichiers CSV seront téléchargés
    #download_folder = r'/home/nadim/orchestration/data/gazs'
    download_folder = r'/opt/airflow/dags/data/gazs'
    
    # Assurez-vous que le dossier de téléchargement existe
    os.makedirs(download_folder, exist_ok=True)

    gazs_data = GazsData(download_folder)
    gazs_data.download_csv_files()  # Télécharge les fichiers CSV contenant les données sur les gaz.
