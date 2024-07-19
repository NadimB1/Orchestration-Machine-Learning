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

class GazDataProcessor:
    def __init__(self, input_folder, output_folder):
        self.input_folder = input_folder
        self.output_folder = output_folder

    # Définit une fonction d'agrégation personnalisée pour utiliser dans la méthode groupby
    def custom_agg(self, x):
        if pd.api.types.is_string_dtype(x):
            return x.iloc[0]
        elif pd.api.types.is_numeric_dtype(x):
            return x.mean()
        else:
            return np.nan

    # Traite les fichiers CSV pour nettoyer les données et extraire les informations pertinentes
    def process_csv_files(self):
        # Parcourir tous les fichiers du répertoire
        for file_name in os.listdir(self.input_folder):
            # Vérifier si le fichier est un fichier CSV et ne se termine pas par '_output.csv'
            if file_name.endswith('.csv') and not file_name.endswith('_output.csv'):
                # Définir le chemin complet du fichier
                file_path = os.path.join(self.input_folder, file_name)

                # Vérifier si le fichier de sortie existe déjà
                output_file_name = file_name[:-4] + '_output.csv'
                output_file_path = os.path.join(self.output_folder, output_file_name)
                if os.path.exists(output_file_path):
                    print(f"Output file {output_file_name} already exists in the output folder. Skipping this file.")
                    continue

                # Charger les données
                data = pd.read_csv(file_path, sep=';')

                # Vérifier si les colonnes 'Polluant' et 'Zas' existent dans le DataFrame
                if 'Polluant' not in data.columns or 'Zas' not in data.columns:
                    print(f"'Polluant' or 'Zas' column not found in {file_name}. Skipping this file.")
                    continue

                # Remplacer les polluants selon les conditions données
                data['Polluant'] = data['Polluant'].replace({'NO': 'NO2', 'NOX': 'NO2', 'NOX as NO2': 'NO2', 'PM2.5': 'PM25'})

                # Supprimer les lignes contenant 'C6H6', 'SO2', et 'CO' dans la colonne 'Polluant'
                data = data[~data['Polluant'].isin(['C6H6', 'SO2', 'CO'])]

                # Définir les colonnes d'intérêt
                cols_of_interest = ['Date de début', 'Date de fin', 'Polluant', 'valeur', 'code qualité', 'unité de mesure']

                # Supprimer les lignes contenant des valeurs vides dans les colonnes d'intérêt
                data = data.dropna(subset=cols_of_interest)

                # Convertir 'Date de fin' au format datetime et extraire uniquement la date
                data['Date de fin'] = pd.to_datetime(data['Date de fin']).dt.date

                # Grouper par 'Date de fin', 'Polluant', et 'Zas' et calculer la moyenne de 'valeur'
                # Garder aussi les autres colonnes en prenant la première valeur de chaque groupe
                grouped_data = data.groupby(['Date de fin', 'Polluant', 'Zas']).agg(self.custom_agg).reset_index()

                # Supprimer les colonnes spécifiées
                grouped_data = grouped_data.drop(columns=['taux de saisie', 'couverture temporelle', 'couverture de données'])
                grouped_data['unité de mesure'] = grouped_data['unité de mesure'].str.replace('Â', '')

                # Sélectionner les colonnes à conserver selon l'analyse
                cols_to_keep = ['Date de fin', 'Polluant', 'Zas', "type d'implantation", "type d'influence", "type d'évaluation", 'procédure de mesure', 'valeur', 'code qualité', 'unité de mesure']

                # Créer un nouveau DataFrame contenant uniquement les colonnes à conserver
                final_data = grouped_data[cols_to_keep]

                # Sauvegarder le DataFrame final dans un nouveau fichier CSV
                final_data.to_csv(output_file_path, index=False)

if __name__ == "__main__":
    # Dossiers pour le traitement des données
    #input_folder = r"/home/nadim/orchestration/data/gazs"
    #output_folder = r"/home/nadim/orchestration/data/gazs_output"
    
    input_folder = r"/opt/airflow/dags/data/gazs"
    output_folder = r"/opt/airflow/dags/data/gazs_output"

    os.makedirs(output_folder, exist_ok=True)

    gaz_data_processor = GazDataProcessor(input_folder, output_folder)
    gaz_data_processor.process_csv_files()