import os
import json
import shutil

# Configurations
dossier_source = "Cvs_telecharges"  # Dossier contenant les fichiers CVs
dossier_destination = "cvs_nontraites"  # Dossier où copier les CVs correspondants
json_file = "cv_incomplets.json"  # Fichier JSON contenant les noms des fichiers CVs

# Charger les noms des fichiers depuis le JSON
with open(json_file, "r", encoding="utf-8") as file:
    cvs_data = json.load(file)

# Extraire les noms des fichiers depuis le JSON
file_names_in_json = {cv["file_name"] for cv in cvs_data}

# Vérifier que le dossier de destination existe, sinon le créer
if not os.path.exists(dossier_destination):
    os.makedirs(dossier_destination)

# Parcourir les fichiers dans le dossier source
for file_name in os.listdir(dossier_source):
    if file_name in file_names_in_json:
        # Construire les chemins complets source et destination
        source_path = os.path.join(dossier_source, file_name)
        destination_path = os.path.join(dossier_destination, file_name)

        # Copier le fichier vers le dossier destination
        shutil.copy(source_path, destination_path)
        print(f"Fichier copié : {file_name}")

print(f"Tous les fichiers correspondants ont été copiés dans '{dossier_destination}'.")
