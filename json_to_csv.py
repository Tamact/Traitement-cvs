import json
import csv

# Fonction pour convertir JSON en CSV
def json_to_csv(json_file, csv_file):
    with open(json_file, 'r', encoding='utf-8') as infile:
        data = json.load(infile)

    # Assurez-vous que les données sont une liste de dictionnaires
    if not isinstance(data, list):
        data = [data]

    # Ouvrir le fichier CSV pour écrire les données
    with open(csv_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)

# Nom du fichier JSON d'entrée et du fichier CSV de sortie
json_file = 'cvs.json'
csv_file = 'cvs.csv'

# Conversion JSON en CSV
json_to_csv(json_file, csv_file)
print(f"Conversion de {json_file} en {csv_file} réussie !")
