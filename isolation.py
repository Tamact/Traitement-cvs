import json

# Charger le fichier JSON
input_file = 'cvs.json'  # Nom du fichier contenant les CVs
output_file = 'cv_incomplets1.json'  # Nom du fichier pour sauvegarder les CVs incomplets

# Charger les données
with open(input_file, 'r', encoding='utf-8') as file:
    cvs_data = json.load(file)

# Liste pour sauvegarder les CVs incomplets
cv_incomplets = []

# Parcourir chaque CV dans les données
for cv in cvs_data:
    # Vérifier les cas incomplets
    is_cv_incomplet = (
        not cv.get("cv_text", "").strip() or  # cv_text est vide ou contient seulement des espaces
        "Veuillez me fournir le CV à analyser" in cv.get("analysis", "") or  # analysis contient un message générique
        not cv.get("analysis", "").strip()  # analysis est vide ou contient seulement des espaces
    )

    # Ajouter à la liste des CVs incomplets si le cas est vrai
    if is_cv_incomplet:
        cv_incomplets.append(cv)

# Sauvegarder les CVs incomplets dans un nouveau fichier JSON
with open(output_file, 'w', encoding='utf-8') as output:
    json.dump(cv_incomplets, output, indent=4, ensure_ascii=False)

print(f"Extraction terminée. {len(cv_incomplets)} CVs incomplets sauvegardés dans '{output_file}'.")
