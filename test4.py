import os
import logging
import json
import time
import atexit
import re

os.environ["GRPC_VERBOSITY"] = "ERROR"  
os.environ["GRPC_TRACE"] = ""           
logging.getLogger("absl").setLevel(logging.CRITICAL)

@atexit.register
def finalize_grpc():
    time.sleep(2)

import google.generativeai as genai

# Mettez votre vraie clé API ici
genai.configure(api_key="AIzaSyCABxlb20gsU0V5wqRqsCwo6ulB1b7hcMs")

def load_json_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Erreur : Fichier introuvable à l'emplacement '{path}'.")
        return None
    except json.JSONDecodeError:
        print(f"Erreur : Le fichier '{path}' ne contient pas un JSON valide.")
        return None

def transform_json_structure(file_to_transform, file_reference, model):
    if file_to_transform is None or file_reference is None:
        return None

    chat_session = model.start_chat(
        history=[
            {
                "role": "user",
                "parts": [
                    json.dumps(file_to_transform),
                    json.dumps(file_reference),
                    "Soit ces deux fichiers json. Tu transformes la structure du premier fichier "
                    "en te basant sur la structure du deuxième fichier et mets le résultat dans un nouveau json.\n"
                ],
            },
        ]
    )

    response = chat_session.send_message("Transform json")
    
    # Nettoyage de la chaîne brute
    response_text = response.text.strip()
    # Si le modèle renvoie des balises de code (```json ... ```), on les supprime
    response_text = re.sub(r"```(?:json)?(.*?)```", r"\1", response_text, flags=re.DOTALL)

    # DEBUG - Décommentez pour voir le texte brut si vous voulez diagnostiquer
    # print("DEBUG - Réponse brute du modèle :", response_text)

    try:
        transformed_json = json.loads(response_text)
        return transformed_json
    except json.JSONDecodeError:
        return None

def save_json_file(data, output_path):
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du JSON dans '{output_path}' : {e}")
        return False

generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 819002,
    "response_mime_type": "text/plain",
}

model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config=generation_config,
)

if __name__ == "__main__":
    file_to_transform_path = "cvs_fin.json"
    file_reference_path = "cv10.json"
    output_file_path = "transformed_cvs_fin.json"

    file_to_transform = load_json_file(file_to_transform_path)
    file_reference = load_json_file(file_reference_path)

    transformed_data = transform_json_structure(file_to_transform, file_reference, model)

    if isinstance(transformed_data, dict) or isinstance(transformed_data, list):
        saved = save_json_file(transformed_data, output_file_path)
        if saved:
            print("Le fichier a été transformé et enregistré avec succès.")
    else:
        print("Erreur : la transformation n'a pas produit de JSON valide.")
