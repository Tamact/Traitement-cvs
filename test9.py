import os
import json
import PyPDF2
import google.generativeai as genai
from dotenv import load_dotenv
import time
import traceback
from docx import Document
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from datetime import date
from threading import Lock

# --- import des embeddings
from sentence_transformers import SentenceTransformer

# 1. Chargement de la clé API
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    print("Erreur: Clé API Gemini non trouvée.")
    exit()

# 2. Configuration de l'API Generative AI
genai.configure(api_key=API_KEY)

# Configuration du modèle : ajustez selon vos besoins
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 381920,  
}

# --- Chargement de 3 modèles SentenceTransformer
model1 = SentenceTransformer('all-MPNet-base-v2')
model2 = SentenceTransformer('paraphrase-MiniLM-L12-v2')
model3 = SentenceTransformer('all-MiniLM-L12-v2')


# 3. Fonctions d'extraction de texte
def extract_text_from_docx(file_path):
    document = Document(file_path)
    return "\n".join(paragraph.text for paragraph in document.paragraphs)

def extract_text_from_pdf(file_path):
    with open(file_path, 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        return "\n".join(page.extract_text() or "" for page in reader.pages)

# 4. Prétraitement du texte
def preprocess_text(text):
    # Conversion en minuscule
    text = text.lower()
    # Suppression des caractères spéciaux
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return text

# 5. Concaténation des embeddings
def get_concatenated_embeddings(preprocessed_text):
    """
    Retourne un unique vecteur (liste de floats) qui est la concaténation
    des embeddings issus de model1, model2 et model3.
    """
    emb1 = model1.encode([preprocessed_text])[0].tolist()
    emb2 = model2.encode([preprocessed_text])[0].tolist()
    emb3 = model3.encode([preprocessed_text])[0].tolist()

    concatenated_vector = emb1 + emb2 + emb3
    return concatenated_vector

# 6. Analyse du CV via l'API Generative AI
def analyze_cv(cv_text):
    """
    Envoie le texte à l'API Generative AI avec un prompt adapté,
    et gère le retry en cas de 429 (rate limit).
    """
    retries = 3
    delay = 30

    for attempt in range(retries):
        try:
            model = genai.GenerativeModel(
                model_name="gemini-1.5-flash",
                generation_config=generation_config,
            )

            chat_session = model.start_chat(
                history=[
                    {
                        "role": "user",
                        "parts": [
                            "Voici un CV. Veuillez extraire et structurer les informations demandées :",
                            cv_text,
                            """Le JSON que je souhaite en sortie doit avoir la forme :
{
  "candidat": {
    "nom_prenom": "...",
    "mail": "...",
    "num_tel": "...",
    "profil": "..."
  },
  "cv": {
    "date_insertion": "YYYY-MM-DD",
    "cv_text": "...",
    "cv_text_nonpretraite": "...",
    "competences": [...],
    "experience": [
      {
        "poste": "...",
        "entreprise": "...",
        "date_debut": "...",
        "date_fin": "...",
        "missions": [...]
      }
    ],
    "resume_cv": "...",
    "commitment": "null",
    "disponibilite": "null",
    "exp_salaire": 0,
    "domaine_etude": "...",
    "langues": [...],
    "education": [...]
  }
}

Notes :
- "date_insertion" correspond à la date du jour au format YYYY-MM-DD.
- "commitment" et "disponibilite" sont "null" par défaut si non spécifié.
- "exp_salaire" vaut un nombre, par exemple 150000.
- Tu dois essayer de deviner, extraire ou adapter au mieux. 
- S'il n'y a pas d'information, mets des chaînes vides ou "null".
- Pas de commentaire dans le JSON, juste les champs demandés.
"""
                        ],
                    }
                ]
            )

            response = chat_session.send_message(
                "Génère uniquement ce JSON, sans balises de code. "
                "Pas besoin de phrases supplémentaires ni d'explications."
            )

            response_text = response.text.strip()
            # Enlever les éventuelles balises ```...```
            response_text = re.sub(r"```(?:json)?(.*?)```", r"\1", response_text, flags=re.DOTALL)

            # Tenter de parser en JSON
            try:
                return json.loads(response_text)
            except json.JSONDecodeError:
                # En cas d'erreur, on peut renvoyer la chaîne brute pour debug
                return response_text

        except Exception as e:
            if "429" in str(e):
                print(f"Erreur 429 (rate limit) - tentative {attempt+1}/{retries}, "
                      f"nouvel essai dans {delay}s.")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Erreur lors de l'analyse du CV : {str(e)}")
                print(traceback.format_exc())
                break

    return None


def load_or_init_results(output_file):
    """
    Charge les résultats existants si le JSON est valide.
    Sinon, renvoie une liste vide.
    """
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("Warning: fichier JSON invalide, on va le remplacer.")
    return []


# --- Nouveau : fonction pour éviter les conflits d'écriture concurrente
def safe_append_result(output_file, new_entry, lock):
    """
    Lit le JSON existant (ou initialise une liste),
    ajoute l'entrée new_entry,
    et réécrit le tout dans le fichier.
    Le lock assure qu'une seule thread écrit à la fois.
    """
    lock.acquire()
    try:
        results = load_or_init_results(output_file)
        results.append(new_entry)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
    finally:
        lock.release()


# 7. Fonction pour traiter un CV
def process_cv_file(cv_file_path):
    if cv_file_path.endswith(".docx"):
        cv_text = extract_text_from_docx(cv_file_path)
    elif cv_file_path.endswith(".pdf"):
        cv_text = extract_text_from_pdf(cv_file_path)
    else:
        return None  # Format non pris en charge

    # Prétraitement
    preprocessed_text = preprocess_text(cv_text)

    # Embeddings concaténés
    concatenated_vector = get_concatenated_embeddings(preprocessed_text)

    # Analyse via l'API
    analysis = analyze_cv(cv_text=preprocessed_text)

    # On vérifie que l'analyse est un dict
    if analysis and isinstance(analysis, dict):
        final_json = {
            "file_name": os.path.basename(cv_file_path),
            "cv_vector": concatenated_vector
        }
        final_json.update(analysis)
        return final_json
    else:
        
        return None


# 8. Fonction principale
def process_all_cvs(cv_folder, output_file):
    if not os.path.isdir(cv_folder):
        print(f"Erreur: Le dossier {cv_folder} n'existe pas.")
        return

    # Chargement du JSON pour voir quels CV sont déjà traités
    existing_results = load_or_init_results(output_file)
    processed_files = {res["file_name"] for res in existing_results if "file_name" in res}

    # Repérage des CVs non encore traités
    cv_files = [
        os.path.join(cv_folder, f)
        for f in os.listdir(cv_folder)
        if f.endswith(('.pdf', '.docx')) and f not in processed_files
    ]

    print(f"Nombre de CVs à traiter : {len(cv_files)}")

    # Création d'un verrou pour l'écriture concurrente dans le fichier JSON
    write_lock = Lock()

    # ThreadPoolExecutor
    max_workers = 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_cv_file, cv_file_path): cv_file_path
            for cv_file_path in cv_files
        }

        for future in as_completed(futures):
            cv_file_path = futures[future]
            try:
                result = future.result()
                if result:
                    # Ecriture immédiate dans le fichier JSON
                    safe_append_result(output_file, result, write_lock)
                    print(f"Traitement terminé pour : {os.path.basename(cv_file_path)}")
                else:
                    print(f"Pas de résultat pour {os.path.basename(cv_file_path)} (analyse échouée).")
            except Exception as e:
                print(f"Erreur dans le thread pour {cv_file_path}: {str(e)}")
                traceback.print_exc()

    print(f"Toutes les tâches de traitement de CV sont terminées.\n"
          f"Vous pouvez consulter à tout moment le fichier {output_file} pour voir les enregistrements.")


# Point d'entrée
if __name__ == "__main__":
    cv_folder = 'CVs_telecharges'
    output_file = 'cv2.json'
    process_all_cvs(cv_folder, output_file)
