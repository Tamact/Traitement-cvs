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

# Charger la clé API depuis le fichier .env
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

# Check API key
if not API_KEY:
    print("Erreur: Clé API Gemini non trouvée.")
    exit()

# Configurer l'API Generative AI
genai.configure(api_key=API_KEY)

# Configuration du modèle
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 38192,
}

def extract_text_from_docx(file_path):
    document = Document(file_path)
    return "\n".join(paragraph.text for paragraph in document.paragraphs)

def extract_text_from_pdf(file_path):
    with open(file_path, 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        return "\n".join(page.extract_text() for page in reader.pages)

def preprocess_text(text):
    # Conversion en minuscule
    text = text.lower()
    # Suppression des caractères spéciaux
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return text

def analyze_cv(cv_text):
    """
    Envoie le texte à l'API Generative AI avec gestion du retry en cas d'erreur 429.
    """
    retries = 3
    delay = 10

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
                            "Analyse le CV suivant et extrait les informations importantes :",
                            cv_text,
                            """Récupère les informations suivantes :
                            - le texte entier du cv après un traitement (mise en minuscule, suppression des caractères spéciaux).
                            - un résumé de 3 phrases du cv.
                            - le domaine d'étude du candidat.
                            - les années d'expérience du candidat.
                            - Le cursus scolaire du candidat.
                            Fournis une réponse JSON comme ceci: 
                            {
                              'texte_cv_pretraitement': '...', 
                              'resume': '...',
                              'domaine_etude': '...',
                              'annees_experience': '...',
                              'nom': '...',
                              'email': '...',
                              'telephone': '...', 
                              'competences': ['...', '...'],
                              'profil':'...',
                              'langue':'...',
                              'education':['...', '...']
                            }
                            """
                        ],
                    }
                ]
            )

            response = chat_session.send_message(
                "Fournis les informations demandées en un format structuré. "
                "Pas besoin de commentaires, donne uniquement les infos demandées. "
            )

            response_text = response.text.strip()
            # Nettoyage de la chaîne brute (suppression des ```...```)
            response_text = re.sub(r"```(?:json)?(.*?)```", r"\1", response_text, flags=re.DOTALL)

            try:
                return json.loads(response_text)
            except json.JSONDecodeError:
                # Retourne quand même la chaîne si pas de JSON valide
                return response_text

        except Exception as e:
            # On ne fait des backoff que si on a un 429
            if "429" in str(e):
                print(f"Erreur 429 (rate limit) - tentative {attempt + 1}/{retries}, "
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
            print("Warning: Fichier JSON invalide, on va le remplacer.")
    return []

def process_cv_file(cv_file_path):
    """
    Lit le fichier CV, le prétraite, puis l'envoie à l'API Generative AI.
    Retourne un dictionnaire contenant l'analyse.
    """
    if cv_file_path.endswith(".docx"):
        cv_text = extract_text_from_docx(cv_file_path)
    elif cv_file_path.endswith(".pdf"):
        cv_text = extract_text_from_pdf(cv_file_path)
    else:
        # Format non supporté
        return None

    preprocessed_text = preprocess_text(cv_text)
    
    # Appel à l'API
    analysis = analyze_cv(preprocessed_text)
    
    if analysis:
        return {
            "file_name": os.path.basename(cv_file_path),
            "cv_text": cv_text,
            "analysis": analysis,
        }
    else:
        return None

def process_all_cvs(cv_folder, output_file):
    # Vérification dossier
    if not os.path.isdir(cv_folder):
        print(f"Erreur: Le dossier {cv_folder} n'existe pas.")
        return

    # On charge l'ancien JSON
    results = load_or_init_results(output_file)

    # On récupère les fichiers déjà traités
    processed_files = {res["file_name"] for res in results}

    # Liste des CV à traiter
    cv_files = [
        os.path.join(cv_folder, f)
        for f in os.listdir(cv_folder)
        if f.endswith(('.pdf', '.docx')) and f not in processed_files
    ]

    print(f"Nombre de CVs à traiter : {len(cv_files)}")

    # Etape 1 : lecture et stockage du texte des CVs
    # (Optionnel si vous voulez séparer la lecture de l'analyse)
    # Ici, on va directement tout faire en un threadpool
    new_results = []

    # Augmenter le nombre de workers si besoin
    max_workers = 5

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_cv_file, cv_file_path): cv_file_path for cv_file_path in cv_files}
        
        for future in as_completed(futures):
            cv_file_path = futures[future]
            try:
                result = future.result()
                if result:
                    new_results.append(result)
                    print(f"Traitement terminé pour : {os.path.basename(cv_file_path)}")
            except Exception as e:
                print(f"Erreur dans le thread pour {cv_file_path}: {str(e)}")
                traceback.print_exc()

    # Fusion des anciens et nouveaux résultats
    results.extend(new_results)

    # Sauvegarde unique à la fin
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print(f"Tous les CVs ont été traités. Résultats sauvegardés dans {output_file}")

if __name__ == "__main__":
    cv_folder = 'CVs_telecharges'
    output_file = 'cvs_fin.json'
    process_all_cvs(cv_folder, output_file)
