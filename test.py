import os
import json
from docx import Document
import PyPDF2
import google.generativeai as genai
from dotenv import load_dotenv
import time

# Charger la clé API depuis le fichier .env
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

# Configurer l'API Generative AI
genai.configure(api_key=API_KEY)

# Configuration du modèle
generation_config = {
  "temperature": 1,
  "top_p": 0.95,
  "top_k": 40,
  "max_output_tokens": 700192,
  
}

# Fonction pour extraire le texte d'un fichier DOCX
def extract_text_from_docx(file_path):
    document = Document(file_path)
    return "\n".join([paragraph.text for paragraph in document.paragraphs])

# Fonction pour extraire le texte d'un fichier PDF
def extract_text_from_pdf(file_path):
    with open(file_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        return "\n".join(page.extract_text() for page in reader.pages)

# Fonction pour analyser un CV avec Generative AI
def analyze_cv(cv_text):
    retries = 3  # Nombre maximal de tentatives en cas d'erreur 429
    delay = 30  # Délai initial entre les tentatives (en secondes)

    for attempt in range(retries):
        try:
            # Initialiser le modèle
            model = genai.GenerativeModel(
                model_name="gemini-1.5-flash",
                generation_config=generation_config,
            )

            # Début de la session de chat
            chat_session = model.start_chat(
                history=[
                    {
                        "role": "user",
                        "parts": [
                            "Analyse le CV suivant et extrait les informations importantes :",
                            cv_text,
                            "Récupère : Nom, Email, Téléphone, Compétences, domaine_etude, annee_experience, text_cv, profil, resumé de phrase du cv"
                        ],
                    }
                ]
            )

            # Envoi de la requête d'analyse
            response = chat_session.send_message(
    "Fournis les informations demandées en un format JSON structuré. Exemple : "
    "{'nom': 'John Doe', 'email': 'john.doe@example.com', 'competences': ['Python', 'Machine Learning']}. "
    "Pas besoin de commentaires, juste les infos demandées."
)

            return response.text  # Texte généré par le modèle

        except Exception as e:
            if "429" in str(e):
                print(f"Tentative {attempt + 1} échouée : Limite atteinte, nouvelle tentative dans {delay} secondes.")
                time.sleep(delay)
                delay *= 2  # Doubler le délai après chaque tentative
            else:
                print(f"Erreur lors de l'analyse du CV : {str(e)}")
                break

    return None  # Retourne None si toutes les tentatives échouent

# Fonction principale pour traiter tous les CVs dans un dossier
def process_all_cvs(cv_folder, output_file):
    # Charger les résultats existants, s'ils existent
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
    else:
        results = []

    # Obtenir la liste des fichiers déjà traités
    processed_files = {result["file_name"] for result in results}

    # Obtenir la liste des CVs à traiter
    cv_files = [
        os.path.join(cv_folder, f)
        for f in os.listdir(cv_folder)
        if f.endswith(('.pdf', '.docx')) and os.path.basename(f) not in processed_files
    ]

    print(f"Nombre de CVs à traiter : {len(cv_files)}")

    for cv_file_path in cv_files:
        try:
            # Extraire le texte en fonction du format du fichier
            if cv_file_path.endswith(".docx"):
                cv_text = extract_text_from_docx(cv_file_path)
            elif cv_file_path.endswith(".pdf"):
                cv_text = extract_text_from_pdf(cv_file_path)
            else:
                print(f"Format de fichier non pris en charge : {cv_file_path}")
                continue

            # Pause pour respecter les quotas de l'API
            time.sleep(1)  # Pause de 10 secondes entre les appels

            # Analyser le texte avec Generative AI
            analysis = analyze_cv(cv_text)

            # Sauvegarder les résultats
            if analysis:
                results.append({
                    "file_name": os.path.basename(cv_file_path),
                    "cv_text": cv_text,  
                    "analysis": analysis
                })

                # Sauvegarder immédiatement après chaque traitement
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=4, ensure_ascii=False)

                print(f"Traitement terminé pour : {os.path.basename(cv_file_path)}")

        except Exception as e:
            print(f"Erreur lors du traitement du fichier {cv_file_path}: {str(e)}")
            continue

    print(f"Tous les CVs ont été traités. Résultats sauvegardés dans {output_file}")

# Dossier contenant les CVs
cv_folder = 'cv'  
output_file = 'cv2_fin.json'  # Nom du fichier de sortie

# Lancer le traitement
process_all_cvs(cv_folder, output_file)
