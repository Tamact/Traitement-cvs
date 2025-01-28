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

# 5. Fonction d'analyse via l'API
def analyze_cv(cv_text):
    """
    Envoie le texte à l'API Generative AI avec un prompt adapté,
    et gère le retry en cas de 429 (rate limit).
    """
    retries = 3
    delay = 10

    for attempt in range(retries):
        try:
            model = genai.GenerativeModel(
                model_name="gemini-1.5-flash",
                generation_config=generation_config,
            )

            # Préparation de l'historique pour le Chat
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
    "num_tel": "..."
    "profil": "..."
  },
  "cv": {
    "date_insertion": "YYYY-MM-DD",
    "cv_text": "...",
    "cv_pretraite": "...",
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
    "langues": [...]
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
- le texte entier du cv après un traitement (mise en minuscule, suppression des caractères spéciaux).
"""
                        ],
                    }
                ]
            )

            # Envoi du message final dans la conversation
            response = chat_session.send_message(
                "Génère uniquement ce JSON, sans balises de code. Pas besoin de phrases supplémentaires."
            )

            # Nettoyage de la chaîne brute
            response_text = response.text.strip()
            # Enlever les éventuelles balises ```...```
            response_text = re.sub(r"```(?:json)?(.*?)```", r"\1", response_text, flags=re.DOTALL)

            # Tentative de chargement en JSON
            try:
                return json.loads(response_text)
            except json.JSONDecodeError:
                # On retourne quand même le texte pour déboguer
                return response_text

        except Exception as e:
            # Gestion du quota (429)
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

# 6. Fonction pour traiter un CV
def process_cv_file(cv_file_path):
    """
    Lit le fichier CV, prétraite le texte, puis appelle l'API pour obtenir
    un JSON structuré. Retourne un dict { 'file_name': ..., 'analysis': ... }.
    """
    if cv_file_path.endswith(".docx"):
        cv_text = extract_text_from_docx(cv_file_path)
    elif cv_file_path.endswith(".pdf"):
        cv_text = extract_text_from_pdf(cv_file_path)
    else:
        # Format non pris en charge
        return None

    # Prétraitement
    preprocessed_text = preprocess_text(cv_text)
    # Appel à l'API
    analysis = analyze_cv(cv_text=preprocessed_text)  

    if analysis:
        return {
            "file_name": os.path.basename(cv_file_path),
            "analysis": analysis
        }
    return None

# 7. Fonction principale
def process_all_cvs(cv_folder, output_file):
    """
    - Charge les résultats existants.
    - Cherche les PDF/DOCX non encore traités.
    - Lance le traitement en parallèle.
    - Sauvegarde en une seule fois à la fin.
    """
    if not os.path.isdir(cv_folder):
        print(f"Erreur: Le dossier {cv_folder} n'existe pas.")
        return

    # On charge l'ancien JSON
    results = load_or_init_results(output_file)

    # Liste des fichiers déjà traités
    processed_files = {res["file_name"] for res in results}

    # Liste des CV à traiter
    cv_files = [
        os.path.join(cv_folder, f)
        for f in os.listdir(cv_folder)
        if f.endswith(('.pdf', '.docx')) and f not in processed_files
    ]

    print(f"Nombre de CVs à traiter : {len(cv_files)}")

    new_results = []
    # Ajustez max_workers selon vos ressources / quotas
    max_workers = 5

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
                    new_results.append(result)
                    print(f"Traitement terminé pour : {os.path.basename(cv_file_path)}")
            except Exception as e:
                print(f"Erreur dans le thread pour {cv_file_path}: {str(e)}")
                traceback.print_exc()

    # Fusion des anciens et nouveaux résultats
    results.extend(new_results)

    # Sauvegarde en un seul bloc
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print(f"Tous les CVs ont été traités. Résultats sauvegardés dans {output_file}")


if __name__ == "__main__":
    cv_folder = 'cv'
    output_file = 'cvs.json'
    process_all_cvs(cv_folder, output_file)
