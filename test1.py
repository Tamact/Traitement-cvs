import os
import json
import PyPDF2
import google.generativeai as genai
from dotenv import load_dotenv
import time
import traceback
from docx import Document
from concurrent.futures import ThreadPoolExecutor
import re


# Charger la clé API depuis le fichier .env
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

# Check API key is defined
if not API_KEY:
    print("Erreur: Clé API Gemini non trouvée. Veuillez configurer la variable d'environnement GEMINI_API_KEY.")
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

# Fonction pour extraire le texte d'un fichier DOCX
def extract_text_from_docx(file_path):
    document = Document(file_path)
    return "\n".join([paragraph.text for paragraph in document.paragraphs])

# Fonction pour extraire le texte d'un fichier PDF
def extract_text_from_pdf(file_path):
    with open(file_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        return "\n".join(page.extract_text() for page in reader.pages)

def preprocess_text(text):
    # Conversion en minuscule
    text = text.lower()
    # Suppression des caractères spéciaux
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return text

# Fonction pour analyser un CV avec Generative AI
def analyze_cv(cv_text):
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
                             {'texte_cv_pretraitement': '...', 
                              'resume': '...',
                              'domaine_etude': '...',
                              'annees_experience': '...',
                              'nom': '...',
                              'email': '...',
                              'telephone': '...', 
                              'competences': ['...', '...']
                              'profil':'...'
                              'langue':'...'}
                              'education':['...', '...']
                            """,
                        ],
                    }
                ]
            )

            response = chat_session.send_message(
                "Fournis les informations demandées en un format structuré. Pas besoin de faire des commentaires. Donne juste les infos demandées. Il ne se peut que ca soit une image ou que les écriture ne soit pas visible ou que ca soit difficile de récupérer les infos mais tu dois essayer. J'ai vu que tu mets à chaque fois json dans le 'analysis'. Ne mets pas ca dans le json"
                
            )

            # Nettoyage de la chaîne brute
            response_text = response.text.strip()
            # Si le modèle renvoie des balises de code (```json ... ```), on les supprime
            response_text = re.sub(r"```(?:json)?(.*?)```", r"\1", response_text, flags=re.DOTALL)
            
            try:
                return json.loads(response_text)  
            except json.JSONDecodeError:
                return response_text 

        except Exception as e:
            if "429" in str(e):
                print(f"Tentative {attempt + 1} échouée : Limite atteinte, nouvelle tentative dans {delay} secondes.")
                time.sleep(delay)
                delay *= 2
            else:
                 print(f"Erreur lors de l'analyse du CV : {str(e)} \n {traceback.format_exc()}")
                 break

    return None

# Fonction principale pour traiter tous les CVs dans un dossier
def process_all_cvs(cv_folder, output_file):
    # Verify input directory exists
    if not os.path.isdir(cv_folder):
        print(f"Erreur: Le dossier {cv_folder} n'existe pas.")
        return

    # Charger les résultats existants, s'ils existent
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            try:
               results = json.load(f)
            except json.JSONDecodeError:
                print("Warning : Invalid JSON found in output file, creating a new one")
                results = []
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

    with ThreadPoolExecutor(max_workers=5) as executor:  
        
        futures = []
        for cv_file_path in cv_files:
          future = executor.submit(process_single_cv, cv_file_path, results, output_file)
          futures.append(future)

        for future in futures:
            try:
                result = future.result()
                if result:
                    results = result
            except Exception as e:
                 print(f"An error occurred in thread: {e}")

    print(f"Tous les CVs ont été traités. Résultats sauvegardés dans {output_file}")


def process_single_cv(cv_file_path, results, output_file):
    try:
        if cv_file_path.endswith(".docx"):
            cv_text = extract_text_from_docx(cv_file_path)
        elif cv_file_path.endswith(".pdf"):
            cv_text = extract_text_from_pdf(cv_file_path)
        else:
            print(f"Format de fichier non pris en charge : {cv_file_path}")
            return results  # return results as no new results are generated

         # Preprocess text
        preprocessed_text = preprocess_text(cv_text)

        # Pause to respect API limit
        time.sleep(1)

        # Analyze text
        analysis = analyze_cv(preprocessed_text)

        # Save results
        if analysis:
          results.append({
                "file_name": os.path.basename(cv_file_path),
                "cv_text": cv_text,
                "analysis": analysis,
            })

          # Save results to file directly after each process
          with open(output_file, 'w', encoding='utf-8') as f:
               json.dump(results, f, indent=4, ensure_ascii=False)


          print(f"Traitement terminé pour : {os.path.basename(cv_file_path)}")
        return results

    except Exception as e:
        print(f"Erreur lors du traitement du fichier {cv_file_path}: {str(e)} \n {traceback.format_exc()}")
        return results # Return results if an error occurs

# Dossier contenant les CVs
cv_folder = 'CVs_telecharges'
output_file = 'cvs_fin.json'

# Lancer le traitement
process_all_cvs(cv_folder, output_file)