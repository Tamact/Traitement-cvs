import json
import uuid
import string
from supabase import create_client, Client
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from dotenv import load_dotenv
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

stop_words = set(stopwords.words('french'))

def preprocess_text(text):
    if not text:
        return ""
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in stop_words]
    return " ".join(tokens)

def get_or_create_candidat(supabase: Client, nom_prenom: str, mail: str, numero_tlfn: str, profil: str, code=""):
    """
    Vérifie si le candidat existe (via mail). 
    Sinon, l'insère et retourne son id_candidat.
    """

    response = supabase.table("candidat").select("id_candidat").eq("mail", mail).execute()

    data = response.data
    if data:
        # Candidat déjà présent => on retourne son id
        return data[0]["id_candidat"]

    # 2) Sinon, on insère un nouveau candidat
    id_candidat = str(uuid.uuid4())

    # Tronquer si besoin
    nom_prenom = nom_prenom[:255]
    mail = mail[:254]
    numero_tlfn = numero_tlfn[:20]
    profil = profil[:255]
    code = code[:255]

    insert_data = {
        "id_candidat": id_candidat,
        "nom_prenom": nom_prenom,
        "mail": mail,
        "numero_tlfn": numero_tlfn,
        "profil": profil,
        "code": code,
    }

    supabase.table("candidat").insert(insert_data).execute()
    return id_candidat

def insert_cv_analysis(supabase: Client, id_candidat: str, cv_info: dict, cv_vector: list):
    # Générer un uuid pour le CV
    id_cv = str(uuid.uuid4())

    competences = cv_info.get("competences", [])
    experience = cv_info.get("experience", [])
    langues = cv_info.get("langues", [])
    education = cv_info.get("education", [])

    # date par défaut si pas présente
    date_insertion = cv_info.get("date_insertion", "2025-01-01")
    raw_text = cv_info.get("cv_text", "")
    cv_pretraite = preprocess_text(raw_text)

    resume_cv = cv_info.get("resume_cv", "") or ""
    resume_cv = resume_cv[:255]

    commitment = cv_info.get("commitment", None)
    disponibilite = cv_info.get("disponibilite", None)
    exp_salaire = cv_info.get("exp_salaire", 0)
    domaine_etude = cv_info.get("domaine_etude", "")[:255]

    insert_data = {
        "id_cv": id_cv,
        "date_insertion": date_insertion,
        "cv_text": raw_text,
        "cv_pretraite": cv_pretraite,
        "cv_vector": cv_vector,  # On suppose que votre colonne est de type array ou json
        "competences": competences,
        "experience": experience,
        "resume_cv": resume_cv,
        "commitment": commitment,
        "disponibilite": disponibilite,
        "exp_salaire": exp_salaire,
        "domaine_etude": domaine_etude,
        "langue": langues,
        "education": education,
        "id_candidat": id_candidat,
    }

    # Insertion dans la table "cv_analysis"
    supabase.table("cv_analysis").insert(insert_data).execute()

# === Lecture du fichier JSON et insertion ===
with open("cv1.json", "r", encoding="utf-8") as f:
    cvs_data = json.load(f)

for cv_item in cvs_data:
    try:
        candidat_info = cv_item.get("candidat", {})
        nom_prenom = candidat_info.get("nom_prenom", "")
        mail = candidat_info.get("mail", "")
        if not nom_prenom or not mail:
            raise ValueError("Nom/Prénom ou e-mail manquant")

        numero_tlfn = candidat_info.get("num_tel", "")
        profil = candidat_info.get("profil", "")

        # 1) Récupérer (ou créer) l'id du candidat
        id_candidat = get_or_create_candidat(supabase, nom_prenom, mail, numero_tlfn, profil)

        # 2) Insérer l'analyse du CV
        cv_info = cv_item.get("cv", {})
        cv_vector = cv_item.get("cv_vector", [])
        if not isinstance(cv_vector, list):
            cv_vector = []

        insert_cv_analysis(supabase, id_candidat, cv_info, cv_vector)

        print(f"Insertion réussie pour {nom_prenom} (CV : {cv_item.get('file_name')})")

    except Exception as e:
        print(f"Erreur pour {cv_item.get('file_name')}: {e}")
