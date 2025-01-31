import json
import uuid
import psycopg2
import psycopg2.extras
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

conn = psycopg2.connect(
    host="localhost",
    database="ujuzai_db",
    user="postgres",
    password="azerty12"
)
cursor = conn.cursor()

stop_words = set(stopwords.words('french'))

def preprocess_text(text):
    if not text:
        return ""
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in stop_words]
    return " ".join(tokens)

def insert_candidat(cursor, nom_prenom, mail, numero_tlfn, profil, code=""):
    # Vérifie si ce mail existe déjà
    cursor.execute("SELECT id_candidat FROM candidat WHERE mail = %s", (mail,))
    res = cursor.fetchone()
    if res:
        return res[0]  # Retourne l'id existant

    id_candidat = str(uuid.uuid4())
    # On tronque si nécessaire
    nom_prenom = nom_prenom[:255]
    mail = mail[:254]
    numero_tlfn = numero_tlfn[:20]
    profil = profil[:255]
    code = code[:255]

    cursor.execute("""
        INSERT INTO candidat
        (id_candidat, nom_prenom, mail, numero_tlfn, profil, code)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id_candidat;
    """, (id_candidat, nom_prenom, mail, numero_tlfn, profil, code))
    return cursor.fetchone()[0]

def insert_cv_analysis(cursor, id_candidat, cv_info, cv_vector):
    id_cv = str(uuid.uuid4())

    # Convertir competences, experience, langues et education en JSON (si dans la table, c'est jsonb)
    competences = cv_info.get("competences", [])
    experience = cv_info.get("experience", [])
    langues = cv_info.get("langues", [])
    education = cv_info.get("education", [])
    education_json = psycopg2.extras.Json(education)

    competences_json = psycopg2.extras.Json(competences)
    experience_json = psycopg2.extras.Json(experience)
    langues_json = psycopg2.extras.Json(langues)
    education_json = psycopg2.extras.Json(education)

    date_insertion = cv_info.get("date_insertion", "2025-01-01")
    raw_text = cv_info.get("cv_text", "")
    cv_pretraite = preprocess_text(raw_text)

    resume_cv = cv_info.get("resume_cv", "")
    if resume_cv is None:
        resume_cv = ""
    resume_cv = resume_cv[:255]

    commitment = cv_info.get("commitment", None)
    disponibilite = cv_info.get("disponibilite", None)
    exp_salaire = cv_info.get("exp_salaire", 0)
    domaine_etude = cv_info.get("domaine_etude", "")[:255]

    cursor.execute("""
        INSERT INTO cv_analysis (
            id_cv, date_insertion, cv_text, cv_pretraite, cv_vector,
            competences, experience, resume_cv, commitment, disponibilite,
            exp_salaire, domaine_etude, langue, id_candidat, education
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        id_cv,
        date_insertion,
        raw_text,
        cv_pretraite,
        cv_vector,  # double precision[]
        competences_json,
        experience_json,
        resume_cv,
        commitment,
        disponibilite,
        exp_salaire,
        domaine_etude,
        langues_json,
        id_candidat,
        education_json  # si education est un jsonb
        # OU si c'est un text[], alors education directement
    ))

with open("cvs.json", "r", encoding="utf-8") as f:
    cvs_data = json.load(f)

for cv_item in cvs_data:
    try:
        # Ex: votre item a la forme :
        # {
        #   "file_name": "MonCV.pdf",
        #   "cv_vector": [...],
        #   "candidat": {...},
        #   "cv": {...}
        # }

        candidat_info = cv_item.get("candidat", {})
        nom_prenom = candidat_info.get("nom_prenom", "")
        mail = candidat_info.get("mail", "")
        if not nom_prenom or not mail:
            raise ValueError("Nom/Prénom ou e-mail manquant")

        numero_tlfn = candidat_info.get("num_tel", "")
        profil = candidat_info.get("profil", "")

        id_candidat = insert_candidat(cursor, nom_prenom, mail, numero_tlfn, profil)

        cv_info = cv_item.get("cv", {})
        cv_vector = cv_item.get("cv_vector", [])
        if not isinstance(cv_vector, list):
            cv_vector = []

        insert_cv_analysis(cursor, id_candidat, cv_info, cv_vector)

        conn.commit()
        print(f"Insertion réussie pour {nom_prenom} (CV : {cv_item.get('file_name')})")

    except Exception as e:
        conn.rollback()
        print(f"Erreur pour {cv_item.get('file_name')}: {e}")

cursor.close()
conn.close()
