import json
import uuid
import psycopg2
import psycopg2.extras
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import os
# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("HOST"),
    database=os.getenv("DATABASE"),
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD")
)
cursor = conn.cursor()

# Préparer les stopwords pour le prétraitement
stop_words = set(stopwords.words('french'))

# Fonction pour prétraiter le texte
def preprocess_text(text):
    if not text:  # Si le texte est None ou vide
        return ""
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))  # Retirer la ponctuation
    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in stop_words]  # Retirer les stopwords
    return " ".join(tokens)

# Charger les données JSON
with open('cvs.json', 'r', encoding='utf-8') as file:
    cvs_data = json.load(file)

# Fonction pour insérer un candidat (en vérifiant les doublons)
def insert_candidat(cursor, nom_prenom, mail, numero_tlfn, profil, code=""):
    # Vérifier si le candidat existe déjà (mail unique)
    cursor.execute("""
        SELECT id_candidat FROM public.candidat WHERE mail = %s
    """, (mail,))
    result = cursor.fetchone()

    if result:
        return result[0]  # Renvoyer l'ID existant

    # Générer un UUID pour le candidat
    id_candidat = str(uuid.uuid4())

    # Insérer un nouveau candidat
    cursor.execute("""
        INSERT INTO public.candidat (id_candidat, nom_prenom, mail, numero_tlfn, profil, code)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id_candidat;
    """, (id_candidat, nom_prenom[:255], mail, numero_tlfn[:20], profil[:255], code))
    
    return cursor.fetchone()[0]  # Retourne l'ID nouvellement inséré

# Fonction pour insérer un CV
def insert_cv_analysis(cursor, id_candidat, cv_info, cv_vector):
    # Générer un UUID pour le CV
    id_cv = str(uuid.uuid4())

    # Préparer les données JSONB pour PostgreSQL
    competences_json = psycopg2.extras.Json(cv_info.get("competences", []))
    experience_json = psycopg2.extras.Json(cv_info.get("experience", []))
    langues_json = psycopg2.extras.Json(cv_info.get("langues", []))

    # Prétraiter le texte brut du CV
    cv_text = cv_info.get("cv_text", "")  # On utilise directement cv_text
    cv_pretraite = preprocess_text(cv_text)

    # Insérer les données dans la table cv_analysis
    cursor.execute("""
        INSERT INTO public.cv_analysis (
            id_cv, date_insertion, cv_text, cv_pretraite, cv_vector,
            competences, experience, resume_cv, commitment, disponibilite,
            exp_salaire, domaine_etude, langue, id_candidat, education
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        id_cv,
        cv_info.get("date_insertion", "2025-01-01"),  # Date par défaut
        cv_text,                  # Texte brut du CV
        cv_pretraite,             # Texte prétraité
        cv_vector,                # Liste de floats (double precision[])
        competences_json,
        experience_json,
        cv_info.get("resume_cv", "")[:255],  # Limite à 255 caractères
        cv_info.get("commitment", None),
        cv_info.get("disponibilite", None),
        cv_info.get("exp_salaire", 0),
        cv_info.get("domaine_etude", "")[:255],  # Limite à 255 caractères
        langues_json,
        id_candidat,
        cv_info.get("education", [])  # Liste de strings (text[])
    ))

# Parcourir les CVs dans le JSON et insérer dans la base
for cv_item in cvs_data:
    try:
        # 1. Récupérer les informations du candidat
        candidat_info = cv_item.get("candidat", {})
        nom_prenom = candidat_info.get("nom_prenom", "")
        mail = candidat_info.get("mail", "")
        numero_tlfn = candidat_info.get("num_tel", "")
        profil = candidat_info.get("profil", "")

        # Valider les champs critiques
        if not nom_prenom or not mail:
            raise ValueError("Nom/Prénom ou e-mail manquant dans le CV.")

        # 2. Insérer le candidat (ou récupérer son ID)
        id_candidat = insert_candidat(cursor, nom_prenom, mail, numero_tlfn, profil)

        # 3. Récupérer les informations du CV
        cv_info = cv_item.get("cv", {})
        cv_vector = cv_item.get("cv_vector", [])

        # Valider le vecteur (doit être une liste de floats)
        if not isinstance(cv_vector, list):
            raise ValueError("cv_vector doit être une liste de nombres.")

        # 4. Insérer le CV
        insert_cv_analysis(cursor, id_candidat, cv_info, cv_vector)

        # Commit après insertion réussie
        conn.commit()
        print(f"Insertion réussie pour {nom_prenom} (CV : {cv_item.get('file_name', '')})")

    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de l'insertion du CV {cv_item.get('file_name', '')} : {e}")

# Fermeture de la connexion
cursor.close()
conn.close()
