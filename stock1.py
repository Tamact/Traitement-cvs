import json
import psycopg2

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="ujuzai_db",
    user="postgres",
    password="azerty12"
)
cursor = conn.cursor()

# Charger le fichier JSON (vous pouvez changer 'cvs_data.json' par le nom de votre fichier)
with open('cvs_fin.json', 'r', encoding='utf-8') as file:
    cvs_data = json.load(file)

# Insérer les données dans les tables PostgreSQL
for cv_data in cvs_data:
    try:
        if not isinstance(cv_data, dict):
            raise ValueError("cv_data doit être un dictionnaire")

        candidat_data = cv_data.get("candidat", {})
        if not isinstance(candidat_data, dict):
            raise ValueError("candidat_data doit être un dictionnaire")

        cv_data_content = cv_data.get("cv", {})
        if not isinstance(cv_data_content, dict):
            raise ValueError("cv_data_content doit être un dictionnaire")

        # Extraction des informations du candidat
        nom = candidat_data.get("nom_prenom")
        email = candidat_data.get("mail")
        telephone = candidat_data.get("numero_tlfn")
        profil = candidat_data.get("profil")

        # Vérification des données avant insertion
        if not nom or not email or not telephone or not profil:
            raise ValueError("Les informations du candidat ne peuvent pas être vides.")

        # Insertion dans la table 'candidat'
        cursor.execute("""
            INSERT INTO candidat (nom_prenom, mail, numero_tlfn, profil)
            VALUES (%s, %s, %s, %s)
            RETURNING id_candidat;
        """, (nom, email, telephone, profil))

        id_candidat = cursor.fetchone()
        if id_candidat is None:
            raise ValueError("Aucun id_candidat retourné")

        id_candidat = id_candidat[0]

        # Extraction des informations du cv
        date_insertion = cv_data_content.get("date_insertion")
        cv_text = cv_data_content.get("cv_text")
        cv_pretraite = cv_data_content.get("cv_pretraite")
        competences = cv_data_content.get("competences")
        experience = cv_data_content.get("experience")
        resume_cv = cv_data_content.get("resume_cv")
        commitment = cv_data_content.get("commitment")
        disponibilite = cv_data_content.get("disponibilite")
        exp_salaire = cv_data_content.get("exp_salaire")
        domaine_etude = cv_data_content.get("domaine_etude")
        langues = cv_data_content.get("langues")

        # Insertion dans la table 'cv'
        cursor.execute("""
            INSERT INTO cv (id_candidat, date_insertion, cv_text, cv_pretraite, competences, experience, resume_cv, commitment, disponibilite, exp_salaire, domaine_etude, langues)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (id_candidat, date_insertion, cv_text, cv_pretraite, json.dumps(competences), json.dumps(experience), resume_cv, commitment, disponibilite, exp_salaire, domaine_etude, json.dumps(langues)))

        print(f"Insertion réussie pour : {nom}")

    except Exception as e:
        print(f"Erreur lors de l'insertion pour le cv de {nom if nom else 'inconnu'}: {e}")
        conn.rollback()
    else:
        conn.commit()

# Fermer la connexion
cursor.close()
conn.close()