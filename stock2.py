import psycopg2
import json
import uuid

def insert_data_from_json(json_file_path, connection_params):
    """
    Insère les données d'un fichier JSON dans une base de données PostgreSQL.

    Args:
        json_file_path (str): Le chemin vers le fichier JSON.
        connection_params (dict): Paramètres de connexion à la base de données.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        conn = psycopg2.connect(**connection_params)
        cur = conn.cursor()

        candidat_data = data.get("candidat", {})
        cv_data = data.get("cv", {})

        # Générer un UUID pour le candidat
        candidat_id = str(uuid.uuid4())
        print(candidat_id)
        # Insertion dans la table candidat
        cur.execute("""
            INSERT INTO candidat (id_candidat, nom_prenom, mail, profil)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (mail) DO UPDATE SET nom_prenom = EXCLUDED.nom_prenom
            RETURNING id_candidat;
        """, (candidat_id, candidat_data.get("nom_prenom"), candidat_data.get("mail")))
        
        # Récupérer l'id du candidat inséré (au cas où il existait déjà)
        candidat_id = cur.fetchone()[0]

        # Générer un UUID pour le CV
        cv_id = str(uuid.uuid4())

        # Insertion dans la table cv_analysis_cv
        cur.execute("""
            INSERT INTO cv_analysis (id_cv, id_candidat, date_insertion, cv_text, cv_pretraite, competences, experience, resume_cv, commitment, disponibilite, exp_salaire, domaine_etude, langues)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            cv_id,
            candidat_id,
            cv_data.get("date_insertion"),
            cv_data.get("cv_text"),
            cv_data.get("cv_pretraite"),
            json.dumps(cv_data.get("competences")),  # Convertir en JSON string
            json.dumps(cv_data.get("experience")), # Convertir en JSON string
            cv_data.get("resume_cv"),
            cv_data.get("commitment"),
            cv_data.get("disponibilite"),
            cv_data.get("exp_salaire"),
            cv_data.get("domaine_etude"),
            cv_data.get("langues")
        ))

        conn.commit()
        print("Données insérées avec succès.")

    except (Exception, psycopg2.Error) as error:
        print(f"Erreur lors de l'insertion des données : {error}")
        conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


# Exemple d'utilisation :
connection_params = {
    "host": "localhost",
    "database": "ujuzai_db",
    "user": "postgres",
    "password": "azerty12",
    "port": "5432"  
}

json_file_path = "cv2_fin.json" 

insert_data_from_json(json_file_path, connection_params)