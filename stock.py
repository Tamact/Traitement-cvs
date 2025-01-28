import json
import psycopg2
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="rhh",
    user="postgres",
    password="azerty12"
)
cursor = conn.cursor()

# Fonction pour prétraiter le texte des CVs
def preprocess_cv_text(text):
    
    text = text.lower()
   
    text = text.translate(str.maketrans("", "", string.punctuation))
    
    words = word_tokenize(text)
    
    words = [word for word in words if word not in stopwords.words('french')]
    return ' '.join(words)

# Charger le fichier JSON
with open('cv1_fin.json', 'r', encoding='utf-8') as file:
    cvs_data = json.load(file)

# Insérer les données dans les tables PostgreSQL
for cv in cvs_data:
    try:
        
        analysis = cv["analysis"]

       
        nom_prenom = analysis.split("**Nom:**")[1].split("\n")[0].strip()
        email = analysis.split("**Email:**")[1].split("\n")[0].strip()
        telephone = analysis.split("**Téléphone:**")[1].split("\n")[0].strip()

        
        competences_section = analysis.split("**Compétences:**")[1]
        competences = [
            line.strip("* ").strip()
            for line in competences_section.split("\n")
            if line.startswith("* ")
        ]

        
        cv_text = preprocess_cv_text(cv["cv_text"])
        
        # Insérer les données dans la table `candidat`
        cursor.execute("""
            INSERT INTO candidat (mail, numero_tlfn, nom_prenom)
            VALUES (%s, %s, %s)
            RETURNING user_id;
        """, (email, telephone, nom_prenom))
        user_id = cursor.fetchone()[0]

        
        cursor.execute("""
            INSERT INTO cv (user_id, cv_text, competences)
            VALUES (%s, %s, %s);
        """, (user_id, cv_text, competences))

        print(f"Insertion réussie pour : {nom_prenom}")

    except Exception as e:
        
        print(f"Erreur lors de l'insertion pour le fichier {cv.get('file_name', 'inconnu')}: {e}")
        conn.rollback()  
    else:
        conn.commit()  

# Fermer la connexion
cursor.close()
conn.close()
