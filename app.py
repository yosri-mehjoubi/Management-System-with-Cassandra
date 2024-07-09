import io
import os
import json
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from flask import Flask, abort, flash, redirect, render_template, request, send_file, url_for
import base64  # Ajout de l'importation de base64


app = Flask(__name__)


# Get the absolute path to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Specify the absolute paths to the secure connect bundle and token JSON file
secure_connect_bundle_path = os.path.join(script_dir, 'secure-connect-test2.zip')
token_file_path = os.path.join(script_dir, 'test2-token.json')

# Verify the paths
print(f"Secure connect bundle path: {secure_connect_bundle_path}")
print(f"Token file path: {token_file_path}")

# Check if the files exist
if not os.path.exists(secure_connect_bundle_path):
    raise FileNotFoundError(f"Secure connect bundle not found at {secure_connect_bundle_path}")
if not os.path.exists(token_file_path):
    raise FileNotFoundError(f"Token file not found at {token_file_path}")

# Load secrets from token JSON file
with open(token_file_path) as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cloud_config = {
    'secure_connect_bundle': secure_connect_bundle_path
}
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

session = cluster.connect("test2")

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")


keyspace="test2"


# Use the keyspace
session.set_keyspace(keyspace)

# Create table
session.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY,
        name TEXT,
        age INT,
        email TEXT
    )
""")

session.execute("""
        CREATE TABLE IF NOT EXISTS images (
            image_id UUID PRIMARY KEY,
            image_name TEXT,
            image_data BLOB
        )
    """)


# # Insert data
# session.execute("""
#     INSERT INTO users (user_id, name, age, email)
#     VALUES (%s, %s, %s, %s)
# """, (uuid.uuid4(), 'Alice', 30, 'alice@example.com'))

# session.execute("""
#     INSERT INTO users (user_id, name, age, email)
#     VALUES (%s, %s, %s, %s)
# """, (uuid.uuid4(), 'Bob', 25, 'bob@example.com'))

# session.execute("""
#     INSERT INTO users (user_id, name, age, email)
#     VALUES (%s, %s, %s, %s)
# """, (uuid.uuid4(), 'yopi', 25, 'yupi@example.com'))





# partie d image 

from cassandra_image_handler import get_cassandra_session, create_keyspace_and_table, insert_image, retrieve_image

app = Flask(__name__)

app.secret_key = 'supersecretkey'  # Nécessaire pour utiliser flash messages
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')

KEYSPACE = 'test2'
app.config['UPLOAD_FOLDER'] = 'uploads'  # Remplacez 'uploads' par le dossier de votre choix

# Définition des routes
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'name' in request.form:
            # Ajouter un utilisateur
            name = request.form['name']
            age = int(request.form['age'])  # Convertir l'âge en entier
            email = request.form['email']
            user_id = uuid.uuid4()

            if 'file' in request.files:
                # Upload d'une image pour l'utilisateur
                file = request.files['file']
                if file.filename == '':
                    flash('No selected file')
                    return redirect(url_for('index'))

                if file:
                    # Assurez-vous que le dossier 'uploads' existe
                    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

                    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
                    file.save(file_path)
                    image_id = insert_image(session, file_path)

                    session.execute("""
                        INSERT INTO users (user_id, name, age, email, image_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (user_id, name, age, email, image_id))
                    flash('User and image added successfully!')
            else:
                session.execute("""
                    INSERT INTO users (user_id, name, age, email)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, name, age, email))
                flash('User added successfully!')

        return redirect(url_for('index'))

    # Récupérer les utilisateurs
    user_rows = session.execute("SELECT user_id, name, age, email, image_id FROM users")
    users = []
    for user_row in user_rows:
        user = {
            'user_id': user_row.user_id,
            'name': user_row.name,
            'age': user_row.age,
            'email': user_row.email,
            'image_data': None
        }

        if user_row.image_id:
            # Récupérer l'image associée
            image_row = session.execute("SELECT image_data FROM images WHERE image_id = %s", (user_row.image_id,)).one()
            if image_row:
                user['image_data'] = base64.b64encode(image_row.image_data).decode('utf-8')

        users.append(user)

    return render_template('index.html', users=users)


def insert_image(session, file_path):
    # Fonction d'insertion d'image dans la base de données Cassandra
    image_id = uuid.uuid4()
    with open(file_path, 'rb') as f:
        image_data = f.read()

    session.execute("""
        INSERT INTO images (image_id, image_name, image_data)
        VALUES (%s, %s, %s)
    """, (image_id, os.path.basename(file_path), image_data))

    return image_id

@app.route('/delete_user/<user_id>', methods=['POST'])
def delete_user(user_id):
    # Supprimer l'utilisateur correspondant à user_id
    session.execute("DELETE FROM users WHERE user_id = %s", (uuid.UUID(user_id),))
    return "User deleted successfully", 200

# Créer l'index sur la colonne name
# session.execute("CREATE INDEX IF NOT EXISTS ON users (name)")
@app.route('/search_user', methods=['GET'])
def search_user():
    search_query = request.args.get('q', '')

    # Utiliser une requête préparée pour la recherche
    rows = session.execute(
        """
        SELECT user_id, name, age, email, image_id 
        FROM users
        WHERE name = %s
        """,
        (search_query,)
    )

    search_results = []
    for row in rows:
        user = {
            'user_id': row.user_id,
            'name': row.name,
            'age': row.age,
            'email': row.email,
            'image_id': row.image_id
        }
        search_results.append(user)

    for user in search_results:
        image_row = session.execute(
            """
            SELECT image_data, image_name
            FROM images
            WHERE image_id = %s
            """,
            (user['image_id'],)
        ).one()

        if image_row:
            user['image_data'] = base64.b64encode(image_row.image_data).decode('utf-8')
            user['image_name'] = image_row.image_name
        else:
            user['image_data'] = None
            user['image_name'] = None

    # Récupérer tous les utilisateurs pour afficher le tableau initial
    all_users = []
    rows = session.execute("SELECT * FROM users")
    for row in rows:
        user = {
            'user_id': row.user_id,
            'name': row.name,
            'age': row.age,
            'email': row.email,
            'image_id': row.image_id,
        }
        image_row = session.execute(
            """
            SELECT image_data
            FROM images
            WHERE image_id = %s
            """,
            (user['image_id'],)
        ).one()
        if image_row:
            user['image_data'] = base64.b64encode(image_row.image_data).decode('utf-8')
        else:
            user['image_data'] = None
        all_users.append(user)

    return render_template('index.html', users=all_users, search_results=search_results)



# Route pour traiter les modifications de l'utilisateur
@app.route('/update_user/<uuid:user_id>', methods=['POST'])
def update_user(user_id):
    name = request.form['name']
    age = request.form['age']
    email = request.form['email']
    file = request.files['file']
    try:
        
        age = int(age)
    except ValueError:
        return "Age must be an integer", 400

    image_id = None
    if file:
        image_data = file.read()
        image_id = uuid.uuid4()  # Générer un UUID pour l'image
        session.execute(
            "INSERT INTO images (image_id, image_data, image_name) VALUES (%s, %s, %s)",
            (image_id, image_data, file.filename)
        )

    if image_id:
        session.execute(
            "UPDATE users SET name = %s, age = %s, email = %s, image_id = %s WHERE user_id = %s",
            (name, age, email, image_id, user_id)
        )
    else:
        session.execute(
            "UPDATE users SET name = %s, age = %s, email = %s WHERE user_id = %s",
            (name, age, email, user_id)
        )

    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True, host='192.168.56.1', port=5000)
