import io
import os
import json
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from flask import Flask, abort, render_template, request, send_file


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

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Ajouter un utilisateur
        name = request.form['name']
        age = int(request.form['age'])  # Convertir l'âge en entier
        email = request.form['email']
        user_id = uuid.uuid4()
        
        session.execute("""
            INSERT INTO users (user_id, name, age, email)
            VALUES (%s, %s, %s, %s)
        """, (user_id, name, age, email))
    
    # Afficher les utilisateurs
    rows = session.execute("SELECT user_id, name, age, email FROM users")
    users = []
    for row in rows:
        users.append({
            'user_id': row.user_id,
            'name': row.name,
            'age': row.age,
            'email': row.email
        })
    
    return render_template('index.html', users=users)




# partie d image 

from cassandra_image_handler import get_cassandra_session, create_keyspace_and_table, insert_image, retrieve_image

UPLOAD_FOLDER = 'static/uploads'
KEYSPACE = 'test2'

# Ensure the upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)




@app.route('/upload', methods=['POST'])
def upload_image():
    if 'file' not in request.files:
        return 'No file part', 400

    file = request.files['file']
    if file.filename == '':
        return 'No selected file', 400

    if file:
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(file_path)
        image_id = insert_image(session, KEYSPACE, file_path)
        return f'Image uploaded successfully with ID: {image_id}', 200
    

@app.route('/image', methods=['GET'])
def get_image():
    # Récupérer l'image_id depuis les paramètres de la requête GET
    image_id = request.args.get('image_id')
    print("Received GET request with image_id:", image_id)
    
    try:
        # Vérifier si l'image_id est un UUID valide
        image_uuid = uuid.UUID(image_id)
        print("Valid UUID:", image_uuid)
    except ValueError:
        # Si ce n'est pas un UUID valide, retourner une erreur 400
        abort(400, 'Invalid image ID')

    # Exécuter la requête CQL pour récupérer l'image
    row = session.execute("SELECT image_name, image_data FROM images WHERE image_id = %s", (image_uuid,)).one()

    if row:
        image_name = row.image_name
        image_data = row.image_data
        
        if image_data:
            # Retourner l'image si elle est trouvée
            return send_file(
                io.BytesIO(image_data),
                
                mimetype='image/jpeg',
                download_name=image_name
            )
        else:
            # Si l'image n'est pas trouvée, retourner une erreur 404
            abort(404, 'Image data not found')
    else:
        # Si l'image n'est pas trouvée, retourner une erreur 404
        abort(404, 'Image not found')
        
if __name__ == '__main__':
    app.run(debug=True)