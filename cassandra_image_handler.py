import os
import uuid
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def get_cassandra_session():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secure_connect_bundle_path = os.path.join(script_dir, 'secure-connect-test2.zip')
    token_file_path = os.path.join(script_dir, 'test2-token.json')

    if not os.path.exists(secure_connect_bundle_path):
        raise FileNotFoundError(f"Secure connect bundle not found at {secure_connect_bundle_path}")
    if not os.path.exists(token_file_path):
        raise FileNotFoundError(f"Token file not found at {token_file_path}")

    with open(token_file_path) as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
    cloud_config = {
        'secure_connect_bundle': secure_connect_bundle_path
    }
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    return session

def create_keyspace_and_table(session, keyspace):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(keyspace)
    session.execute("""
        CREATE TABLE IF NOT EXISTS images (
            image_id UUID PRIMARY KEY,
            image_name TEXT,
            image_data BLOB
        )
    """)

def insert_image(session, keyspace, image_path):
    session.set_keyspace(keyspace)
    with open(image_path, 'rb') as f:
        image_data = f.read()

    image_id = uuid.uuid4()
    image_name = os.path.basename(image_path)

    session.execute("""
        INSERT INTO images (image_id, image_name, image_data)
        VALUES (%s, %s, %s)
    """, (image_id, image_name, image_data))

    return image_id

def retrieve_image(session, image_id):
    session.set_keyspace('test2')
    row = session.execute("SELECT image_name, image_data FROM images WHERE image_id = %s", (uuid.UUID(image_id),)).one()

    if row:
        image_name = row.image_name
        image_data = row.image_data
        return image_name, image_data
    else:
        return None, None