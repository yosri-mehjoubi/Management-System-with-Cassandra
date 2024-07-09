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

# Insert data
session.execute("""
    INSERT INTO users (user_id, name, age, email)
    VALUES (%s, %s, %s, %s)
""", (uuid.uuid4(), 'Alice', 30, 'alice@example.com'))

session.execute("""
    INSERT INTO users (user_id, name, age, email)
    VALUES (%s, %s, %s, %s)
""", (uuid.uuid4(), 'Bob', 25, 'bob@example.com'))
