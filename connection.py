import requests
import json

# Initialize the client
base_url = "https://2382a4e0-ae12-4ade-aa0d-bac6154e230f-us-east-2.apps.astra.datastax.com"
token = "AstraCS:IPPInHGKFbyYhBFDODKDBiep:dd273e0fad9aee0b4d2ec4c0a76312e605e460da11f54d33eab7ec036d4641f1"
headers = {
    'X-Cassandra-Token': token,
    'Content-Type': 'application/json'
}

# CQL query to insert data
cql_query = """
INSERT INTO robot.test (user_id, username, email, created_at)
VALUES (uuid(), 'john_doe', 'john@example.com', toTimestamp(now()));
"""

response = requests.post(
    f"{base_url}/api/rest/v2/keyspaces/robot/cql",
    headers=headers,
    json={"query": cql_query}
)

if response.status_code == 200:
    print("Data inserted successfully")
else:
    print("Failed to insert data")
    print(response.json())
