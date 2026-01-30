"""
Every time data is re-ingested (after any sort of update to the  database or spread sheet), a new index has 
to be created and used. However, GLOBUS has a limit of three search indices, meaning you essentially need to 
delete an old index everytime you create a new one. This script can delete indices.
"""

import globus_sdk

# .secrets/globus_search_index format
"""
client_uuid adjsfalksjdfkl
client_id adjsfalksjdfkl@clients.auth.globus.org
secret xvjfklsdakljfe
"""

# Parsing secrets
client_uuid = ""
secret = ""
with open(".secrets/globus_search_index", "r") as f:
    secrets = f.readlines()
    client_uuid = secrets[0].split(' ')[1].strip('\n')
    secret = secrets[2].split(' ')[1].strip('\n')

CLIENT_ID = client_uuid      # from your .secrets file
CLIENT_SECRET = secret

authorizer = globus_sdk.ClientCredentialsAuthorizer(
    globus_sdk.ConfidentialAppAuthClient(CLIENT_ID, CLIENT_SECRET),
    globus_sdk.SearchClient.scopes.all,
)

sc = globus_sdk.SearchClient(authorizer=authorizer)

# Put index to delete in this variable
INDEX_ID = "024ff433-3dd2-496a-85b6-47f4b32e4d42"

print("Deleting index:", INDEX_ID)
sc.delete_index(INDEX_ID)
print("Index successfully deleted.")