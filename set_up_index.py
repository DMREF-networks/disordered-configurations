"""
This script takes the data from ingest_data.json and creates a new index with it, saving it to the index_id textfile.

If a new index needs to be created, be sure to delete the index_id textfile. Otherwise, this script will not
detect the lack on an index_id. Once the new index has been created, make sure to put it in the static.json file 
so that the correct search results will show up in the searchable database.  
"""

import globus_sdk
from globus_sdk import ConfidentialAppAuthClient, ClientCredentialsAuthorizer
import argparse
import json

# COMMAND TO RUN: python3 set_up_index.py -c .secrets/globus_search_index (you have to run from inside the directory)

parser = argparse.ArgumentParser()
parser.add_argument("--cred", "-c", help="Path to Globus project service account credentials")
parser.add_argument("--index", "-i", default="index_id", help="Path to Globus Search Index ID")

args = parser.parse_args()


def get_index(index_path):
    """Read index ID from a file"""

    with open(index_path, "r") as r:
        return r.readline().strip()

def save_index(index_path, index_id):
    """Save index ID to a text file"""

    with open(index_path, "w") as f:
        f.write(index_id)

    return None

def get_client_authorizer(CLIENT_ID, CLIENT_SECRET):
    """Use service account credentials to authenticate and authorize client"""
    scopes = globus_sdk.SearchClient.scopes.all
    client = ConfidentialAppAuthClient(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    return ClientCredentialsAuthorizer(client, scopes)

def create_index(search_client, name, description):
    """Using a Globus Search Client, create an index with a certain name and description"""

    r = search_client.create_index(name, description)

    return r['id']


if __name__=="__main__":

    # read credentials
    auth_info = {}
    with open(args.cred, "r") as r:
        for line in r.readlines():
            line = line.strip()
            parts = line.split(" ")
            auth_info[parts[0]] = parts[1]

    authorizer = get_client_authorizer(auth_info["client_uuid"], auth_info["secret"])
    sc = globus_sdk.SearchClient(authorizer=authorizer)

    try: # if there's an index, read the index and ingest the data into the index
        index_id = get_index(args.index)
        sc.get_index(index_id)

        with open("ingest_data.json") as f:
            ingest_data = json.load(f)
        
        # injest data
        sc.ingest(index_id, ingest_data)

    except: # if there's no index, create a new one
        index_id = create_index(sc,"RFS Test Search Index", "Just like the name says")
        save_index(args.index, index_id)

    # Ingest new data into the index
    # ingest_data - assign to a json file that includes a file

    
