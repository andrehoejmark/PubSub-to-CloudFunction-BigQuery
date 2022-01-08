import base64
from google.cloud import bigquery
import json

def hello_pubsub(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    #{"SearchedAt": "2018-12-19 09:26:03.478039", "SearchTerm": "playstation"}

    client = bigquery.Client()

    table_id = "CapStoneProject.Search-Terms-From-Cloud-Function"

    rows_to_insert= [
        json.loads(pubsub_message)
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))