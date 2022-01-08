import os
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import time
import threading
from google.cloud import pubsub_v1
import json


credentials_path = "C:/Users/andrehoejmark/Desktop/GCP/CapStone Project/testing.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
publisher = pubsub_v1.PublisherClient()
topic_path = "projects/ultra-unfolding-335012/topics/search-term-topic"

client = bigquery.Client()


# Send message
attributes = {
        "SearchTerm": "playstation",
        "SearchedAt": "2022-12-19 09:26:03.478039"
}   

data = json.dumps(attributes).encode('utf-8')
future = publisher.publish(topic_path, data)

time_at_message_sent = time.time()



# Reading the database until new insert found
rows = []
times_since_start = []


def get_rows():
    # The database for cloud function
    # query = ("SELECT * FROM `CapStoneProject.Search-Terms-From-Cloud-Function` ")

    # The database for dataflow
    query = ("SELECT * FROM `CapStoneProject.Search-Terms-From-Dataflow` ")
    
    query_job = client.query(
            query,
            location="europe-west3",
        )

    t0 = time.time()

    results = query_job.result()

    row_count = results.total_rows
    delta_t = time.time() - time_at_message_sent

    print("Query time: " + str(time.time() -t0) + "s")

    rows.append(row_count)
    times_since_start.append(delta_t)


threads = []
for i in range(100):
    thread = threading.Thread(target=get_rows)
    thread.start()
    threads.append(thread)
    time.sleep(0.1)

# Makes sure that all threads are done before continuing. Probably could have been added after t1.start also.
for thread in threads:
    thread.join()


dictionary = {"row_count": rows,
              "delta_time": times_since_start}


df = pd.DataFrame(dictionary)


df.to_csv('speed_of_dataflow10.csv', sep=",")
print('done')




