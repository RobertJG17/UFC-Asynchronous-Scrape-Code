import pandas as pd
from time import time
import json



def hello_gcs(event, context):
  print(event["name"])
  if event["name"] == "stats.csv":
    start = time()

    fighters = pd.read_csv('gs://ufc-csv-data/fighters.csv')
    stats = pd.read_csv('gs://ufc-csv-data/stats.csv')
    merged = fighters.merge(stats, left_on="name", right_on="Fighter")
    merged.to_csv('gs://ufc-csv-data/merged.csv')

    end = time()

    return json.dumps({
        "Succesful": f"Asynchronous html allocation and synchronous parsing took {end - start} seconds"
    })
  
  return json.dumps({
    "Message": "Event Detected"
  })