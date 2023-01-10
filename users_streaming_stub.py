from google.cloud import bigquery

from datetime import datetime
import random

import uuid 
  

# Construct a BigQuery client object.
client = bigquery.Client()

table_id = "bq-insert-buffer.ds_streaming.tbl_users"


age=[25, 35, 45, 22, 42, 29, 39, 21, 36, 42, 19, 55, 37]
name=["Phred Phlyntstone", "Wylma Phlyntstone", "John Robers", "Biil Robers", "Donald John", "John Joe", "George Tom", "Kelly Robers", "Kathy Robers", "Marie John"]


for i in range(10):    
    id = str(uuid.uuid4())
    user_age=random.choice(age)
    user_name=random.choice(name)
    usrdatetime=str(datetime.now())

    rows_to_insert = [
        {u"id": id, u"full_name": user_name, u"age": user_age, u"datetime": usrdatetime}
    ]
    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.

if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))