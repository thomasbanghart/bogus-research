from google.cloud import storage
import sys
import json
from datetime import datetime
from faker import Faker
import argparse
import ast
import random
import pprint as pp


upload = open(sys.argv[1], "r")
parsed = json.load(upload)

table_names = []
for field in parsed:
    if field["table_name"] in table_names:
        pass
    else:
        table_names.append(field["table_name"])


print(str(len(table_names)) + " tables in total.")

def getffktype(i, j):
    for field in parsed:
        if field["field_name"] == j and field["table_name"] == i:
            if field["dist"] == "fk":
                return getffktype(field["from"].split(".")[0], field["from"].split(".")[1])
            else:
                return field["fk_type"]

def getfbqtype(i, j):
    for field in parsed:
        if field["field_name"] == j and field["table_name"] == i:
            if field["dist"] == "fk":
                return getfbqtype(field["from"].split(".")[0], field["from"].split(".")[1])
            else:
                return field["bq_type"]

def getfargs(i, j):
    for field in parsed:
        if field["field_name"] == j and field["table_name"] == i:
            if field["dist"] == "fk":
                return getfargs(field["from"].split(".")[0], field["from"].split(".")[1])
            else:
                return field["fk_args"]


def upload_table(tablebogus, n, append, bogus):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
    fake = Faker()
    storage_client = storage.Client()
    bucket = storage_client.bucket("brick-layer-testing")
    outstring = ""
    for i in range(0, int(n)):
        populated = {}
        for record in tablebogus:
            # print(ast.literal_eval(record["args"]))
            if record["dist"] == "fk":
                record["fk_type"] = getffktype(record["from"].split(".")[0], record["from"].split(".")[1])
                record["bq_type"] = getfbqtype(record["from"].split(".")[0], record["from"].split(".")[1])
                record["fk_args"] = getfargs(record["from"].split(".")[0], record["from"].split(".")[1])
            if (record["fk_type"] == "past_datetime"):
                populated[record["field_name"]] = getattr(fake, record["fk_type"])(**ast.literal_eval(json.dumps(record["fk_args"]))).isoformat()
            elif (record["fk_type"] == "random_element"):
                populated[record["field_name"]] = random.choice(ast.literal_eval(json.dumps(record["fk_args"]["elements"][1:-1].split(", "))))
            elif (record["fk_type"] == "paragraph"):
                record["fk_args"] = {
                  "nb_sentences": 3,
                  "variable_nb_sentences": True
                }
                # print(json.dumps(record["fk_args"]).replace("'",'"').replace("'",'"').replace("'",'"').replace("'",'"').replace("true", "True"))
                populated[record["field_name"]] = getattr(fake, record["fk_type"])(**ast.literal_eval(json.dumps(record["fk_args"]).replace("'",'"').replace("'",'"').replace("'",'"').replace("'",'"').replace("true", "True")))
                # print(ast.literal_eval(record["args"])["elements"][1:-1].split(", "))
                # print(populated[record["name"]])
            elif (record["fk_type"] == "longitude" or record["fk_type"] == "latitude"):
                populated[record["field_name"]] = float(getattr(fake, record["fk_type"])(**ast.literal_eval(json.dumps(record["fk_args"]))))
            else:
                # print(record)
                populated[record["field_name"]] = getattr(fake, record["fk_type"])(**ast.literal_eval(json.dumps(record["fk_args"])))
        outstring += json.dumps(populated) + "\n"

    # purg_filename = tablebogus[0]['table_name']+datetime.now().isoformat()+".txt"
    # file = open(purg_filename, "w")
    # file.write(outstring)
    blob = bucket.blob(tablebogus[0]['table_name'])

    blob.upload_from_string(outstring)
    # os.remove(purg_filename)

    print(
        ''' 
            -> {} uploaded to The Cloud.

            '''.format(
            tablebogus[0]['dataset']+"."+tablebogus[0]['table_name']
        )
    )

    from google.cloud import bigquery
    client = bigquery.Client()
    dataset_ref = tablebogus[0]['dataset']

    ids = []
    for i in list(client.list_datasets()):
        ids.append(i.dataset_id)
    if dataset_ref in ids:
        dataset_ref = client.dataset(dataset_ref)
    else:
        dataset_ref = client.create_dataset(dataset_ref)  # Make an API request.
        # print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    print(" -> This is where I am :: " + tablebogus[0]['table_name'])
    # dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    sch_lst = []
    for field in tablebogus:
        sch_lst.append(bigquery.SchemaField(field['field_name'], field['bq_type']))
    if append:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = sch_lst
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = "gs://brick-layer-testing/"+tablebogus[0]['table_name']

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(tablebogus[0]['table_name']),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )  # API request
    print(" -> Starting to move shit over to BQ {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    # print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(tablebogus[0]['table_name']))
    print(" -> There are {} bogus rows.".format(destination_table.num_rows))

    blob = bucket.blob(tablebogus[0]['table_name'])
    blob.delete()
    print(" -> Tidying up...")
    # extract schema.json
    # make fakeout.json
    # upload to cloud storage
    # move from cloud storage to bq



if len(sys.argv) == 2:
    add_drop = input("bogus: add(a) / drop&replace(d)")
    if add_drop == "a":
        print("Adding to All")
        for table in table_names:
            package = []
            n = input("How many rows in {}? ".format(table))
            for field in parsed:
                if field["table_name"] == table:
                    package.append(field)
            upload_table(package, n, True, parsed)
        # upload each to cloud bucket
        # append to each TABLE_NAME (sys.arg[2])
    elif add_drop == "d":
        print("Dropping and Replacing All")
        for table in table_names:
            package = []
            for field in parsed:
                if field["table_name"] == table:
                    package.append(field)
            upload_table(package, n, False, parsed)
        # upload each to cloud bucket
        # drop & replace each TABLE_NAME (sys.arg[2])
    else:
        print("a or d")
elif len(sys.argv) == 3:
    add_drop = input("bogus: add / drop&replace")
    if add_drop == "a":
        print("Adding to " + sys.argv[2])
        package = []
        for field in parsed:
            if field["table_name"] == sys.argv[2]:
                package.append(field)
        n = input("How many rows? ")
        upload_table(package, n, True, parsed)
        # upload to cloud bucket
        # append to TABLE_NAME (sys.arg[2])
    elif add_drop == "d":
        print("Dropping and Replacing " + sys.argv[2])
        package = []
        for field in parsed:
            if field["table_name"] == sys.argv[2]:
                package.append(field)
        n = input("How many rows? ")
        upload_table(package, n, False, parsed)
        # upload to cloud bucket
        # drop & replace TABLE_NAME (sys.arg[2])
    else:
        print("a or d")
else:
    print("usage: python3 makedata.py block.bogus [ TABLE_NAME ] ")

