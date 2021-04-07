# -*- coding: utf-8 -*-

import settings
import os
import subprocess
from datetime import datetime
from datetime import timedelta  
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(
    settings.key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


def prepare_dataset(dataset_name, table_name):
    print("Preparing dataset...")
    try:
        dataset = bigquery_client.get_dataset(dataset_name)
    except NotFound:
        datasetObj = bigquery_client.dataset(dataset_name)
        datasetObj.location = "US"
        datasetObj.default_table_expiration_ms = 1440000
        dataset = bigquery_client.create_dataset(datasetObj)

    try:
        table = bigquery_client.get_table(dataset.table(table_name))
        #table_ref = client.dataset(dataset_id).table(table_id)
        bigquery_client.delete_table(table)  # API request
    except NotFound:
        pass

    tableref = dataset.table(table_name)
    table = bigquery.Table(tableref)
    table.expires = datetime.now() + timedelta(days=1)
    bigquery_client.create_table(table)


def load_file(filename, dataset_name, table_name):
    #remove gz if exists
    gzfilename = "{filename}.gz".format(filename=filename)
    gzfilename = os.path.join(settings.dumps, gzfilename)
    if os.path.exists(gzfilename):
        try:
            os.remove(gzfilename)
        except Exception as e:
            print(e)
            print("Error: File target {gzfilename} cannot be removed".format(file=gzfilename))
            return False

    print("Compressing file in gzip format...")

    cmd = "gzip {filename}".format(filename=os.path.join(settings.dumps, filename))
    proc = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    proc.wait()
    stdout_value, stderr_value = proc.communicate()
        
    if len(stdout_value) != 0:
        print(stdout_value)
        sys.exit(0)

    print("Uploading file into bigquery...")
    dataset = bigquery_client.get_dataset(dataset_name)
    table = bigquery_client.get_table(dataset.table(table_name))
    
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open(gzfilename, 'rb') as source_file:
        job = bigquery_client.load_table_from_file(
            source_file,
            table,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))
