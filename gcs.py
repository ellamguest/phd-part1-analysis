#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from tools import credentialsPath, cachePath
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage


"""IMPORTING DATA FROM GOOGLE CLOUD STORAGE"""
storage_client = lambda: storage.Client.from_service_account_json('service_account.json')

def fetchBlob(date):
    client = storage.Client.from_service_account_json('service_account.json')
    bucket = client.get_bucket('emg-author-subreddit-pairs')
    blob = bucket.blob(f"""{date}.gzip""")
    return blob

download_blob = lambda blob, date: blob.download_to_filename(cachePath(f"""{date}/author-subreddit-pairs.gzip"""))
read_blob = lambda date: pd.read_csv(cachePath(f"""{date}/author-subreddit-pairs.gzip"""), compression="gzip")


s3_bucket = f"""s3://emg-phd-part1"""
gs_bucket = f"""gs://emg-author-subreddit-pairs"""

def rsyncCloudStorage(origin, destination):
    s3_bucket = f"""s3://emg-phd-part1"""
    gs_bucket = f"""gs://emg-author-subreddit-pairs"""
    
    
    origin = f"""gs://emg-author-subreddit-pairs/{date}.gzip"""
    destination = f"""s3://emg-phd-part1/{date}.gzip"""
    f"""gsutil cp {origin} {destination}"""





"""COLLECTING DATA FROM GOOGLE BIGQUERY"""
def client():
    """REQUIRES A FILE CALLED 'bigquery.json' in credentials folder"""
    creds = credentialsPath('gcs-credentatials.json')
    return bigquery.Client.from_service_account_json(creds)

def jobConfig():
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('size', 'INT64', 10),)
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(7e9)

    return config

def fetchQuery(query, year, month, cache=False):
    j = client().query(query=query, job_config=jobConfig())
    df = j.to_dataframe() # do i need to be keeping as dataframe?
    if cache:
        date = getDate(year, month)
        df.to_csv(cachePath(f"""{date}/author-subreddit-counts.csv"""))

    return df
