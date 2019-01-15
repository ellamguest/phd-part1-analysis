#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from aws import s3
from tools import cachePath, outputPath, getDate
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import pickle


"""IMPORTING DATA FROM GOOGLE CLOUD STORAGE"""
storage_client = lambda: storage.Client.from_service_account_json('service_account.json')

def fetchBlob(date):
    bucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    return bucket.blob(f"""{date}.gzip""")

storeBlob = lambda blob, date: blob.download_to_filename(cachePath(f"""{date}/author-subreddit-pairs.gzip"""))  
readBlob = lambda date: pd.read_csv(cachePath(f"""{date}/author-subreddit-pairs.gzip"""), compression="gzip")


"""COLLECTING DATA FROM GOOGLE BIGQUERY"""
bigquery_client = lambda: bigquery.Client.from_service_account_json('service_account.json')

def jobConfig():
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('size', 'INT64', 10),)
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(7e9)

    return config

def fetchQuery(query, year, month, cache=False):
    j = bigquery_client().query(query=query, job_config=jobConfig())
    df = j.to_dataframe() # do i need to be keeping as dataframe?
    if cache:
        date = getDate(year, month)
        df.to_csv(cachePath(f"""{date}/author-subreddit-counts.csv"""))

    return df


""" ANDY'S S3 """
def saveToS3(date, filename):
    file = pd.read_csv(outputPath(f"""{date}/{filename}"""))

    s3.Path(f"""emg-phd-part1/{date}/{filename}""").write_bytes(pickle.dumps(file))


def pullFromS3(date, filename):
    """is this actually working?"""
    file = s3.Path(f"""emg-phd-part1/{date}/{filename}""").read_bytes()

    with open(outputPath(f"""{date}/{filename}"""), 'wb') as filename:
        pickle.dump(file, filename)
    



