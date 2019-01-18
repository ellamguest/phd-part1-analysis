#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from aws import s3
from tools import cachePath, outputPath, getDate, createDirectories
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import pickle
import gcsfs


"""IMPORTING DATA FROM GOOGLE CLOUD STORAGE"""
storage_client = lambda: storage.Client.from_service_account_json('service_account.json')

def fetchBlob(date):
    bucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    return bucket.blob(f"""{date}.gzip""")

storeBlob = lambda blob, date: blob.download_to_filename(cachePath(f"""{date}/author-subreddit-pairs.gzip"""))  
readBlob = lambda date: pd.read_csv(cachePath(f"""{date}/author-subreddit-pairs.gzip"""), compression="gzip")

def readIDs(date):
    """returning None"""
    print("""opening df w/ ids from GCS""")
    fs = gcsfs.GCSFileSystem(project='author-subreddit-counts',token='service_account.json')
    with fs.open(f"""emg-author-subreddit-pairs-ids/{date}.gzip""") as f:
        return pd.read_csv(f, compression="gzip")


def writeBlob(date):
    bucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    blob = bucket.blob(f"""{date}-IDS.gzip""")

    filename = cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip""")
    with open(filename, 'r') as f:
        blob.upload_from_file(f)

def upload_blob(source_file_name, destination_blob_name):
    """Uploads a file to the bucket
    but slow"""
    bucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

def uploadIDs(date):
    upload_blob(str(cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip""")), f"""{date}-IDs.gzip""")

def checkBlobs():
    rawBucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    raw = []
    for blob in rawBucket.list_blobs():
        raw.append(blob.name)

    idBucket = storage_client().get_bucket('emg-author-subreddit-pairs-ids')
    ids = []
    for blob in idBucket.list_blobs():
        ids.append(blob.name)

    noIDs = []
    for blob in raw:
        if blob not in ids:
            noIDs.append(blob.strip('.gzip'))

    uploadIDs = []
    runIDs = []
    for date in noIDs:
        if cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip""").is_file():
            uploadIDs.append(date)
        else:
            runIDs.append(date)

    for date in runIDs:
        createDirectories(date)
        freshData(date)

    with open('uploadCommands.sh', 'w') as f:
        for date in uploadIDs:
            command = f"""gsutil cp cache/{date}/author-subbreddit-pairs-IDs.gzip gs://emg-author-subreddit-pairs-ids/{date}.gzip"""
            f.write("%s\n" % command)
        

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


## COPY FILE
# gsutil cp cache/{date}/author-subbreddit-pairs-IDs.gzip gs://emg-author-subreddit-pairs-ids/{date}.gzip
    



