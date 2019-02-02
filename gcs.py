from pathlib import Path
from tools import cachePath, outputPath, createDirectories
import pandas as pd
from google.cloud import storage
import gcsfs
import os


"""IMPORTING DATA FROM GOOGLE CLOUD STORAGE"""
storage_client = lambda: storage.Client.from_service_account_json('service_account.json')

def fetchBlob(date):
    bucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    return bucket.blob(f"""{date}.gzip""")

def streamBlob(bucket_name, date):
    print(f"""opening blob {date} for bucket {bucket_name} from GCS""")
    fs = gcsfs.GCSFileSystem(project='author-subreddit-counts',token='service_account.json')
    with fs.open(f"""{bucket_name}/{date}.gzip""") as f:
        return pd.read_csv(f, compression="gzip", index_col = 0)

def writeBlob(bucket_name, filename, date):
    bucket = storage_client().get_bucket(bucket_name)
    blob = bucket.blob(f"""{date}.gzip""")
    blob.upload_from_filename(filename)

def bucketDates(bucket_name):
    bucket = storage_client().get_bucket(bucket_name)
    result = []
    for blob in bucket.list_blobs():
        result.append(blob.name.strip('.gzip'))

    return result

def checkUploaded(bucket_name, file_name):
    uploaded = bucketDates(bucket_name)

    dates = getDates()
    run = []
    set(dates) - set(uploaded) - set(run)


