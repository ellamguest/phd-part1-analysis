from pathlib import Path
from tools import cachePath, outputPath, createDirectories, getDates
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


