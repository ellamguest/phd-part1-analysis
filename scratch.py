#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from tools import credentialsPath, cachePath, getDate
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from io import BytesIO
from dataProcessing import *


"""IMPORTING DATA FROM GOOGLE CLOUD STORAGE"""
storage_client = lambda: storage.Client.from_service_account_json('service_account.json')

def fetchBlob(date):
    client = storage.Client.from_service_account_json('service_account.json')
    bucket = client.get_bucket('emg-author-subreddit-pairs')
    blob = bucket.blob(f"""{date}.gzip""")
    return blob

download_blob = lambda blob, date: blob.download_to_filename(cachePath(f"""{date}/author-subreddit-pairs.gzip"""))
read_blob = lambda date: pd.read_csv(cachePath(f"""{date}/author-subreddit-pairs.gzip"""), compression="gzip")


def sortedSubredditIds(df):
    order = df['subreddit'].value_counts().sort_values(ascending=False).reset_index().reset_index()
    return dict(zip(order['index'], order['level_0']))

def sample(month, num_subreddits=200):
    year = '2018'
    date = getDate(year, month)
    
    createDirectories(date)
    
    print("getting and storing blob""")
    blob = fetchBlob(date)
    download_blob(blob, date) # look into gcsfs to avoiding storing locally
    
    print("opening df and subsetting")
    df = read_blob(date)
    df = df[['subreddit','author','num_comments']]
    subset = subsetDF(df)
    
    print("getting sub ids and top subreddits")
    subIds = sortedSubredditIds(subset)
    subset['subreddit_id'] = subset['subreddit'].map(lambda x: subIds[x]) # gets setting with copywarning
    sample = subset[subset['subreddit_id']<num_subreddits]

    print("getting author stats")
    updated = getAuthorStats(sample, date) # still longest bit ~ 3 mins
    
    print("getting subreddit stats")
    subredditStats(updated, date)


def statsDf():
    months = ['01','02','03','04','05']
    dfs = []
    for month in months:
        date = getDate('2018', month)
        df = pd.read_csv(outputPath(f"""{date}/subredditStats.csv"""), index_col=0)
        df['month'] = int(month)
        dfs.append(date)
        
    result = pd.concat(dfs)
        
    return results

stats = statsDf()
        
sample('05')
