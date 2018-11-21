#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 16:13:52 2018

@author: emg
"""




from google.cloud import storage
from dataProcessing import credentialsPath, cachePath
import pandas as pd

creds = credentialsPath('gcs-credentials.json')
client = storage.Client().from_service_account_json(creds)

bucket = client.get_bucket('emg-author-subreddit-pairs')
blob = bucket.get_blob('2018-01.csv')

blob.download_to_filename(cachePath('2018-01/author-subreddit-pairs.csv'))
df = pd.read_csv(cachePath('2018-01/author-subreddit-pairs.csv'))

df = df[['author','subreddit','num_comments']]