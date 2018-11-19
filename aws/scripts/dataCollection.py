#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 14:28:37 2018

@author: emg
"""
import pandas as pd
from pathlib import Path
from setup import credentialsPath, cachePath, createMonthDirectories

try:
 from google.cloud import bigquery
except ImportError:
    import pip
    pip.main(['install', '--upgrade', 'google-cloud-bigquery'])

"""QUERYING GOOGLE BIGQUERY"""
def client():
    """REQUIRES A FILE CALLED 'bigquery.json' in credentials folder"""
    bigquery_credentials = credentialsPath('bigquery.json')
    return bigquery.Client.from_service_account_json(bigquery_credentials)

def jobConfig():
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('size', 'INT64', 10),)
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(7e9)

    return config

def fetchQuery(query, year, month, cache=False):
    j = client().query(query=query, job_config=jobConfig())
    df = j.to_dataframe()
    if cache:
        date = '{}-{}'.format(year, month)
        df.to_csv(cachePath(date + '/author-subreddit-counts.csv'))

    return df

def fetchAuthorSubredditPairs(year, month, *kwargs):
    query = """SELECT * FROM `author-subreddit-counts.{}.{}`""".format(year, month)
    df = fetchQuery(query, *kwargs)
    
    return df

def main():
    year, month = '2018','02'
    date = '{}-{}'.format(year, month)
    
    createMonthDirectories(date)