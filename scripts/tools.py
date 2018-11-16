#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 14:28:37 2018

@author: emg
"""
import pandas as pd
import os
from sqlalchemy import create_engine

try:
 from google.cloud import bigquery
except ImportError:
    import pip
    pip.main(['install', '--upgrade', 'google-cloud-bigquery'])

"""SETTING LOCAL FILE STRUCTURE"""
fileDir = os.path.dirname(os.path.realpath('__file__'))
cache = os.path.join(fileDir, 'cache')

"""QUERYING GOOGLE BIGQUERY"""
def client():
    """REQUIRES A FILE CALLED 'bigquery.json' in credentials folder"""
    credentials = os.path.join(fileDir, 'credentials')
    bigquery_credentials = os.path.join(credentials, 'bigquery.json')
    return bigquery.Client.from_service_account_json(bigquery_credentials)

def jobConfig():
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('size', 'INT64', 10),)
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(7e9)

    return config

def fetchQuery(query):
    j = client().query(query=query, job_config=jobConfig())
    df = j.to_dataframe()

    return df


"""STORING LOCAL SQL DATABASE"""
def saveSQL(df, table_name, database_name, **kwargs):
    engine = create_engine('sqlite:///{}.db'.format(database_name), echo=False)
    df.to_sql(name=table_name, con=engine,  **kwargs)

def loadSQL(table_name, database_name, index_col='index'):
    engine = create_engine('sqlite:///{}.db'.format(database_name), echo=False)
    df = pd.read_sql_table(table_name=table_name, con=engine, index_col=index_col)

    return df

"""DATA CLEANING"""
def removeAuthors(df):
    """The two most prolific comment authors are almost certainly [deleted]
        and AutoModerator, thus return their ids to exclude from analysis"""
    dropAuthors = list((df.groupby('author')['count'].sum().sort_values()
                            .tail(2).index))

    return df[~df['author'].isin(dropAuthors)]


"""CALCULATING MEASURES"""
def gini(values):
    "calculate the gini coefficient for a list of values"
    v = values.copy()
    v.sort()

    sum_iy = 0
    for i, y in enumerate(v):
        i += 1
        sum_iy += i*y

    sum_y = sum(v)
    n = len(v)
    return ((2*sum_iy)/(n*sum_y)) - ((n+1)/n)
