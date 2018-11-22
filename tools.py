#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from time import time

def setupGCS():
    try:
     from google.cloud import bigquery
    except ImportError:
        import pip
        pip.main(['install', '--upgrade', 'google-cloud-bigquery'])
        
    try:
     from google.cloud import storage
    except ImportError:
        import pip
        pip.main(['install', '--upgrade', 'google-cloud-storage'])

getDate = lambda year, month: f"""{year}-{month}"""
cachePath = lambda filename: Path(f"""cache/{filename}""")
credentialsPath = lambda filename: Path(f"""credentials/{filename}""")
outputPath = lambda filename: Path(f"""output/{filename}""")

def createDirectories(date):
    """creates sub-directories for monthly data, if they don't exist already"""
    Path(f"""cache/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""output/{date}""").mkdir(exist_ok=True, parents=True)


elapsed = lambda start, end: print(f"""{end-start} elapsed""")  
  
def timed(function, args):
    start=time()
    output = function(*args)
    end = time()
    elapsed(start,end)
    return output

def getIds(series):
    unique = series.unique()
    ids = {}
    for i,x in enumerate(unique):
        ids[x]=i

    return ids