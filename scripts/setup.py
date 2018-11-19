"""assumes you are in a subdirectory such as /script or /notebook"""

from pathlib import Path
import pandas as pd
from scipy import stats
import time
from dataCollection import *
from dataAnalysis import *


def createDirectories():
    Path('../cache').mkdir(exist_ok=True, parents=True)
    Path('../credentials').mkdir(exist_ok=True, parents=True)
    Path('../output').mkdir(exist_ok=True, parents=True)
    
def createMonthDirectories(date):
    Path('../cache/' + date).mkdir(exist_ok=True, parents=True)
    Path('../output/' + date).mkdir(exist_ok=True, parents=True)
    
cachePath = lambda filename: Path('../cache/{}'.format(filename))
credentialsPath = lambda filename: Path('../credentials/{}'.format(filename))
outputPath = lambda filename: Path('../output/{}'.format(filename))

def main():
    createDirectories()
    
    start_time = time.time()
    
    year, month = '2018','02'
    date = '{}-{}'.format(year, month)
    
    createMonthDirectories(date)
    
    query = """SELECT * FROM `author-subreddit-counts.{}.{}`""".format(year, month)
    
    # Make sure that you have file ../credentials/bigquery.json
    df = fetchQuery(query, year=year, month=month, cache=True)
    
    df = pd.read_csv(cachePath(date + '/author-subreddit-counts.csv'), index_col=0)
    
    copy = getAuthorStats(df, date, cache=True)
    runSubredditStats(copy, date, output=True)

    authorLevel, subredditLevel = loadSubredditStats(date)
    
    end_time = time.time()
    
    print('Took', end_time-start_time, 'to run')