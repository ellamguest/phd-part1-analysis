import pandas as pd
from scipy import stats
from pathlib import Path
from time import time
import pickle
from aws import s3
import numpy as np
from scipy.sparse import csr_matrix
import scipy as sp

"""Collecting data from bigquery"""

try:
 from google.cloud import bigquery
except ImportError:
    import pip
    pip.main(['install', '--upgrade', 'google-cloud-bigquery'])
    
getDate = lambda year, month: f"""{year}-{month}"""
elapsed = lambda start, end: print(f"""{end-start} elapsed""")
    
cachePath = lambda filename: Path(f"""cache/{filename}""")
credentialsPath = lambda filename: Path(f"""credentials/{filename}""")
outputPath = lambda filename: Path(f"""output/{filename}""")

def createDirectories(date):
    """creates sub-directories for monthly data, if they don't exist already"""
    Path(f"""cache/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""output/{date}""").mkdir(exist_ok=True, parents=True)


"""COLLECTING DATA FROM BIGQUERY"""
    
def client():
    """REQUIRES A FILE CALLED 'bigquery.json' in credentials folder"""
    creds = credentialsPath('gcs-credentatials.json')
    return bigquery.Client.from_service_account_json(creds)

def jobConfig():
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('size', 'INT64', 10),)
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(7e9)

    return config

def fetchQuery(query, year, month, cache=False):
    j = client().query(query=query, job_config=jobConfig())
    df = j.to_dataframe() # do i need to be keeping as dataframe?
    if cache:
        date = getDate(year, month)
        df.to_csv(cachePath(f"""{date}/author-subreddit-counts.csv"""))

    return df

"""RUNNING STATS ON DATA"""

def getAuthorStats(df, date, cache=False):
    """takes df with columns author, subreddit, num_author_comments
    returns adds columns author_total_subreddits, author_comment_entropy, author_insubreddit_ratio
    """
    start = time()
    copy = df.copy()
    author_stats = pd.DataFrame({'author_total_subreddits':copy.groupby('author')['subreddit'].count(),
                                 'author_total_comments':copy.groupby('author')['num_comments'].sum(),
                                 'author_comment_entropy':copy.groupby('author')['num_comments'].apply(
                                    lambda x: stats.entropy(x))})

    copy = copy.merge(author_stats, left_on='author', right_index=True)
    copy['author_insubreddit_ratio']=copy['num_comments']/copy['author_total_comments']
    
    end = time()
    elapsed(start, end)
    
    if cache:
        copy.to_csv(cachePath(f"""{date}/author-subreddit-counts-plus-author-stats.csv"""))

    return copy

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

sortedIndices = lambda series: sp.unique(series, return_inverse=True) # takes awhile

def makeCSR(df, variable, row_indices, col_indices):
    data = df[variable]
    incidence = csr_matrix((data, (row_indices, col_indices)))
    
    return incidence


def subredditLevelCSR(df, subreddit_indices, author_indices, subreddit_unique):
    incidence = makeCSR(df, 'num_comments', subreddit_indices, author_indices)
    
    ids = dict(zip(set(subreddit_indices), subreddit_unique))
    results = {}
    for k,v in ids.items():
        subset = incidence[k].toarray()
        values = subset[np.nonzero(subset)]
        
        results[v] = {'subreddit_author_count':np.count_nonzero(values),
               'subreddit_comment_count':np.sum(values),
               'subreddit_author_entropy': stats.entropy(values),
                       'subreddit_author_gini': gini(values)}
    
    return pd.DataFrame.from_dict(results, orient='index')   

def describeStatCSR(df, variable, subIds):
    incidence = csr_matrix((df[variable], (df['subreddit_id'], df['author_id'])))
    
    #ids = dict(zip(set(subreddit_indices), subreddit_unique))
    results = {}
    for k,v in subIds.items():
        subset = incidence[v].toarray()
        values = subset[np.nonzero(subset)]
        if len(values) == 0:
            values = [0]
        
        lower, median, upper = np.percentile(values, [25,50,75])
        results[k] = {
                        'min':np.min(values),
                        'max':np.max(values),
                        'mean':np.mean(values),
                        'std':np.std(values),
                        '25%':lower,
                        'median':median,
                        '75%':upper
                        }
    
    return pd.DataFrame.from_dict(results, orient='index')   


def authorLevelCSR(df):
    variables = ['author_total_subreddits', 'author_total_comments',
       'author_comment_entropy', 'author_insubreddit_ratio']
    results = []
    for variable in variables:
        stats = describeStatCSR(df, variable)
        stats.columns = [f"""{variable}_{x}""" for x in stats.columns]
        results.append(stats)
    
    return pd.concat(results, axis=1)

def getIds(series):
    unique = series.unique()
    ids = {}
    for i,x in enumerate(unique):
        ids[x]=i
        
    return ids

def subredditStats(df, date):
    subIds = getIds(df['subreddit'])
    df['subreddit_id']=df['subreddit'].map(lambda x: subIds[x])
    
    authorIds = getIds(df['author'])
    df['author_id']=df['author'].map(lambda x: authorIds[x])
    
    authorLevel = authorLevelCSR(df, subIds)
    subredditLevel = subredditLevelCSR(df, subIds)
    
    output = pd.concat([subredditLevel, authorLevel], axis=1)
    
    output.to_csv(outputPath(f"""{date}/subredditStats.csv"""))
    
    
def runMonth(year, month):
    start = time()
    
    date = getDate(year, month)
    createDirectories(date)
    
    print(f"""fetching data for {date}""")
    query = f"""SELECT * FROM `author-subreddit-counts.{year}.{month}`"""
    df = fetchQuery(query, year=year, month=month)
    
    end = time()
    elapsed(start,end)
    print()
    
    start=time()
    print(f"""getting author stats for {date}""")
    copy = getAuthorStats(df, date, cache=True)
    
    end = time()
    elapsed(start,end)
    print()
    
    print(f"""getting subreddit stats for {date}""")
    subredditStats(copy, date)
    print()
    
    end = time()
    elapsed(start,end)
    
    print('DONE!')
    
def saveToS3(date, filename):
    file = pd.read_csv(outputPath(f"""{date}/{filename}"""))
  
    s3.Path(f"""emg-phd-part1/{date}/{filename}""").write_bytes(pickle.dumps(file))

    
def pullFromS3(date, filename):
    """is this actually working?"""
    file = s3.Path(f"""emg-phd-part1/{date}/{filename}""").read_bytes()
    
    with open(outputPath(f"""{date}/{filename}"""), 'wb') as filename:
        pickle.dump(file, filename)

    
def main():
    createDirectories()
    """currently have author, subreddit pair data for jan to may 2018"""
    year = '2018'
    months = ['01','02','03','04','05']
    for month in months:
        runMonth(year, month)