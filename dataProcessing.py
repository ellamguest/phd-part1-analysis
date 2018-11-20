import pandas as pd
from scipy import stats
from pathlib import Path
import time
import pickle
from aws import s3

"""Collecting data from bigquery"""

try:
 from google.cloud import bigquery
except ImportError:
    import pip
    pip.main(['install', '--upgrade', 'google-cloud-bigquery'])
    
cachePath = lambda filename: Path(f"""cache/{filename}""")
credentialsPath = lambda filename: Path(f"""credentials/{filename}""")
outputPath = lambda filename: Path(f"""output/{filename}""")

getDate = lambda year, month: f"""{year}-{month}"""
    
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
        date = getDate(year, month)
        df.to_csv(cachePath(f"""{date}/author-subreddit-counts.csv"""))

    return df

def getAuthorStats(df, date, cache=False):
    """takes df with columns author, subreddit, num_author_comments
    returns adds columns author_total_subreddits, author_comment_entropy, author_insubreddit_ratio
    """
    print("calculating author-level stats...")
    copy = df.copy()
    author_stats = pd.DataFrame({'author_total_subreddits':copy.groupby('author')['subreddit'].count(),
                                 'author_total_comments':copy.groupby('author')['num_comments'].sum(),
                                 'author_comment_entropy':copy.groupby('author')['num_comments'].apply(
                                    lambda x: stats.entropy(x))})

    copy = copy.merge(author_stats, left_on='author', right_index=True)
    copy['author_insubreddit_ratio']=copy['num_comments']/copy['author_total_comments']
    
    if cache:
        copy.to_csv(cachePath(f"""{date}/author-subreddit-counts-plus-author-stats.csv"""))

    return copy

def aggregateAuthorLevelStats(df, date, output=False):
    """! NEED IT FIX MULTIINDEXING """
    print("getting distribution stats on author num comments, num subreddits, and insubreddit ratio...")
    num_author_subreddits = df.groupby('subreddit')['author_total_subreddits'].describe()
    num_author_comments = df.groupby('subreddit')['author_total_comments'].describe()
    author_insubreddit_ratio = df.groupby('subreddit')['author_insubreddit_ratio'].describe()
    author_subreddit_entropy = df.groupby('subreddit')['author_comment_entropy'].describe()
    author_subreddit_entropy.columns = pd.MultiIndex.from_product([['author_subreddit_entropy'], author_subreddit_entropy.columns])

    num_author_subreddits.columns = pd.MultiIndex.from_product([['num_author_subreddits'], num_author_subreddits.columns])
    num_author_comments.columns = pd.MultiIndex.from_product([['num_author_comments'], num_author_comments.columns])
    author_insubreddit_ratio.columns = pd.MultiIndex.from_product([['author_insubreddit_ratio'], author_insubreddit_ratio.columns])

    results = pd.merge(num_author_comments, num_author_subreddits, left_index=True, right_index=True)
    results = results.merge(author_insubreddit_ratio, left_index=True, right_index=True)
    results = results.merge(author_subreddit_entropy, left_index=True, right_index=True)

    if output:
        results.to_csv(
                outputPath(f"""{date}/aggregateAuthorLevelStats.csv"""))

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

def subredditLevelStats(df, date, output=False):
    print("calculating subreddit author entropy...")
    author_entropy = df.groupby('subreddit')['num_comments'].apply(lambda x: stats.entropy(x))

    print("calculating subreddit author gini coefficient...")
    author_gini = df.groupby('subreddit')['num_comments'].apply(lambda x: gini(list(x)))

    print("calculating subreddit author and comment counts...")
    author_count = df.groupby('subreddit')['author'].count()
    comment_count = df.groupby('subreddit')['num_comments'].sum()
    
    results = pd.concat([author_entropy, author_gini,
                         author_count, comment_count], axis=1)
    results.columns = ['subreddit_author_entropy', 'subreddit_author_gini', 
                       'subreddit_author_count', 'subreddit_comment_count']

    if output:

        results.to_csv(
                outputPath(f"""{date}/subredditLevelStats.csv"""))

def runSubredditStats(df, date, **kwargs):
    aggregateAuthorLevelStats(df, date, **kwargs)
    subredditLevelStats(df, date, **kwargs)
    
def loadSubredditStats(date):
    authorLevel = pd.read_csv(outputPath(f"""{date}/aggregateAuthorLevelStats.csv"""),
                              index_col=0, header=[0,1])

    subredditLevel = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""),
                                 index_col=0, header=0)

    return authorLevel, subredditLevel

def mainStats(date, stat='50%'):
    """merge subreddit level stats with author level stats
    stats should be one of ['25%', '50%', '75%', 'count', 'max', 'mean', 'min', 'std']"""
    authorLevel, subredditLevel = loadSubredditStats(date)
    authorMeasures = list(authorLevel.columns.levels[0])
    
    authorLevel.columns = authorLevel.columns.droplevel()
    subset = authorLevel[stat]
    
    subset.columns = [f"""{measure}_{stat}""" for measure in authorMeasures]
    
    result = subredditLevel.merge(subset, left_index=True, right_index=True)
    return result

def createDirectories(date):
    """creates sub-directories for monthly data, if they don't exist already"""
    Path(f"""cache/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""output/{date}""").mkdir(exist_ok=True, parents=True)

def runMonthTest(year, month):
    date = getDate(year, month)
    createDirectories(date)
    
    print(f"""fetching and caching author subreddit pair data for {date}""")
    query = f"""SELECT * FROM `author-subreddit-counts.{year}.{month}` LIMIT 1000"""
    df = fetchQuery(query, year=year, month=month, cache=False)
    print()
    
    print(f"""opening raw date for {date}""")
    df = pd.read_csv(cachePath(f"""{date}/author-subreddit-counts.csv"""), index_col=0)
    print()
    
    print(f"""getting author stats for {date}""")
    copy = getAuthorStats(df, date, cache=False)
    print()
    
    print(f"""getting subreddit stats for {date}""")
    runSubredditStats(copy, date, output=True)
    print()
    
def oldCode():
    start = time.time()
    date = getDate('2018', '01')
    df = pd.read_csv(cachePath(f"""{date}/author-subreddit-counts-plus-author-stats.csv"""), index_col=0)
    runSubredditStats(df, date, output=True)
    end = time.time()
    print(f"""original code took {end-start}""")
    
    
def newCode():
    start = time.time()
    date = getDate('2018', '01')
    df = pd.read_csv(cachePath(f"""{date}/author-subreddit-counts-plus-author-stats.csv"""), index_col=0)
    subredditStats(df)
    end = time.time()
    print(f"""new code took {end-start}""")
    
""" import cProfile
cProfile.run('testCode()', "path_to_stats.prof_file") 

"""
    
def runMonth(year, month):
    start_time = time.time()
    date = getDate(year, month)
    createDirectories(date)
    
    print(f"""fetching and caching author subreddit pair data for {date}""")
    query = f"""SELECT * FROM `author-subreddit-counts.{year}.{month}`"""
    df = fetchQuery(query, year=year, month=month, cache=True)
    print()
    
    print(f"""opening raw date for {date}""")
    df = pd.read_csv(cachePath(f"""{date}/author-subreddit-counts.csv"""), index_col=0)
    print()
    
    print(f"""getting author stats for {date}""")
    copy = getAuthorStats(df, date, cache=True)
    print()
    
    print(f"""getting subreddit stats for {date}""")
    runSubredditStats(copy, date, output=True)
    print()
    
    end_time = time.time()
    print(f"""Done, that took {end_time-start_time} seconds""")
    
def saveToS3(date):
    authorLevel, subredditLevel = loadSubredditStats(date)
  
    s3.Path("emg-phd-part1/2018-01/aggregateAuthorLevelStats.csv").write_bytes(pickle.dumps(authorLevel))
    s3.Path("emg-phd-part1/2018-01/subredditLevelStats.csv").write_bytes(pickle.dumps(subredditLevel))
    
def pullFromS3(date):
    authorLevel = s3.Path("emg-phd-part1/2018-01/aggregateAuthorLevelStats.csv").read_bytes()
    
    with open(outputPath(f"""{date}/authorLevel.pickle"""), 'wb') as filename:
        pickle.dump(authorLevel, filename)

    
def main():
    createDirectories()
    """currently have author, subreddit pair data for jan to may 2018"""
    year = '2018'
    months = ['01','02','03','04','05']
    for month in months:
        runMonth(year, month)