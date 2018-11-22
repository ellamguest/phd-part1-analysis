import pandas as pd
from scipy import stats
from pathlib import Path
from time import time
import pickle
from aws import s3
import numpy as np
from scipy.sparse import csr_matrix
import scipy as sp
from tools import *

REQUIREMENTS = """
conda install boto3 pandas pathlib scipy -y
pip install google-cloud-bigquery
pip install google-cloud-storage
"""

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

def makeCSR(df, variable, row_indices, col_indices):
    data = df[variable]
    incidence = csr_matrix((data, (row_indices, col_indices)))

    return incidence


def subredditLevelCSR(df):
    incidence = csr_matrix((df['num_comments'], (df['subreddit_id'], df['author_id'])))

    results = {}
    for i in df['subreddit_id'].unique():
        subset = incidence[i].toarray()
        values = subset[np.nonzero(subset)]

        results[i] = {'subreddit_author_count':np.count_nonzero(values),
               'subreddit_comment_count':np.sum(values),
               'subreddit_author_entropy': stats.entropy(values),
                       'subreddit_author_gini': gini(values)}

    return pd.DataFrame.from_dict(results, orient='index')

def describeStatCSR(df, variable):
    incidence = csr_matrix((df[variable], (df['subreddit_id'], df['author_id'])))

    results = {}
    for i in df['subreddit_id'].unique():
        subset = incidence[i].toarray()
        values = subset[np.nonzero(subset)]
        if len(values) == 0:
            values = [0]

        lower, median, upper = np.percentile(values, [25,50,75])
        results[i] = {
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

def subredditStats(df, date):
    authorIds = getIds(df['author'])
    df['author_id']=df['author'].map(lambda x: authorIds[x])
    
    authorLevel = authorLevelCSR(df)
    subredditLevel = subredditLevelCSR(df)

    output = pd.concat([subredditLevel, authorLevel], axis=1)
    
    subIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit_id'] = output.index
    output['subreddit'] = output['subreddit_id'].map(lambda x: subIds[x])
    output = output.sort_values('subreddit_comment_count', ascending=False).reset_index(drop=True)
    
    output.to_csv(outputPath(f"""{date}/subredditStats.csv"""))

def addIds(df):
    subIds = getIds(df['subreddit'])
    df['subreddit_id']=df['subreddit'].map(lambda x: subIds[x])

    authorIds = getIds(df['author'])
    df['author_id']=df['author'].map(lambda x: authorIds[x])
    
    return df
    

def runMonth(year, month):
    start = time()

    date = getDate(year, month)
    createDirectories(date)

    end = time()
    elapsed(start,end)
    print()
    
    df = addIds(df)

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
        
def subsetDF(df):
    defaults = """Art+AskReddit+DIY+Documentaries+EarthPorn+Futurology+GetMotivated+IAmA+InternetIsBeautiful+Jokes+\
LifeProTips+Music+OldSchoolCool+Showerthoughts+TwoXChromosomes+UpliftingNews+WritingPrompts+\
announcements+askscience+aww+blog+books+creepy+dataisbeautiful+explainlikeimfive+food+funny+\
gadgets+gaming+gifs+history+listentothis+mildlyinteresting+movies+news+nosleep+nottheonion+\
personalfinance+philosophy+photoshopbattles+pics+science+space+sports+television+tifu+\
todayilearned+videos+worldnews""".split('+')
    
    return df[(~df['subreddit'].isin(defaults)) &
                (~df['author'].isin(['[deleted]','AutoModerator'])) &
                (~df['subreddit'].str.startswith('u_'))
                ]

def main():
    setupGCS()
    """currently have author, subreddit pair data for jan to may 2018"""
    year = '2018'
    months = ['01','02','03','04','05']
    for month in months:
        runMonth(year, month)
