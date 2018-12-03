import pandas as pd
from scipy import stats
from pathlib import Path
from time import time
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from tools import *
from google.cloud import storage
from gcs import fetchBlob, storeBlob, readBlob

REQUIREMENTS = """
conda install boto3 pandas pathlib scipy -y
pip install google-cloud-bigquery
pip install google-cloud-storage
"""

""" DIVERSITY MEASURES """
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

def blau(values):
    pi = values/np.sum(values)
    pi2 = [p**2 for p in pi]
    sum_pi2 = np.sum(pi2)
    
    return 1-sum_pi2

"""RUNNING STATS ON DATA"""
def getAuthorStats(df):
    """slicing the csr is still the least efficient part of data processing"""
    start = time()
    incidence = csr_matrix((df['num_comments'], (df['author_id'], df['subreddit_id'])))
    
    results = {}
    for i in df['author_id'].unique():
        if i%500 == 0:
            print(i)
            
        subset = incidence[i].toarray()
        values = subset[np.nonzero(subset)]

        results[i] = {'aut_sub_count':np.count_nonzero(values),
               'aut_com_count':np.sum(values),
               'aut_com_entropy': stats.entropy(values),
                       'aut_com_gini': gini(values),
                       'aut_com_blau': blau(values)}
        
    authorStats = pd.DataFrame.from_dict(results, orient='index')
    authorStats.index = df['author_id'].unique()
    
    result = df.merge(authorStats, left_on='author_id', right_index=True)
    result['aut_insub'] = result['num_comments']/result['aut_com_count']
    
    end = time()
    elapsed(start, end)

    return result


def subredditLevelCSR(df):
    incidence = csr_matrix((df['num_comments'], (df['subreddit_id'], df['author_id'])))

    results = {}
    for i in df['subreddit_id'].unique():
        subset = incidence[i].toarray()
        values = subset[np.nonzero(subset)]

        results[i] = {'subreddit_author_count':np.count_nonzero(values),
               'subreddit_comment_count':np.sum(values),
               'subreddit_author_entropy': stats.entropy(values),
                       'subreddit_author_gini': gini(values),
                       'subreddit_author_blau': blau(values)}

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
    variables = ['aut_sub_count', 'aut_com_count', 'aut_com_entropy', 'aut_com_gini',
       'aut_com_blau', 'aut_insub']
    results = []
    for variable in variables:
        stats = describeStatCSR(df, variable)
        stats.columns = [f"""{variable}_{x}""" for x in stats.columns]
        results.append(stats)

    return pd.concat(results, axis=1)

def subredditStats(df, date):
    authorLevel = authorLevelCSR(df)
    subredditLevel = subredditLevelCSR(df)

    output = pd.concat([subredditLevel, authorLevel], axis=1)
    
    subIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit_id'] = output.index
    output['subreddit'] = output['subreddit_id'].map(lambda x: subIds[x])
    output = output.sort_values('subreddit_comment_count', ascending=False).reset_index(drop=True)
    
    output.to_csv(outputPath(f"""{date}/subredditStats.csv"""))
        
def cleanDF(df):
    defaults = """Art+AskReddit+DIY+Documentaries+EarthPorn+Futurology+GetMotivated+IAmA+InternetIsBeautiful+Jokes+\
LifeProTips+Music+OldSchoolCool+Showerthoughts+TwoXChromosomes+UpliftingNews+WritingPrompts+\
announcements+askscience+aww+blog+books+creepy+dataisbeautiful+explainlikeimfive+food+funny+\
gadgets+gaming+gifs+history+listentothis+mildlyinteresting+movies+news+nosleep+nottheonion+\
personalfinance+philosophy+photoshopbattles+pics+science+space+sports+television+tifu+\
todayilearned+videos+worldnews""".split('+')
    defaults.append('politics')
    
    return df[(~df['subreddit'].isin(defaults)) &
                (~df['author'].isin(['[deleted]','AutoModerator'])) &
                (~df['subreddit'].str.startswith('u_'))
                ]

def sortedIds(series):
    order = series.value_counts().sort_values(ascending=False).reset_index().reset_index()
    return dict(zip(order['index'], order['level_0']))

def run(year, month, num_subreddits=100000, fetch=False):
    """
    pulls data from GCS
    runs stats on top *num_subreddits* by num of authors
    """
    date = getDate(year, month)
    
    createDirectories(date)
    
    if fetch:
        print("getting and storing blob for""", date)
        blob = fetchBlob(date)
        storeBlob(blob, date) # look into gcsfs to avoiding storing locally
        
    print("opening df and subsetting")
    df = readBlob(date)
    #df = df[['subreddit','author','num_comments']]
    clean = cleanDF(df)
    
    print("getting sub ids and top subreddits")
    subIds = sortedIds(clean['subreddit'])
    clean['subreddit_id'] = clean['subreddit'].map(lambda x: subIds[x]) # gets setting with copywarning
    subset = clean[clean['subreddit_id']<num_subreddits]

    print("getting author stats")
    authorIds = sortedIds(subset['author'])
    subset['author_id']=subset['author'].map(lambda x: authorIds[x])
    authorStats = getAuthorStats(subset) # still longest bit, also gets setting with copywarning
    
    print("getting subreddit stats")
    subredditStats(authorStats, date)


