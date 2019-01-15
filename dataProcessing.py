import pandas as pd
from scipy import stats
from pathlib import Path
import time
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from tools import *
from google.cloud import storage
from gcs import fetchBlob, storeBlob, readBlob
from tqdm import tqdm
from andyTools import parallel]
import os

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

def blauNormal(blau, N):
    """
    takes the blau and size of population (N)
    returns the normalized Herfindahl index
    """
    nom = blau - (1/N)
    den = 1 - (1/N)

    return nom / den


"""RUNNING STATS ON DATA"""
from tqdm import tqdm

def authorStats(values):
    return {'aut_sub_count':np.count_nonzero(values),
               'aut_com_count':np.sum(values),
               'aut_com_entropy': stats.entropy(values),
                       'aut_com_gini': gini(values),
                       'aut_com_blau': blau(values)}
                       
def getAuthorStats(df):
    """slicing the csr is still the least efficient part of data processing"""
    start = time.time()

    row = df['author_id']
    col = df['subreddit_id']
    data = df['num_comments']

    incidence = csr_matrix((data, (row, col)))
    
    results = {}
    for i in tqdm(row.unique()):
        values = incidence[i,:].data
        results[i] = authorStats(values)
    
    authorStats = pd.DataFrame.from_dict(results, orient='index')
    authorStats.index = df['author_id'].unique()
    
    result = df.merge(authorStats, left_on='author_id', right_index=True)
    result['aut_insub'] = result['num_comments']/result['aut_com_count']
    
    end = time.time()
    elapsed(start, end)

    return result

def rangeStats(values):
    lower, median, upper = np.percentile(values, [25,50,75])
    return {
                    'min':np.min(values),
                    'max':np.max(values),
                    'mean':np.mean(values),
                    'std':np.std(values),
                    '25%':lower,
                    'median':median,
                    '75%':upper
                    }

def authorLevelCSR(df):
    variables = ['aut_sub_count', 'aut_com_count', 'aut_com_entropy', 'aut_com_gini',
       'aut_com_blau', 'aut_insub']
    
    stats = []
    for variable in variables:
        print(variable)
        row = df['subreddit_id']
        col = df['author_id']
        data = df[variable].values
        incidence = csr_matrix((data, (row, col)))

        with parallel(rangeStats) as g:
            results = g.wait({i: g(incidence[i,:].data) for i in row.unique()})

        output = pd.DataFrame.from_dict(results, orient='index')
        output.columns = [f"""{variable}_{stat}""" for stat in output.columns]

        stats.append(output)

    return pd.concat(stats, axis=1)

def subStats(values):
    return {'author_count':np.count_nonzero(values),
               'comment_count':np.sum(values),
               'entropy': stats.entropy(values),
                       'gini': gini(values),
                       'blau': blau(values)}

def subredditLevelCSR(df, row='subreddit_id', col='author_id'):
    row = df[row]
    col = df[col]
    
    data = df['num_comments'].values
    incidence = csr_matrix((data, (row, col)))
    with parallel(subStats) as g:
         results = g.wait({i: g(incidence[i,:].data) for i in row.unique()})

    return pd.DataFrame.from_dict(results, orient='index')

def subredditStats(df, date):
    subIds = dict(zip(df['subreddit_id'],df['subreddit']))

    authorLevel = authorLevelCSR(df)
    authorLevel['subreddit'] = authorLevel.index.map(lambda x: subIds[x])
    authorLevel.to_csv(outputPath(f"""{date}/authorLevelStats.csv"""))

    subredditLevel = subredditLevelCSR(df)
    subredditLevel['subreddit'] = subredditLevel.index.map(lambda x: subIds[x])
    subredditLevel.to_csv(outputPath(f"""{date}/subredditLevelStats.csv"""))

    output = pd.concat([subredditLevel, authorLevel], axis=1)
    output.to_csv(outputPath(f"""{date}/subredditStats.csv"""))
        
def cleanDF(df):
    defaults = """Art+AskReddit+DIY+Documentaries+EarthPorn+Futurology+GetMotivated+IAmA+InternetIsBeautiful+Jokes+\
LifeProTips+Music+OldSchoolCool+Showerthoughts+TwoXChromosomes+UpliftingNews+WritingPrompts+\
announcements+askscience+aww+blog+books+creepy+dataisbeautiful+explainlikeimfive+food+funny+\
gadgets+gaming+gifs+history+listentothis+mildlyinteresting+movies+news+nosleep+nottheonion+\
personalfinance+philosophy+photoshopbattles+pics+science+space+sports+television+tifu+\
todayilearned+videos+worldnews""".split('+')
    defaults.append('politics')
    
    clean = df.astype({'author':str,'subreddit':str,'num_comments':int})
    return clean[(~clean['subreddit'].isin(defaults)) &
                (~clean['author'].isin(['[deleted]','AutoModerator'])) &
                (~clean['subreddit'].str.startswith('u_'))
                ]

def sortedIds(series):
    order = series.value_counts().sort_values(ascending=False).reset_index().reset_index()
    return dict(zip(order['index'], order['level_0']))

def getIds(df, cache=True):
    df = df.astype({'author':str,'subreddit':str,'num_comments':int})

    print("getting subreddit ids")
    subIds = sortedIds(df['subreddit'])
    df['subreddit_id'] = df['subreddit'].map(lambda x: subIds[x])

    print("getting author ids")
    authorIds = sortedIds(df['author'])
    df['author_id']=df['author'].map(lambda x: authorIds[x])

    if cache:
        print("storing dataset w/ ids")
        df.to_csv(cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip"""),compression='gzip')

    return df

def loadDF(date):
    print(date)
    createDirectories(date)
    
    if cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip""").is_file():
        print("opening df with ids")
        return pd.read_csv(cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip"""),compression='gzip')
    elif cachePath(f"""{date}/author-subreddit-pairs.gzip""").is_file():
        print("opening df, need to get ids")
        df = readBlob(date)

        print("getting ids")
        return getIds(df)
    else:
        print(f"""getting and storing blob for {date}""")
        blob = fetchBlob(date)
        storeBlob(blob, date)

        print("getting ids")
        return getIds


def run(date, num_subreddits=500):
    """
    runs stats on top *num_subreddits* by num of authors
    """
    df = loadDF(date)

    print(f"""getting subreddit of {num_subreddits} subreddits""")
    subset = df[df['subreddit_id']<num_subreddits]

    print("getting aggregate level subreddit stats")
    subredditLevel = subredditLevelCSR(subset)
    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    subredditLevel['subreddit'] = subredditLevel.index.map(lambda x: reverseSubIds[x])
    subredditLevel.to_csv(outputPath(f"""{date}/subredditLevelStats.csv"""))

    print("getting author stats") # LONGEST PART, STILL TAKES A FEW MINUTES
    authorStats = getAuthorStats(subset)
    authorStats.to_csv(cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip"""),compression='gzip')

    print("getting aggregate level author stats")
    authorLevel = authorLevelCSR(authorStats)
    authorLevel['subreddit'] = authorLevel.index.map(lambda x: reverseSubIds[x])
    authorLevel.to_csv(outputPath(f"""{date}/authorLevelStats.csv"""))

    output = pd.merge(subredditLevel, authorLevel, on='subreddit')
    output.to_csv(outputPath(f"""{date}/subredditStats.csv"""))

    print(f"""FINISHED WITH {date}""")

def months():
    dates = sorted(next(os.walk("cache"))[1])
    for date in dates[2:]:
        run(date)
    

