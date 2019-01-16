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
from andyTools import parallel
import os
from tqdm import tqdm
import logging

log = logging.getLogger(__name__)

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
def csr(df, row, col, data):
    row = df[row]
    col = df[col]
    data = df[data]

    return csr_matrix((data, (row, col)))


def getAuthorStats(df, date, num_subreddits, cache=True):
    """slicing the csr is still the least efficient part of data processing"""
    print("getting author stats") # LONGEST PART, STILL TAKES A FEW MINUTES

    start = time.time()

    incidence = csr(df, 'author_id', 'subreddit_id', 'num_comments')
    
    results = {}
    for i in tqdm(df['author_id'].unique()):
        values = incidence[i,:].data
        results[i] = {'aut_sub_count':np.count_nonzero(values),
               'aut_com_count':np.sum(values),
               'aut_com_entropy': stats.entropy(values),
                       'aut_com_gini': gini(values),
                       'aut_com_blau': blau(values)}
    
    output = pd.DataFrame.from_dict(results, orient='index')
    output.index = df['author_id'].unique()
    
    result = df.merge(output, left_on='author_id', right_index=True)
    result['aut_insub'] = result['num_comments']/result['aut_com_count']
    
    end = time.time()
    elapsed(start, end)

    if cache:
        start = time.time()
        print("storing author stats")
        result.to_csv(cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip"""),compression='gzip')

        end = time.time()
        elapsed(start, end)

    return result

def subStats(values):
    return {'author_count':np.count_nonzero(values),
               'comment_count':np.sum(values),
               'entropy': stats.entropy(values),
                       'gini': gini(values),
                       'blau': blau(values)}

def getSubredditStats(df, date, num_subreddits, cache=True):
    print("getting aggregate level subreddit stats")
    incidence = csr(df, 'subreddit_id', 'author_id', 'num_comments')
    with parallel(subStats) as g:
         results = g.wait({i: g(incidence[i,:].data) for i in df['subreddit_id'].unique()})

    output = pd.DataFrame.from_dict(results, orient='index')
    
    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit'] = output.index.map(lambda x: reverseSubIds[x])

    if cache:
        output.to_csv(outputPath(f"""{date}/top_{num_subreddits}_subredditLevelStats.csv"""))

    return output

def aggtats(values):
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

def authorAggStats(df):
    variables = ['aut_sub_count', 'aut_com_count', 'aut_com_entropy', 'aut_com_gini',
       'aut_com_blau', 'aut_insub']
    
    stats = []
    for variable in variables:
        incidence = csr_matrix(df, 'subreddit_id', 'author_id', variable)

        with parallel(aggStats) as g:
            results = g.wait({i: g(incidence[i,:].data) for i in df['subreddit_id'].unique()})

        output = pd.DataFrame.from_dict(results, orient='index')
        output.columns = [f"""{variable}_{stat}""" for stat in output.columns]

        stats.append(output)

    return pd.concat(stats, axis=1)

def getAggAuthorStats(date, num_subreddits):
    df = pd.read_csv(cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip"""),compression='gzip')
    print("getting aggregate level author stats")
    authorAgg = authorAggStats(df)


    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    authorAgg['subreddit'] = authorAgg.index.map(lambda x: reverseSubIds[x])
    authorAgg.to_csv(outputPath(f"""{date}/top_{num_subreddits}_authorLevelStats.csv"""))

def filenames(date, num_subreddits):
    subFile = outputPath(f"""{date}/top_{num_subreddits}_subredditLevelStats.csv""")
    autFile = cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip""")
    autAggFile = outputPath(f"""{date}/top_{num_subreddits}_authorLevelStats.csv""")

    return subFile, autFile, autAggFile

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

def getIds(df, date, cache=True):
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
        return getIds(df, date)
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

    subFile, autFile, autAggFile = filenames(date, num_subreddits)

    if subFile.is_file():
        print("opening subreddit level stats")
        subredditLevel = pd.read_csv(subFile)
    else:
        print("need to get subreddit level stats")
        subredditLevel = getSubredditStats(df, date, num_subreddits)


    if autAggFile.is_file():
        print("opening author agg stats")
        authorLevel = pd.read_csv(authFile)
    else:
        print("need to get author agg stats")
        
        if autFile.is_file():
            print("opening author level stats")
            authorLevel = pd.read_csv(authFile)
            
        else:
            print("need to get author level stats")
            autorLevel = getAuthorStats(df, date, num_subreddits)
        



    print("getting author stats") # LONGEST PART, STILL TAKES A FEW MINUTES
    authorLevel = getAuthorStats(subset)

    print("storing author stats")
    authorLevel.to_csv(cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip"""),compression='gzip')

    print("getting aggregate level author stats")
    authorAgg = authorLevelCSR(authorLevel)
    authorAgg['subreddit'] = authorAgg.index.map(lambda x: reverseSubIds[x])
    authorAgg.to_csv(outputPath(f"""{date}/authorLevelStats.csv"""))

    output = pd.merge(subredditLevel, authorLevel, on='subreddit')
    output.to_csv(outputPath(f"""{date}/subredditStats.csv"""))

    print(f"""FINISHED WITH {date}""")

def months():
    dates = sorted(next(os.walk("cache"))[1])
    for date in dates[2:]:
        run(date)
    

