import pandas as pd
from scipy import stats
from pathlib import Path
import time
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scripts.tools import *
from google.cloud import storage
from tqdm import tqdm
from scripts.andyTools import parallel
import os
from tqdm import tqdm
import shutil
from scripts.tools import *

REQUIREMENTS = """
conda install pandas pathlib scipy -y
pip install google-cloud-storage
"""

""" DIVERSITY MEASURES """
def gini(values):
    """
    calculate the gini coefficient for a list of values
    a measure of equality, 0 = total equality, 1 = total inequality
    """
    v = values.copy()
    v.sort()

    sum_iy = 0
    for i, y in enumerate(v):
        i += 1
        sum_iy += i*y

    sum_y = sum(v)
    n = len(v)

    return 1 - (((2*sum_iy)/(n*sum_y)) - ((n+1)/n))

def blau(values):
    pi = values/np.sum(values)
    pi2 = [p**2 for p in pi]
    sum_pi2 = np.sum(pi2)
    
    return 1-sum_pi2



""" unused index versions """
def blauNormal(blau, N):
    """
    takes the blau and size of population (N)
    returns the normalized Herfindahl index
    """
    nom = blau - (1/N)
    den = 1 - (1/N)

    return nom / den
    
def trueDiversity(blau):
        """
        blau can be expressed as a transformation of true diversity of order 2
        """

        return - np.sqrt(1/(blau-1))


def gini_rewritten(values):
    """
    For a population with values yi, i = 1 to n, that are indexed in non-decreasing
    order (yi ≤ yi+1): 
    """
    v = values.copy()
    v.sort()

    n = len(v)

    main = []
    for i, y in enumerate(v):
        i += 1
        main.append((n+1-i)*y)

    return (1/n)*(n+1-2*(sum(main)/sum(v)))



"""RUNNING STATS ON DATA"""  
def sortedIds(series):
    order = series.value_counts().sort_values(ascending=False).reset_index().reset_index()
    return dict(zip(order['index'], order['level_0']))

def runIDS(date):
    """
    depends on a blob for that date existing in the bucket GCS bucket emg-author-subreddit-pairs,
    with columns author, subreddit, num_comments
    """
    createDirectories(date)
    input_bucket = 'emg-author-subreddit-pairs'
    output_bucket = 'emg-author-subreddit-pairs-ids'
    df = streamBlob(input_bucket, date)
    df = df.reset_index().astype({'author':str,'subreddit':str,'num_comments':int})

    print("getting subreddit ids")
    subIds = sortedIds(df['subreddit'])
    df['subreddit_id'] = df['subreddit'].map(lambda x: subIds[x])

    print("getting author ids")
    authorIds = sortedIds(df['author'])
    df['author_id']=df['author'].map(lambda x: authorIds[x])

    print("storing dataset w/ ids")

    filename = cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip""")
    df.to_csv(filename,compression='gzip')

    uploadCommands(filename, output_bucket, date)

def CSR(df, row, col, data):
    row = df[row]
    col = df[col]
    data = df[data]

    return csr_matrix((data, (row, col)))

def subStats(values):
    return {'author_count':np.count_nonzero(values),
               'comment_count':np.sum(values),
               'entropy': stats.entropy(values),
                       'gini': gini(values),
                       'blau': blau(values)}

def runSubredditStats(date, drop_deleted=True):
    """Computes statistics for each author in the dataset
    variables = ['author_count',comment_count','entropy','gini','blau']
    """
    df = pd.read_csv(cachePath(f"""{date}/authorStats.gzip"""), compression='gzip')

    if drop_deleted:
        df = df[df['author']!='[deleted]']

    print("getting subreddit level stats")
    incidence = CSR(df, 'subreddit_id', 'author_id', 'num_comments')

    with parallel(subStats) as g:
         results = g.wait({i: g(incidence[i,:].data) for i in df['subreddit_id'].unique()})

    output = pd.DataFrame.from_dict(results, orient='index')
    
    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit'] = output.index.map(lambda x: reverseSubIds[x])

    filename = cachePath(f"""{date}/subredditLevelStats.csv""")
    output.to_csv(filename)

    output_bucket = 'emg-subreddit-level-stats'
    uploadCommands(filename, output_bucket, date)

def runAuthorStats(date):
    """Computes statistics for each author in the dataset
    variables = ['aut_sub_count', 'aut_com_count', 'aut_com_entropy', 'aut_com_gini',
       'aut_com_blau', 'aut_insub']
    """
    print("getting author stats")
    input_bucket = 'emg-author-subreddit-pairs-ids'
    output_bucket = 'emg-author-stats'
    df = streamBlob(input_bucket, date)
    df = df.reset_index().astype({'author':str,'subreddit':str,'num_comments':int})

    df = df.sort_values(['author_id','subreddit_id'])
    incidence = CSR(df, 'author_id', 'subreddit_id', 'num_comments')
    
    results = {}
    for i in tqdm(df['author_id'].unique()): #parralel-ise?
        values = incidence[i,:].data
        results[i] = {'aut_sub_count':np.count_nonzero(values),
               'aut_com_count':np.sum(values),
               'aut_com_entropy': stats.entropy(values),    
                       'aut_com_gini': gini(values),
                       'aut_com_blau': blau(values)}
    
    output = pd.DataFrame.from_dict(results, orient='index')
    
    result = df.merge(output, left_on='author_id', right_index=True)
    result['aut_insub'] = result['num_comments']/result['aut_com_count']

    filename = cachePath(f"""{date}/authorStats.gzip""")
    print(f"""saving to {filename}""")
    result.to_csv(filename,compression='gzip')

    uploadCommands(filename, output_bucket, date)


def aggStats(values):
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

def runAggAuthorStats(date, drop_deleted=True):
    """
    TAKES THE AUTHOR LEVEL STATS PRODUCED BY getAuthorStats
    AND GETS SUBREDDIT LEVEL AGGREGATE AUTHOR STATS
    """
    print("getting aggregate level author stats")
    df = pd.read_csv(cachePath(f"""{date}/authorStats.gzip"""), compression='gzip')
    #df = df.reset_index().astype({'author':str,'subreddit':str,'aut_sub_count':int,
    #                                'aut_com_count':int, 'aut_com_entropy':int, 'aut_com_gini':int,
    #                                'aut_com_blau':int, 'aut_insub':int})
    if drop_deleted:
        df = df[df['author']!='[deleted]']
    
    variables = ['aut_sub_count', 'aut_com_count', 'aut_com_entropy', 'aut_com_gini',
       'aut_com_blau', 'aut_insub']
    
    stats = []
    for variable in variables:
        incidence = CSR(df, 'subreddit_id', 'author_id', variable)

        with parallel(aggStats) as g:
            results = g.wait({i: g(incidence[i,:].data) for i in df['subreddit_id'].unique()})

        output = pd.DataFrame.from_dict(results, orient='index')
        output.columns = [f"""{variable}_{stat}""" for stat in output.columns]

        stats.append(output)

    output = pd.concat(stats, axis=1)
    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit'] = output.index.map(lambda x: reverseSubIds[x])

    filename = cachePath(f"""{date}/authorLevelStats.csv""")
    output.to_csv(filename)

    output_bucket = 'emg-author-level-stats'
    uploadCommands(filename, output_bucket, date)


def run(date):
    createDirectories(date)
    runIDS(date)
    runAuthorStats(date)
    runAggAuthorStats(date)
    runSubredditStats(date)


