from pathlib import Path
from setup import cachePath, outputPath, createMonthDirectories
from sqlalchemy import create_engine
import time
import pandas as pd
from scipy import stats


def getAuthorStats(df, date, cache=False):
    """takes df with columns author, subreddit, num_author_comments
    calculates the num_comments, num_subreddits, and comment-entropy per author_stats
    merges author-level stats with a copy of the original df
    calculates the in-subreddit ratio for each author-subreddit pair
    returns df with columns author, subreddit, num_author_comments, author_total_subreddits, author_comment_entropy, author_insubreddit_ratio
    """
    print("calculating author-level stats...")
    copy = df.copy()
    author_stats = pd.DataFrame({'author_total_subreddits':copy.groupby('author')['subreddit'].count(),
                                 'author_total_comments':copy.groupby('author')['num_comments'].sum(),
                                 'author_comment_entropy':copy.groupby('author')['num_comments'].apply(
                                    lambda x: stats.entropy(x))

    # author_stats.to_csv(cachePath('{}/author_stats.csv'.format(date))) # too large?
                                    })
    copy = copy.merge(author_stats, left_on='author', right_index=True)
    copy['author_insubreddit_ratio']=copy['num_comments']/copy['author_total_comments']
    
    if cache:
        copy.to_csv(cachePath(date + '/author-subreddit-counts-plus-author-stats.csv'))

    return copy

def aggregateAuthorLevelStats(df, date, output=False):
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
        results_filename = '{}/aggregateAuthorLevelStats.csv'.format(date)
        results.to_csv(outputPath(results_filename))

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
    df["subreddit_author_entropy"] = df.groupby('subreddit')['num_comments'].apply(lambda x: stats.entropy(x))

    print("calculating subreddit author gini coefficient...")
    df["subreddit_author_gini"] = df.groupby('subreddit')['num_comments'].apply(lambda x: gini(list(x)))

    print("calculating subreddit author and comment counts...")
    df['subreddit_author_count'] = df.groupby('subreddit')['author'].count()
    df['subreddit_comment_count'] = df.groupby('subreddit')['num_comments'].sum()

    if output:
        results_filename = '{}/subredditLevelStats.csv'.format(date)
        df.to_csv(outputPath(results_filename))

def runSubredditStats(df, date, **kwargs):
    aggregateAuthorLevelStats(df, date, **kwargs)
    subredditLevelStats(df, date, **kwargs)

def loadSubredditStats(date):
    authorLevel_file = outputPath('{}/aggregateAuthorLevelStats.csv'.format(date))
    authorLevel = pd.read_csv(authorLevel_file, index_col=0, header=[0,1])

    subredditLevel_file = outputPath('{}/subredditLevelStats.csv'.format(date))
    subredditLevel = pd.read_csv(subredditLevel_file, index_col=0, header=[0,1])

    return authorLevel, subredditLevel

def mainStats(date):
    authorLevel, subredditLevel = loadSubredditStats(date)
    

def main():
    start_time = time.time()

    year, month = '2018','02'
    date = '{}-{}'.format(year, month)

    print(start_time, "fetching data from bigquery...")
    df = pd.read_csv(cachePath(date + '/author-subreddit-counts.csv'), index_col=0)

    copy = getAuthorStats(df, date, cache=True)
    runSubredditStats(copy, date, output=True)

    authorLevel, subredditLevel = loadSubredditStats(date)
