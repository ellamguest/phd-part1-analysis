from pathlib import Path
from setup import cachePath, credentialsPath, dataPath
from sqlalchemy import create_engine
import time


def getAuthorStats(df):
    """takes df with columns author, subreddit, num_author_comments
    calculates the num_comments, num_subreddits, and comment-entropy per author_stats
    merges author-level stats with a copy of the original df
    calculates the in-subreddit ratio for each author-subreddit pair
    returns df with columns author, subreddit, num_author_comments, author_total_subreddits, author_comment_entropy, author_insubreddit_ratio
    """
    print(time.time(), "calculating author-level stats...")
    df = df.copy()
    author_stats = pd.DataFrame({'author_total_subreddits':copy.groupby('author')['subreddit'].count(),
                                 'author_total_comments':copy.groupby('author')['num_comments'].sum(),
                                 'author_comment_entropy':copy.groupby('author')['num_comments'].apply(
                                    lambda x: stats.entropy(x))

    # author_stats.to_csv(cachePath('{}/author_stats.csv'.format(date))) # too large?
                                    })
    copy = copy.merge(author_stats, left_on='author', right_index=True)
    copy['author_insubreddit_ratio']=copy['num_comments']/copy['author_total_comments']

    return copy

def aggregateAuthorLevelStats(df, date):
    print(time.time(), "getting distribution stats on author num comments, num subreddits, and insubreddit ratio...")
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

    print(time.time(), "caching results file...")
    results_filename = '{}/aggregateAuthorLevelStats.csv'.format(date)
    results.to_csv(cachePath(results_filename))


def subredditLevelStats(df, date):
    print(time.time(), "calculating subreddit author entropy...")
    results["subreddit_author_entropy"] = df.groupby('subreddit')['num_comments'].apply(lambda x: stats.entropy(x))

    print(time.time(), "calculating subreddit author gini coefficient...")
    results["subreddit_author_gini"] = df.groupby('subreddit')['num_comments'].apply(lambda x: gini(list(x)))

    results.to_csv(cachePath(results_filename))
    print(time.time(), "calculating subreddit author and comment counts...")
    results['subreddit_author_count'] = df.groupby('subreddit')['author'].count()
    results['subreddit_comment_count'] = df.groupby('subreddit')['num_comments'].sum()

    results_filename = '{}/subredditLevelStats.csv'.format(date)
    results.to_csv(cachePath(results_filename))

def runSubredditStats(df, date):
    aggregateAuthorLevelStats(df, date)
    subredditLevelStats(df, date)

def loadSubredditStats(date):
    fn1 = '{}/aggregateAuthorLevelStats.csv'.format(date)
    authorLevel = pd.read_csv(fn1, index_col=0, header=[0,1]

    fn2 = '{}/subredditLevelStats'.format(date)
    subredditLevel = pd.read_csv(fn1, index_col=0, header=[0,1]

    return authorLevel, subredditLevel

def mainStats(date):
    authorLevel, subredditLevel = loadSubredditStats(date)
    

def main():
    start_time = time.time()

    year, month = '2018','01'
    date = '{}-{}'.format(year, month)

    print(start_time, "fetching data from bigquery...")
    df = fetchQuery("""SELECT *
    FROM `author-subreddit-counts.{}.{}`
    """.format(year, month))

    copy = getAuthorStats(df)
    runSubredditStats(copy, date)

    authorLevel, subredditLevel = loadSubredditStats(date)
