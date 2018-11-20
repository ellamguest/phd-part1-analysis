#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import scipy as sp
def projectSub(df, top='subreddit',bottom='author'):
    top = df[top].values
    bot = df[bottom].values

    top_unique, top_indices = sp.unique(top, return_inverse=True)
    bot_unique, bot_indices = sp.unique(bot, return_inverse=True)

    data = sp.ones(len(top))
    incidence = sp.sparse.coo_matrix((data, (top_indices, bot_indices)))
    adj = incidence.dot(incidence.T)

    G = add_edges_fast(top_unique, adj)

    return G


date = getDate('2018', '01')
df = pd.read_csv(cachePath(f"""{date}/author-subreddit-counts-plus-author-stats.csv"""), index_col=0)


start = time.time()

variable = 'num_comments'
top = df['subreddit'].values
bot = df['author'].values
top_unique, top_indices = sp.unique(top, return_inverse=True)
bot_unique, bot_indices = sp.unique(bot, return_inverse=True)
data = df[variable]
incidence = sp.sparse.csr_matrix((data, (top_indices, bot_indices)))

ids = dict(zip(set(top_indices), top_unique))
results = {}
for k,v in ids.items():
    subset = incidence[24].toarray() # 'AskReddit'
    values = subset[np.nonzero(subset)]
    
    results[v] = {'subreddit_author_count':np.count_nonzero(values),
           'subreddit_comment_count':np.sum(values),
           'subreddit_author_entropy': stats.entropy(values),
                   'subreddit_author_gini': gini(values)}
    
    output = pd.DataFrame.from_dict(results).T


end = time.time()
print(f"""took {end-start}""")

def subredditLevelStats(df, date, output=False):
    start = time.time()
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
    
    end = time.time()
    print(f"""took {end-start}""")

    if output:

        results.to_csv(
                outputPath(f"""{date}/subredditLevelStats.csv"""))