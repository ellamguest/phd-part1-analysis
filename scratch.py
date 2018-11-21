#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import scipy as sp
from scipy import stats

author_stats = pd.DataFrame({'author_total_subreddits':copy.groupby('author')['subreddit'].count(),
                                 'author_total_comments':copy.groupby('author')['num_comments'].sum(),
                                 'author_comment_entropy':copy.groupby('author')['num_comments'].apply(
                                    lambda x: stats.entropy(x))})


incidence = makeCSR(df, 'num_comments')
    
top_unique, top_indices = sp.unique(df['subreddit'], return_inverse=True)
ids = dict(zip(set(top_indices), top_unique))
results = {}
for k,v in ids.items():
    subset = incidence[k].toarray()
    values = subset[np.nonzero(subset)]
    
    results[v] = {'subreddit_author_count':np.count_nonzero(values),
           'subreddit_comment_count':np.sum(values),
           'subreddit_author_entropy': stats.entropy(values),
                   'subreddit_author_gini': gini(values)}


    
    