#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 14:45:41 2018

@author: emg
"""

from gcs import fetchBlob, storeBlob, readBlob
from dataAnalysis import *
from tools import *
from dataProcessing import *
import pandas as pd
import networkx as nx
import scipy as sp


def getSample(df, num_subreddits=200):
    print("subsetting df")
    df = df[['subreddit','author','num_comments']]
    subset = subsetDF(df)
    
    print("getting sub ids and top subreddits")
    subIds = sortedIds(subset['subreddit'])
    subset['subreddit_id'] = subset['subreddit'].map(lambda x: subIds[x]) # gets setting with copywarning
    
    sample = subset[subset['subreddit_id']<num_subreddits]
    
    authorIds = sortedIds(sample['author'])
    sample['author_id'] = sample['author'].map(lambda x: authorIds[x]) # gets setting with copywarning
    
    
    return sample

def adjMatrix(sample):
    sample['1'] = 1
    incidence = makeCSR(sample, '1', sample['subreddit_id'],sample['author_id'])
  
    x = incidence.dot(incidence.T)
    
    m = pd.DataFrame(x.toarray())
    
    subLookup = {}
    for k,v in subIds.items():
        subLookup[v] = k
        
    m.index = m.index.map(lambda x: subLookup[x])
    m.columns = m.index
    
    return m

def add_edges_fast(names, adj):
    G = nx.Graph()
    G._node = {n: {} for n in names}
    G._adj = {n: {} for n in names}
    coo =  adj.tocoo()
    for u, v, w in zip(coo.row, coo.col, coo.data):
        if u != v:
            G._adj[names[u]][names[v]] = {'weight': w}

    return G

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

def subGraph(G, node):
    """get the ego network of node, including edges between alters
    using to compute meta-echo chamber density...
    """

def run():
    date = '2017-11'
    
    print(f"""opening df for {date}""")
    df = readBlob(date)
    
    num_subreddits=500
    sample = getSample(df, num_subreddits=num_subreddits)
    
    G = projectSub(sample)
    
def test():
    row = sample['subreddit_id']
    col = sample['author_id']
    data = sp.ones(len(row))
    incidence = sp.sparse.coo_matrix((data, (row, col)))
    adj = incidence.dot(incidence.T)
    
    