import pandas as pd
import numpy as np
import json
import networkx as nx
from networkx.readwrite import json_graph
from sqlalchemy import create_engine
import scipy as sp

def removeAuthors(df):
    skipAuthors = loadSQL('skipAuthors','reference')['0']
    subset = df[~df['author'].isin(skipAuthors)].copy()

    return subset

def removeDefaults(df):
    defaults = list(loadSQL('defaults','allEdges')['subreddit'])
    subset = df[~df['subreddit'].isin(defaults)].copy()

    return subset

""" NETWORKS """
def getBipartiteNetwork(df):
    """old"""
    B = nx.Graph()
    B.add_nodes_from(df.subreddit.unique(), bipartite=0)
    B.add_nodes_from(df.author.unique(), bipartite=1)
    B.add_weighted_edges_from(
            list(df.itertuples(index=False, name=None)))

    return B

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

def removeIsolates(G):
    isolates = list(nx.isolates(G))
    G.remove_nodes_from(isolates)

    return G

def setAttributes(G):
    nx.set_node_attributes(G, 'group', 1)
    for n in G:
        G.nodes[n]['id'] = n

    return G

def subsetNet(G):
    G = dropSingleEdges(G)
    G = removeIsolates(G)
    G = setAttributes(G)

    return G

def dropSingleEdges(G):
    edges = list(G.edges(data=True))
    single_edges = [edge for edge in edges if edge[2]['weight'] == 1]

    G.remove_edges_from(single_edges)

    return G

def saveGraphJson(G, filename):
    d = json_graph.node_link_data(G)
    json.dump(d, open(filename, 'w'))

def separateUpperQuartile(df, col):
    upper_bound = df.quantile(.75)[col]
    lq = s[s[col]<upper_bound]
    uq = s[s[col]>=upper_bound]

    return lq, uq

def removeUpperQuartileNodes(G):
    s = pd.DataFrame.from_dict(dict(G.degree()), orient='index')
    lq, uq = separateUpperQuartile(s, 0)

    G.remove_nodes_from(uq.index)

def makeNetworkGraph(data, filename):
    G = projectSub(data)
    G.remove_node('changemyview')
    removeUpperQuartileNodes(G)

    defaults = list(loadSQL('defaults','reference')['defaults'])
    G.remove_nodes_from(defaults) # too connected
    G.remove_node('politics') # too connected
    subset = subsetNet(G)

    saveGraphJson(subset, '{}.json'.format(filename))
