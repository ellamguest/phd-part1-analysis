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

    with parallel(authorStats) as g:
        results = g.wait({i: g(incidence[i,:].data) for i in row.unique()})

    return pd.DataFrame.from_dict(results, orient='index')
    
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

