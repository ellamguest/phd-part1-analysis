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

def variableStats(df, variable):
    incidence = CSR(df, 'subreddit_id', 'author_id', variable)

    with parallel(aggStats) as g:
        results = g.wait({i: g(incidence[i,:].data) for i in df['subreddit_id'].unique()})

    output = pd.DataFrame.from_dict(results, orient='index')
    output.columns = [f"""{variable}_{stat}""" for stat in output.columns]

    return output

def parVal(date, num_subreddits, cache=True):
   """
    TAKES THE AUTHOR LEVEL STATS PRODUCED BY getAuthorStats
    AND GETS SUBREDDIT LEVEL AGGREGATE AUTHOR STATS
    """
    print("getting aggregate level author stats")

    df = pd.read_csv(cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip"""),compression='gzip')
    
    variables = ['aut_sub_count', 'aut_com_count', 'aut_com_entropy', 'aut_com_gini',
       'aut_com_blau', 'aut_insub']

    with parallel(variableStats) as g:
        results = g.wait({i: g(df, i) for i in variables})

    output = pd.concat(results, axis=1)
    output.columns = output.columns.droplevel(0)
    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit'] = output.index.map(lambda x: reverseSubIds[x])

    if cache:
        output.to_csv(outputPath(f"""{date}/top_{num_subreddits}_authorLevelStats.csv"""))
        
    return output


def aStats(values):
    return {'aut_sub_count':np.count_nonzero(values),
               'aut_com_count':np.sum(values),
               'aut_com_entropy': stats.entropy(values),
                       'aut_com_gini': gini(values),
                       'aut_com_blau': blau(values)}

def parAuthor(df, date, num_subreddits, cache=True):
    incidence = CSR(df, 'author_id', 'subreddit_id', 'num_comments')
    
    with parallel(aStats) as g:
         results = g.wait({i: g(incidence[i,:].data) for i in df['author_id'].unique()})

    output = pd.DataFrame.from_dict(results, orient='index')

def getAuthorStats(df, date, num_subreddits, cache=True):
    """slicing the csr is still the least efficient part of data processing
    NEEDS TO REPLACE TQDM WITH PARALLEL BUT CRASHING???
    """
    print("getting author stats") # LONGEST PART, STILL TAKES A FEW MINUTES

    incidence = CSR(df, 'author_id', 'subreddit_id', 'num_comments')
    
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

    if cache:
        print("storing author stats")
        result.to_csv(cachePath(f"""{date}/top_{num_subreddits}_authorStats.gzip"""),compression='gzip')

    return result