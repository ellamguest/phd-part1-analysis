

def runSubredditStats(date, drop_deleted=True):
    """Computes statistics for each author in the dataset
    variables = ['author_count',comment_count','entropy','gini','blau']
    """
    input_bucket = 'emg-author-subreddit-pairs-ids'
    output_bucket = 'emg-subreddit-level-stats'
    df = streamBlob(input_bucket, date)
    df = df.reset_index().astype({'author':str,'subreddit':str,'num_comments':int})

    if drop_deleted:
        df = df[df['author']!='[deleted]']

    print("getting subreddit level stats")
    incidence = CSR(df, 'subreddit_id', 'author_id', 'num_comments')

    with parallel(subStats) as g:
         results = g.wait({i: g(incidence[i,:].data) for i in df['subreddit_id'].unique()})

    output = pd.DataFrame.from_dict(results, orient='index')
    
    reverseSubIds = dict(zip(df['subreddit_id'],df['subreddit']))
    output['subreddit'] = output.index.map(lambda x: reverseSubIds[x])

    filename = outputPath(f"""{date}/subredditLevelStats.csv""")
    output.to_csv(filename)

    uploadCommands(filename, output_bucket, date)