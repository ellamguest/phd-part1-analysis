import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from tools import outputPath
import pandas as pd



def compileMonths():
    dates = [date for date in os.listdir('output') if date.startswith('20')]
    dfs = []
    for date in dates:
        df = pd.read_csv(outputPath(f"""{date}/subredditStats.csv"""), index_col=0)
        df['month'] = date
        dfs.append(df)
        
    return pd.concat(dfs)



def mainVariables(df):
    return df[['subreddit','month','subreddit_author_count',
                'subreddit_comment_count','subreddit_author_entropy',
                'subreddit_author_gini','subreddit_author_blau',
                'author_total_subreddits_median', 'author_total_comments_median',
                'author_comment_entropy_median', 'author_comment_gini_median',
                'author_comment_blau_median', 'author_insubreddit_ratio_median']]
    
def getPercentiles(df):
    td_p = {}
    for variable in df.columns:
        td_p[variable] = stats.percentileofscore(df[variable].values,
            df.loc['The_Donald'][variable])

    cmv_p = {}
    for variable in df.columns:
        cmv_p[variable] = stats.percentileofscore(df[variable].values,
             df.loc['changemyview'][variable])

    return pd.DataFrame({'td':td_p,'cmv':cmv_p})
    
    
def predictedValues(df):
    X = df["subreddit_comment_count"]
    
    preds_df = {}
    for variable in df.columns:
        y = df[variable]
        model = sm.OLS(y, X).fit()
        preds_df[variable] = model.predict(X)
        
    return pd.DataFrame(preds_df)

def expectedVsObserved(df):
    stats = df.set_index('subreddit').drop('month',axis=1)
    
    observedPercentiles = getPercentiles(stats)
    expectedValues = predictedValues(stats)
    expectedPercentiles = getPercentiles(expectedValues)
    
    return observedPercentiles, expectedPercentiles
