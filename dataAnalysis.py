import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dataProcessing import outputPath
import pandas as pd
from scipy import stats
import scipy as sp
from sklearn.decomposition import PCA
import statsmodels.api as sm
from tools import *

def loadStats(date):
    if outputPath(f"""{date}/fullStats.csv""").is_file():
        df = pd.read_csv(outputPath(f"""{date}/fullStats.csv"""), index_col=0)
        df.index.name = 'subreddit_id'
        df = df.set_index('subreddit',drop=False)

        return df
    else:
        print("data not found")


def loadMonths():
    dates = getDates()
    results = {}
    for date in dates:
        if outputPath(f"""{date}/fullStats.csv""").is_file():
           df = loadStats(date, num_subreddits)

           results[date] = df

    return results

def mainVariables(df):
    main = df.copy()
    cols = main.columns

    subCols = ['author_count', 'comment_count', 'entropy', 'gini', 'blau']
    medianCols = [col for col in cols if col.endswith('median')]
    mainCols = subCols + medianCols
    
    return main[mainCols]
    
def predictedValues(df, predictor):
    X = df[predictor]
    
    preds_df = {}
    for variable in df.select_dtypes(['float64','int64']).columns:
        y = df[variable]
        model = sm.OLS(y, X).fit()
        preds_df[variable] = model.predict(X)
        
    return pd.DataFrame(preds_df)

def expectedVsObserved(df):
    observedPercentiles = df.rank()
    expectedValues = predictedValues(df)
    expectedPercentiles = expectedValues.rank()
    
    return observedPercentiles, expectedPercentiles

def variableTrends(d):
    dates = getDates()

    variables = d[dates[0]]['U'].columns
    for variable in variables:
        v = [d[date]['U'][variable] for date in dates]

        trend = pd.concat(v, axis=1)
        trend.columns = dates

        trend.T.plot(title=variable)

"""
PCA
"""

def getDict(df):
        main = mainVariables(df)
        U, explained, Y = pca(main, n_components=3)

        return {'df':df, 'main':main, 'U':U, 'explained':explained, 'Y':Y}

def pca(df, n_components=3):
    mu, sigma = df.mean(), df.std()
    
    # get normalised standard score
    X = (df - mu)/sigma
    
    m = PCA(n_components).fit(X)
    
    #components
    U = pd.DataFrame(m.components_, None, X.columns)
    
    explained = pd.Series(m.explained_variance_ratio_)
    
    Y = pd.DataFrame(m.transform(X), X.index, None)
    
    return U, explained, Y

def pcaPlot(U, date=None, save=False):
    fig, axes = plt.subplots(nrows=3, ncols=1)
    for i in U.index:
        U.loc[i].plot(kind='barh', title=f"""PCA component {i}""", ax=axes[i], figsize=(8,12))
    fig.suptitle(date, fontsize=14)
    plt.tight_layout()
    plt.subplots_adjust(top=0.9)
    if save:
        plt.savefig(figurePath(f"""{date}/PCA_components.png"""))

def pcaTrends(d, num_components=3):
    dates = getDates()
    results={}
    for i in range(num_components):
        v = [d[date]['Y'][i] for date in dates]
        trend = pd.concat(v, axis=1)
        trend.columns = dates

        results[i]=trend

    return results

def compareExplained(d):
    dates = getDates()
    e = [d[date]['explained'] for date in dates]

    trend = pd.concat(e, axis=1)
    trend.columns = dates

    trend.T.plot(title='explained')

