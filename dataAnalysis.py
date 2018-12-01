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

def compileMonths():
    dates = [date for date in os.listdir('output') if date.startswith('20')]
    dfs = []
    for date in dates:
        df = pd.read_csv(outputPath(f"""{date}/subredditStats.csv"""), index_col=0)
        df['month'] = date
        dfs.append(df)
        
    return pd.concat(dfs)



def mainVariables(df):
    return df[['subreddit','subreddit_author_count',
                'subreddit_comment_count','subreddit_author_entropy',
                'subreddit_author_gini','subreddit_author_blau',
                'author_total_subreddits_median', 'author_total_comments_median',
                'author_comment_entropy_median', 'author_comment_gini_median',
                'author_comment_blau_median', 'author_insubreddit_ratio_median']]
    
def inverseVariable(df, variable):
    return 1-df[variable]

def correlations(df):
    corr = df.corr().stack().sort_values(ascending=False)
    corr = corr[corr!=1]
    return corr.drop_duplicates()


"""NORMALITY TESTS"""

def normTest(df, variable, log=False):
    x = df[variable]
    if log:
        x = np.log(x)
    k2, p = stats.normaltest(x)
    alpha = 1e-3
    print("p = {:g}".format(p))

    if p < alpha:  # null hypothesis: x comes from a normal distribution
        print(f"""NOT normally distributed""")
    else:
        print(f"""MAYBE normally distributed""")
        
        

        
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


def splitBimodal(df, variable):
    top = jan[jan['author_total_subreddits_median']>=7]
    bottom = jan[jan['author_total_subreddits_median']<7]
    
    
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

def plot(X):
    n = X.shape[1]
    width = int(n**.5)
    height = int(sp.ceil(n/width))
    fig, axes = plt.subplots(height, width, sharey=True)
    
    for (name, series), ax in zip(X.iteritems(), axes.flatten()):
        series.plot.hist(ax=ax, title=name)
        ax.set_ylabel('')
    
    for ax in axes.flatten()[n+1:]:
        ax.display(False)
    
    fig.subplots_adjust(hspace=.5)
    
def pca(main):
    X = main.copy()
    X['subreddit_author_count'] = X['subreddit_author_count'].apply(sp.log)
    X['subreddit_comment_count'] = X['subreddit_comment_count'].apply(sp.log)
    X['subreddit_author_blau'] = (1 - X['subreddit_author_blau']).apply(sp.log)
    
    signs = pd.Series(1, X.columns)
    signs[['subreddit_author_blau', 'subreddit_author_gini']] = -1
    
    # PCA
    mu, sigma = X.mean(), X.std()
    X = (X - mu)/sigma
    
    m = PCA().fit(X)
    U = pd.DataFrame(m.components_, None, X.columns)
    explained = pd.Series(m.explained_variance_ratio_)
    
    Y = pd.DataFrame(m.transform(X), X.index, None)
    
    # Abnormality in latent space (need to use less than D components!)
    Xhat = Y.dot(U) + m.mean_
    residual = (X - Xhat).pow(2).sum(1).pow(.5).sort_values()
    ranks = pd.Series(1, residual.sort_values().index).cumsum().pipe(lambda s: s/s.max())
    
    # Plotting latent factors
    Y.plot.scatter(0, 1)
    probes = ['the_donald', 'changemyview', 'latestagecapitalism']
    for p in probes:
        y = Y.rename(index=str.lower).loc[p]
        plt.annotate(p, (y[0], y[1]))
        
    
    # Controlling for size
    x = X.subreddit_author_count
    ys = X
    b = ys.corrwith(x)
    Xhat = b[None, :]*x[:, None]
    
    e = X - Xhat
    ranks = e.rank().pipe(lambda df: df/len(df))
    
    ranks.rename(index=str.lower).loc[probes].T.stack().sort_values()