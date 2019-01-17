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


def mainVariables(df):
    main = df.copy()
    cols = main.columns

    subCols = ['author_count', 'comment_count', 'entropy', 'gini', 'blau']
    medianCols = [col for col in cols if col.endswith('median')]
    mainCols = subCols + medianCols
    
    return main[mainCols]
    
def correlations(df):
    corr = df.corr().stack().sort_values(ascending=False)
    corr = corr[corr!=1]
    return corr.drop_duplicates()
    
def predictedValues(df):
    X = df["subreddit_comment_count"]
    
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
    
def andy(main):
    X = main.copy()
    
    X['subreddit_author_count'] = X['subreddit_author_count'].apply(sp.log)
    X['subreddit_comment_count'] = X['subreddit_comment_count'].apply(sp.log)
    X['subreddit_author_blau'] = -1*(1-X['subreddit_author_blau']).apply(sp.log)
    X['subreddit_author_gini'] = 1-X['subreddit_author_gini']
    
    corrs = X.corr().stack().sort_values(ascending=False)
    corrs = corrs.drop_duplicates()
    corrs = corrs[corrs<1]
    corrs.head(10)

    # PCA
    slim = confoundLess(df)
    X = slim.copy()
   
    U, explained, Y = pca(X)
    
    
    
    explained = pd.Series(m.explained_variance_ratio_)
    
    Y = pd.DataFrame(m.transform(X), X.index, None)
    
    
    PCA_results = Y.loc[subs][[0,1]]
    
    # Abnormality in latent space (need to use less than D components!)
    Xhat = Y.dot(U) + m.mean_
    residual = (X - Xhat).pow(2).sum(1).pow(.5).sort_values()
    PCA_ranks = pd.Series(1, residual.sort_values().index).cumsum().pipe(lambda s: s/s.max())
    
    # Plotting latent factors
    Y.plot.scatter(0, 1)
    probes = ['the_donald', 'changemyview', 'SandersForPresident', 'latestagecapitalism']
    for p in subs:
        y = Y.loc[p]
        plt.annotate(p, (y[0], y[1]))
        
def sizeControlled(df, size_variable='author_count'):
    X = df.copy().select_dtypes('number')
    
    x = X[size_variable]
    ys = X
    b = ys.corrwith(x)
    Xhat = b[None, :]*x[:, None]
    
    e = X - Xhat
    
    ranks = e.rank().pipe(lambda df: X/len(X))

    return ranks

    
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
    
def getSubs(df):
    subs = ['The_Donald', 'Libertarian','Conservative','changemyview','socialism','SandersForPresident','LateStageCapitalism']
    return df.loc[subs]

def compareSubs():
    dates = getDates()

    d = {}
    for date in dates:
        df = loadStats(date)
        subset = getSubs(df)
        d[date] = getDict(subset.fillna(0))

    return d

def loadStats(date, num_subreddits=500):
    if outputPath(f"""{date}/top_{num_subreddits}_fullStats.csv""").is_file():
        df = pd.read_csv(outputPath(f"""{date}/top_{num_subreddits}_fullStats.csv"""), index_col=0)
        df.index.name = 'subreddit_id'
        df = df.set_index('subreddit',drop=False)

        return df
    else:
        print("data not found")

getDates = lambda: sorted(next(os.walk("cache"))[1])

def getDict(df):
        main = mainVariables(df)
        U, explained, Y = pca(main, n_components=3)

        return {'df':df, 'main':main, 'U':U, 'explained':explained, 'Y':Y}

def loadMonths(num_subreddits=500):
    dates = getDates()
    results = {}
    for date in dates:
        df = loadStats(date, num_subreddits)
        d = getDict(df)

        results[date] = d

    return results

def separateDefaults(df):
    defaults = """Art+AskReddit+DIY+Documentaries+EarthPorn+Futurology+GetMotivated+IAmA+InternetIsBeautiful+Jokes+\
LifeProTips+Music+OldSchoolCool+Showerthoughts+TwoXChromosomes+UpliftingNews+WritingPrompts+\
announcements+askscience+aww+blog+books+creepy+dataisbeautiful+explainlikeimfive+food+funny+\
gadgets+gaming+gifs+history+listentothis+mildlyinteresting+movies+news+nosleep+nottheonion+\
personalfinance+philosophy+photoshopbattles+pics+science+space+sports+television+tifu+\
todayilearned+videos+worldnews""".split('+')
    df['default'] = df['subreddit'].apply(lambda x: True if x in defaults else False)

    default = df[df.index.isin(defaults)]
    nondefault = df[~df.index.isin(defaults)]

    return default, nondefault

def compareDefaults(df):
    default, nondefault = separateDefaults(df)
    dd = getDict(default)
    nd = getDict(nondefault)

    return dd, nd


def compareExplained(d):
    dates = getDates()
    e = [d[date]['explained'] for date in dates]

    trend = pd.concat(e, axis=1)
    trend.columns = dates

    trend.T.plot(title='explained')

def variableTrends(d):
    dates = getDates()

    variables = d[dates[0]]['U'].columns
    for variable in variables:
        v = [d[date]['U'][variable] for date in dates]

        trend = pd.concat(v, axis=1)
        trend.columns = dates

        trend.T.plot(title=variable)

def singleMonth(date):
        df = loadStats(date, num_subreddits)
        df.index.name = 'subreddit_id'
        df = df.set_index('subreddit',drop=False)
        df['gini'] = 1-df['gini']
        main = mainVariables(df)
        U, explained, Y = pca(main, n_components=3)
        subset = getSubs(df)