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

def compileAll():
    dates = [date for date in os.listdir('output') if date.startswith('20')]
    dfs = []
    for date in dates:
        df = pd.read_csv(outputPath(f"""{date}/subredditStats.csv"""), index_col=0)
        df['month'] = date
        dfs.append(df)
        
    return pd.concat(dfs)

def annualComparison():
    month = 11
    years = [2015,2016,2017,2018]
    dates = [getDate(year, month) for year in years]
    dfs = []
    for date in dates:
        df = pd.read_csv(outputPath(f"""{date}/subredditStats.csv"""), index_col=0)
        main = mainVariables(df)
        main['month'] = date
        dfs.append(main)
        
    return pd.concat(dfs)


def mainVariables(df):
    main = df[['subreddit','subreddit_author_count',
                'subreddit_comment_count','subreddit_author_entropy',
                'subreddit_author_gini','subreddit_author_blau',
                'aut_sub_count_median', 'aut_com_count_median',
                'aut_com_entropy_median', 'aut_com_gini_median',
                'aut_com_blau_median', 'aut_insub_median']] 

    main.columns = ['subreddit','num_auts',
                'num_coms','sub_ent',
                'sub_gini','sub_blau',
                'med_num_subs', 'med_num_coms',
                'med_ent', 'med_gini',
                'med_blau', 'med_insub']
    
    return main.set_index('subreddit').drop('SubredditSimulator')
    
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
        
def sizeControlled(df, size_variable='num_auts'):
    X = df.copy()
    
    x = X[size_variable]
    ys = X
    b = ys.corrwith(x)
    Xhat = b[None, :]*x[:, None]
    
    e = X - Xhat
    
    ranks = e.rank().pipe(lambda df: df/len(df))

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

def pcaPlot(U):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    for i in U.index:
        U.loc[i].plot(kind='barh', title=f"""PCA cluster {i}""", ax=axes[i])
        #plt.show()
    
def confoundLess(df):
    """
    auth_com_blau_median and auth_com_entropy_median ~ 96% correlated
    - choose blau because simpler metric to interpret?
    
    subreddit_author_blau and subreddit_author_entropy ~ 89% correlated
    - also choose blau
    
    aut_com_count_median, aut_sub_count_median ~ 96% correlated
    - choose sub count bc more interested in diversity than activity
    
    subreddit_author_gini and subreddit_author_blau ~73% correlated
    - diagnose outliers where they are least correlated
    - what are the measures accounting for differently?
    - probably still choose blau? unless can sufficiently argue they highlight
    -- different processes
    
    auth_com_entropy_median and aut_sub_count_median ~89% correlated
    - so definitely need to account for sub count when interpreting entropy
    
    basics
    - size = sub count
    - diversity = blau
    - at both sub and author levels
    """
    
    return df[['num_auts', 
               'sub_blau', 
               'med_num_subs', 
               'med_blau',
               'med_insub']]
    
    
def compareSubs(df):
    subs = subs = ['The_Donald', 'Libertarian','Conservative','changemyview','socialism','SandersForPresident','LateStageCapitalism']
    return df.loc[subs]