from gcs import *
from tools import *
import matplotlib.pyplot as plt


def run():
    df = streamBlob('emg-author-stats', date)
    result = getAuthorStats(df, date)
    getAggAuthorStats(df, date)

def getData():
    dates = getDates()
    data = {}
    for date in dates:
        if outputPath(f"""{date}/authorLevelStats.csv""").is_file():
            a = pd.read_csv(outputPath(f"""{date}/authorLevelStats.csv"""), index_col=0)
            s = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0)

            df = a.merge(s, on='subreddit').set_index('subreddit')
            data[date] = df

    return data

def timeline(data, variable):
    a = {}
    for k, v in data.items():
        a[k] = v[variable]

    return pd.DataFrame(a)  

def plot(timeline, variable, pct=True, save=False, rolling=True):
    df = timeline.copy()
    
    ylabel = variable
    if pct:
        df = df.rank(pct=True)
        ylabel = f"""{variable} percentile"""

    subset = getSubset(df).T
    if rolling:
        subset = subset.rolling(window=3).mean()

    plt.figure(figsize=(12,9))
    plt.plot(subset)
    plt.legend(subset.columns)
    plt.xticks(rotation='vertical')
    plt.xlabel('month')


    plt.ylabel(ylabel)

    if save:
        plt.savefig(figurePath(f"""{ylabel}.pdf"""))

    plt.show()


def run():
    data = getData()
    variable = 'aut_insub_median'

    t = timeline(date, variable)

    plot(t, variable)

    date = '2017-12'
    df = dfs[date]
