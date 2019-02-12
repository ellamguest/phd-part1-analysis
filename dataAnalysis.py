from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from tools import getDates, outputPath, figurePath, addDefaults, getSubset


def updateData(level):
	"""
	level is 'subredditLevel' or 'authorLevel'
	"""
	dates = getDates()
	data = []
	for date in dates:
		filename = outputPath(f"""{date}/{level}Stats.csv""")
		if filename.is_file():
			df = pd.read_csv(filename, index_col=0)
			df['date'] = date
			data.append(df)
        
	df = pd.concat(data)

	Path(f"""output/{level}""").mkdir(exist_ok=True, parents=True)
	df.to_csv(outputPath(f"""{level}/fullDataset.gzip"""), compression='gzip')

def updateRanks(level):
    """
    level is 'subredditLevel' or 'authorLevel'
    """
    df = pd.read_csv(outputPath(f"""{level}/fullDataset.gzip"""), compression='gzip', index_col=0)

    rank = df.select_dtypes('number').copy()
    for col in rank.columns:
            print(col)
            rank[col] = rank.groupby('date')[col].rank(pct=True)
	rank['date'] = df['date']
	rank['subreddit'] = df['subreddit']

	rank.to_csv(outputPath(f"""{level}/ranks.gzip"""), compression='gzip')

def deciles(series):
        return series.quantile([0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1])

def shadedDeciles(level):
	df = pd.read_csv(outputPath(f"""{level}/fullDataset.csv"""), index_col=0)
	d = df.select_dtypes('number').groupby(df['date']).apply(lambda x: deciles(x))
	d.index.names = ['date','decile']
	for v in d.columns:
		b = d[v].unstack()
		b = np.log(b).fillna(0).replace(np.inf, 0)

		ymin, ymax = b[0].min(), b[1].max()
		plt.ylim([ymin,ymax*1.01])
		for n in reversed(b.columns):
			plt.plot(b[n], color = 'blue', alpha=n, label=f"""{n*100}%""")
		plt.xticks(rotation='70')
		plt.title(f"""{v} (log) deciles over time""")

		plt.tight_layout()
		plt.savefig(figurePath(f"""{level}/{v}/deciles-log.pdf"""))

		plt.close()


def decileCrossTabs():

	test = df[df['date']=='2015-11']
	q = test.select_dtypes('number').apply(lambda x: pd.qcut(x, 10, duplicates='drop', labels=False))
	sns.jointplot(q['author_count'], q['comment_count'], kind="hex", color="k");


def subsetting(df):
        d = df.select_dtypes('number').apply(lambda x: deciles(x))
        subset = df[(df['author_count']>=5)&(df['comment_count']>=10)]
        subset = addDefaults(subset)

def trends():
        data = getData("authorLevelStats.csv")
        for v in data.values():
                v['subRank'] = getSubset(v['rank'])

        x = {}
        for k,v in data.items():
                x[k] = v['subRank']
        x = pd.concat(x)
        x = x.reset_index()

        x['date'] = pd.to_datetime(x['level_0'])
        x = x.set_index('date')

        return x

def timelines(x, v):
        Path(f"""figures/authorLevel/{v}/timeline""").mkdir(exist_ok=True, parents=True)
        x.groupby('subreddit')[v].plot(legend=True)
        plt.title(f"""{v} over time""")
        plt.tight_layout()
        plt.savefig(figurePath(f"""authorLevel/{v}/timeline/subset.pdf"""))

        plt.close()

        x[x['subreddit']!='SandersForPresident'].groupby('subreddit')[v].plot(legend=True)
        plt.title(f"""{v} over time""")
        plt.tight_layout()
        plt.savefig(figurePath(f"""authorLevel/{v}/timeline/subset-no-SFP.pdf"""))

        plt.close()


def month(date):
        sub = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0).set_index('subreddit')


        getSubset(data[date]['rank'])

        rank = sub.select_dtypes('number').rank(pct=True)
