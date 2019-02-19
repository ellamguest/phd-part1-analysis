from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scripts.tools import getDates, outputPath, figurePath, addDefaults, getSubset


def compileData(level):
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

def compileRanks(level):
	"""level is 'subredditLevel' or 'authorLevel'"""
	df = pd.read_csv(outputPath(f"""{level}/fullDataset.gzip"""), compression='gzip', index_col=0)
	rank = df.select_dtypes('number').copy()
	for col in rank.columns:
		print(col)
		rank[col] = rank.groupby(df['date'])[col].rank(pct=True)
		
	rank['date'] = df['date']
	rank['subreddit'] = df['subreddit']

	rank.to_csv(outputPath(f"""{level}/ranks.gzip"""), compression='gzip')

def topSubs(date):
	df = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0)
	df['cumsum'] = df['comment_count'].cumsum()/df['comment_count'].sum()
	df = addDefaults(df)

	defaultless = df[df['default']==False]
	defaultless['cumsum'] = defaultless['comment_count'].cumsum()/defaultless['comment_count'].sum()
	top = defaultless[(defaultless['comment_count']>=defaultless['comment_count'].quantile(0.9))]

	print(f"""If we exclude defaults, there are {top.shape[0]} subreddits in the top decile by comment_count. Those still account for {top['cumsum'].iloc[-1]*100}% of comments on Reddit in {date}""")

	defaultComments = df[df['default']==True]['comment_count'].sum()
	totalComments = df['comment_count'].sum()

	pctDefault = df[df['default']==True].shape[0]/df.shape[0] * 100

	print(f"""In {date} only {pctDefault}% of subreddits were defaults but they accounted for {(defaultComments/totalComments)*100}% of all comments on Reddit""")

def deciles(series):
        return series.quantile([0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1])

def shadedDeciles(level):
	df = pd.read_csv(outputPath(f"""{level}/fullDataset.gzip"""), compression='gzip', index_col=0)
	d = df.select_dtypes('number').groupby(df['date']).apply(lambda x: deciles(x))
	d.index.names = ['date','decile']
	columns = d.columns
	if level == 'authorLevel':
		columns = [col for col in columns if 'median' in col]
	for v in columns:
		b = d[v].unstack()

		if 'count' in v:
			b = np.log(b)

		ymin, ymax = b[0].min(), b[1].max()
		plt.ylim([ymin,ymax*1.01])
		for n in reversed(b.columns):
			plt.plot(b[n], color = 'blue', alpha=n, label=f"""{n*100}%""")
		plt.xticks(rotation='70')
		plt.title(f"""{v} deciles over time""")

		plt.tight_layout()
		plt.savefig(figurePath(f"""{level}/{v}/deciles.pdf"""))

		plt.close()


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

def timelines(subset, v):
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

