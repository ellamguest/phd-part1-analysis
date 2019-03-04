#from scripts.dataAnalysis import *
import numpy as np
import pandas as pd	
import matplotlib.pyplot as plt
from scripts.tools import addDefaults, figurePath, cachePath, getSubset
from pathlib import Path
import seaborn as sns

latexPath = lambda filename: Path(f"""latex/{filename}""")

""" TOOLS """
def getLog(df):
	""" for any count variable "col" in the df add a column "col_log10" """
	copy = df.copy()
	for col in copy.columns:
		if 'count' in col:
			copy[f"""{col}_log10"""] = np.log10(copy[col])

	return copy

def desc(df):
	"""
	return descriptive statistics for the df
	dropping any variables ending with "count" (assume already ran getLog)
	dropping 'count' from stats
	"""
	cols = df.columns
	subset = df[[col for col in cols if col.endswith('count') is False]]
	return subset.describe().drop('count')


def subsetDecile(df, variable='author_count'):
	"""
	sorting df by variable
	returns top decile of values by variable
	"""
	copy = df.copy().sort_values(variable, ascending=False).reset_index(drop=False)
	tenth = copy.shape[0]/10
	
	return copy.loc[:np.round(tenth)-1].set_index('subreddit')

def tableFile(body, caption=None, label=None):
	"""
	adds header and footer to give latex table body
	opt: given caption and label for latex table
	"""
	header = "\\begin{table}\n\centering\n"
	footer = f"""\caption{{{caption}}}\n\label{{{label}}}\n\end{{table}}"""
	text = header + body + footer

	with open(latexPath(f"""{label}.tex"""), "w") as f:
		f.write(text)

def settings():
	"""
	defines matplotlib parameters for better plots
	"""

	pd.set_option('display.float_format', lambda x: '%.3f' % x)
	plt.style.use(['seaborn-paper'])
	plt.rc('font', serif='Computer Modern Roman')
	plt.rc('figure', figsize=(3,3))
	plt.rc('font', size=20)

def histograms(df):
	"""
	saves histogram plots for all columns in df
	y axis range 0-1
	saves in path "latex/hist"
	applied custom plt setting
	"""
	settings()
	Path(f"""latex/hist""").mkdir(exist_ok=True, parents=True)

	for v in df.select_dtypes('number').columns:
		data = df[v]
		filename = latexPath(f"""hist/{v}.pdf""")
		plt.hist(data, color='grey')
		plt.xlabel(v)
		xmin, xmax = data.min(), data.max()
		plt.xlim(xmin, xmax)

		locs, labels = plt.yticks()
		y = np.arange(0.2, 1.1, step=0.2)
		y_scaled = y * locs.max()
		y_labels = [x.round(1) for x in y]
		plt.yticks(y_scaled, y_labels)

		plt.tight_layout()
		plt.savefig(filename, bbox_inches='tight')
		plt.close()

def kde(df, subset='active'):
	"""
	saves kernel density estimate plots for all columns in df
	y axis range 0-1
	saves in path "latex/kde"
	applied custom plt setting
	"""
	settings()
	Path(f"""latex/kde""").mkdir(exist_ok=True, parents=True)

	for v in df.select_dtypes('number').columns:
		data = df[v]
		filename = latexPath(f"""kde/{v}-{subset}.pdf""")
		sns.kdeplot(data, shade=True, color='grey', legend=False)
		plt.xlabel(v)
		xmin, xmax = data.min(), data.max()
		plt.xlim(xmin, xmax)

		locs, labels = plt.yticks()
		y = np.arange(0.2, 1.1, step=0.2)
		y_scaled = y * locs.max()
		y_labels = [x.round(1) for x in y]
		plt.yticks(y_scaled, y_labels)

		plt.tight_layout()
		plt.savefig(filename, bbox_inches='tight')
		plt.close()


"""SUBREDDIT LEVEL"""
def subData(date):
	"""
	returns dictionary of subreddit data dataframes for date
	df = full dataset
	active = top decile of subreddits
	defaults = defaults only
	"""
	settings()

	date = "2018-02"
	df = pd.read_csv(cachePath(f"""{date}/subredditLevelStats.csv"""), index_col=0)
	df = addDefaults(df)
	df = df[~df['subreddit'].str.startswith('u_')].set_index('subreddit') # dropping homepages
	df = getLog(df)

	df['entropy_max'] = df['author_count'].apply(lambda x: np.log(x))
	df['entropy_norm'] = df['entropy']/df['entropy_max'].fillna(0)

	defaults = df[df['default']==True].sort_values('author_count')

	active = subsetDecile(df)

	return {'date':date,
			'df':df,
			'active':active,
			'defaults':defaults,
			'pol':getSubset(df),
			'pol_active_ranks':getSubset(active.rank(pct=True))}

def subTables(data):
	"""
	saves latex table files for subreddit level data subsets
	"""
	Path(f"""latex/table""").mkdir(exist_ok=True, parents=True)
	tableFile(desc(data['df']).T.to_latex(),caption="Descriptive Statistics for all Subreddits", label="table/all")
	tableFile(desc(data['defaults']).T.to_latex(),caption="Descriptive Statistics for Default Subreddits", label="table/defaults")
	tableFile(desc(data['active']).T.to_latex(),caption="Descriptive Statistics for Top Decile of Subreddits by Author Count", label="table/active")

def runSub(date):
	""" runs subreddit level data plots """
	sub = subData(date)
	subTables(sub)
	kde(sub['active'])


##### AUTHOR LEVEL
def autData(date):
	"""
	returns dictionary of author data dataframes for date
	df = full dataset
	active = top decile of subreddits
	defaults = defaults only
	"""
	df = pd.read_csv(cachePath(f"""{date}/authorLevelStats.csv"""), index_col=0)
	df = addDefaults(df)
	df = df[~df['subreddit'].str.startswith('u_')].set_index('subreddit')
	
	subset = df[[c for c in df.columns if 'median' in c]]
	subset.columns = [c.replace('_median','') for c in subset.columns]
	subset = getLog(subset)

	defaults = df[df['default']==True]

	sub = subData(date)
	activeSubs = sub['active'].index
	active = subset.loc[activeSubs]

	return {'date':date,
			'df':df,
			'active':active,
			'defaults':defaults,
			'pol':getSubset(df),
			'pol_active_ranks':getSubset(active.rank(pct=True))}

def autTables(data):
	"""
	saves latex table files for author level data subsets
	"""
	Path(f"""latex/table""").mkdir(exist_ok=True, parents=True)
	tableFile(desc(data['df']).to_latex(),caption="Descriptive Statistics of Author Medians for all Subreddits",label='table/author-medians:all')
	tableFile(desc(data['defaults']).to_latex(),caption="Descriptive Statistics of Author Medians for Default Subreddits",label='table/author-medians:defaults')
	tableFile(desc(data['active']).to_latex(),caption="Descriptive Statistics of Author Medians for Active Subreddits",label="table/author-medians:active")

def runAut(date):
	""" runs author level data plots """
	aut = autData(date)
	settings()
	autTables(aut)
	kde(aut['active'])


""" """
def correlations(date, subset='active'):
	"""
	combines subreddit-level and author-level subsets
	"""
	sub = subData(date)
	aut = autData(date)

	print("GETTING CORRELATION TABLE")
	active = pd.merge(sub[subset], aut[subset], left_index=True, right_index=True)
	body = active.corr().to_latex()
	tableFile(body, caption="Correlations for Active subreddits", label='table/corr:active')

	settings()
	pd.set_option('display.float_format', lambda x: '%.2f' % x)
	plt.rc('font', size=10)

	print("GETTING CORRELATION HEATMAP")
	plt.figure(figsize=(12,12))
	plt.xticks(size=14)
	plt.yticks(size=14)
	sns.heatmap(active.corr(), cmap='RdBu_r', annot=True)
	plt.title('Correlation Matrix for Active Subreddits', size=20)

	filename = latexPath(f"""corr-{subset}.pdf""")
	plt.savefig(filename, bbox_inches='tight')
	plt.close()

	print("GETTING POLITICAL SUBREDDIT CLUSTERMAP")

	pd.set_option('display.float_format', lambda x: '%.2f' % x)
	pol = getSubset(active.rank(pct=True, ascending=True)).drop('default', axis=1)
	plt.figure(figsize=(12,12))
	plt.xticks(size=14)
	plt.yticks(size=14)
	sns.heatmap(pol.T, cmap='Reds', annot=True)
	plt.title('Political Subreddit Variable (Active) Percentiles', size=20)
	filename = latexPath(f"""pol-heatmap.pdf""")
	plt.savefig(filename, bbox_inches='tight')
	plt.close()

	print("GETTING POLTICAL SUBREDDIT TABLES")
	tableFile(pol.T.to_latex(), caption="Political Subreddit Result Percentiles", label='table/pol:pct')
	tableFile(getSubset(active).T.to_latex(), caption="Political Subreddit Results", label='table/pol')

def run(date="2018-02"):
	print("RUNNING SUBREDDIT-LEVEL DATA")
	runSub(date)

	print("RUNNING AUTHOR-LEVEL DATA")
	runAut(date)

	print("RUNNING CORRELATIONS")
	correlations(date)

	print('DONE!')




	


