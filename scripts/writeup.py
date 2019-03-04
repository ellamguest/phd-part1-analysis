#from scripts.dataAnalysis import *
import numpy as np
import pandas as pd	
import matplotlib.pyplot as plt
from scripts.tools import addDefaults, figurePath, outputPath
from pathlib import Path
import seaborn as sns

latexPath = lambda filename: Path(f"""latex/{filename}""")

def monthlyCounts(date):
	full = streamBlob('emg-author-subreddit-pairs-ids',date)
	num_authors = full.author.unique().shape[0] - 1
	num_subreddits = full.subreddit.unique().shape[0]
	num_comments = full.num_comments.sum()
	del_comments = full[full['author']=='[deleted]'].num_comments.sum()

	counts = pd.Series({'num_authors':num_authors,
						'num_subreddits':num_subreddits,
						'num_comments':num_comments,
						'del_comments':del_comments})

	counts.to_csv(latexPath(f"""monthly-counts-{date}.csv"""))



def tableFile(body, caption=None, label=None):
	header = "\\begin{table}\n\centering\n"
	footer = f"""\caption{{{caption}}}\n\label{{{label}}}\n\end{{table}}"""
	text = header + body + footer

	with open(latexPath(f"""{label}.tex"""), "w") as f:
		f.write(text)

def tables(data):
	Path(f"""latex/table""").mkdir(exist_ok=True, parents=True)
	tableFile((data['df'][['log10_author_count','log10_comment_count','entropy_norm','gini','blau']]
						.describe().drop('count').to_latex()),
						 caption="Descriptive Statistics for all Subreddits", label="table/all")
	tableFile((data['defaults'][['log10_author_count','log10_comment_count','entropy_norm','gini','blau']]
						.describe().drop('count').to_latex()),
						caption="Descriptive Statistics for Default Subreddits", label="table/defaults")
	tableFile((data['active'][['log10_author_count','log10_comment_count','entropy_norm','gini','blau']]
						.describe().drop('count').to_latex()),
						caption="Descriptive Statistics for Top Decile of Subreddits by Author Count", label="table/active")

def subsetDecile(df, variable='author_count'):
	"""Subsetting Active Subreddits"""
	copy = df.copy().sort_values(variable, ascending=False).reset_index(drop=True)
	tenth = copy.shape[0]/10
	
	return copy.loc[:np.round(tenth)-1]

def histograms(df):
	Path(f"""latex/hist""").mkdir(exist_ok=True, parents=True)
	for v in ['log10_author_count','log10_comment_count','entropy_norm','gini','blau']:
		print(v)
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

		# smaller bins?

def kde(df):
	Path(f"""latex/kde""").mkdir(exist_ok=True, parents=True)
	for v in ['log10_author_count','log10_comment_count','entropy_norm','gini', 'blau']:
		print(v)
		data = df[v]
		filename = latexPath(f"""kde/{v}-defaults.pdf""")
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

		# larger bins?
		# kernel density over log x axis

def settings():
	pd.set_option('display.float_format', lambda x: '%.3f' % x)
	plt.style.use(['seaborn-paper'])
	plt.rc('font', serif='Computer Modern Roman')
	plt.rc('figure', figsize=(3,3))
	plt.rc('font', size=20)

def plots(df):
	settings()
	histograms(df)
	kde(df)

	# hist tiny bins OR kde larger bins

def loadData(date="2018-02"):
	settings()

	Path(f"""latex""").mkdir(exist_ok=True, parents=True)

	date = "2018-02"
	df = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0)
	df = df[~df['subreddit'].str.startswith('u_')] # dropping homepages

	df['gini'] = 1-df['gini'] #until re-run all stats with inverted gini function

	df['log10_author_count'] = np.log10(df['author_count'])
	df['log10_comment_count'] = np.log10(df['comment_count'])

	df['entropy_max'] = df['author_count'].apply(lambda x: np.log(x))
	df['entropy_norm'] = df['entropy']/df['entropy_max'].fillna(0)

	df = addDefaults(df)
	defaults = df[df['default']==True].sort_values('author_count')

	active = subsetDecile(df)

	return {'date':date,
			'df':df,
			'active':active,
			'defaults':defaults}

def trueDiversity(blau):
        """
        blau can be expressed as a transformation of true diversity of order 2
        """

        return - np.sqrt(1/(blau-1))

def political():
	pol = (getSubset(data['df'].set_index('subreddit'))[['log10_author_count', 'log10_comment_count',
														'entropy_norm', 'gini']]
														.sort_values('log10_author_count', ascending=False))



def corrTable(df, caption=None, label=None):
	body = df.corr().to_latex()
	tableFile(body, caption=caption, label=label)
	

def run():
	data = loadData()
	plots(data['active'])
	tables(data)




##### AUTHOR LEVEL
def authorLevelTable():
	date = "2018-02"
	#df = streamBlob('emg-author-level-stats', date, filetype='csv')
	df = pd.read_csv(outputPath(f"""{date}/authorLevelStats.csv"""))

	df = df.set_index('subreddit')
	df = df[df['author']!='[deleted]']

	sub = df[[c for c in df.columns if 'median' in c]]
	sub.columns = [c.replace('_median','') for c in sub.columns]

	body = sub.describe().drop('count').to_latex()
	tableFile(body, caption='Distribution of Author Medians for All Subreddits', label='table/author-medians:all')
	

	active = data['active'].subreddit
	subset = sub.loc[active]

	body = subset.describe().drop('count').to_latex()
	tableFile(body, caption='Distribution of Author Medians for Active Subreddits', label='table/author-medians:active')
	


