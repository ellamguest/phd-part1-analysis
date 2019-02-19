#from scripts.dataAnalysis import *
import numpy as np
import pandas as pd	
import matplotlib.pyplot as plt
from scripts.tools import addDefaults, figurePath, outputPath
from pathlib import Path

latexPath = lambda filename: Path(f"""latex/{filename}""")

def table(df, caption=None, label=None):
	print("\\begin{table}")
	print("\centering")
	print(df.describe().drop('count').to_latex())
	print(f"""\caption{{{caption}}}""")
	print(f"""\label{{{label}}} """)
	print("\end{table}")

def subsetDecile(df, variable='author_count'):
	"""Subsetting Active Subreddits"""
	copy = df.copy().sort_values(variable, ascending=False).reset_index(drop=True)
	tenth = copy.shape[0]/10
	
	return copy.loc[:np.round(tenth)-1]

def histograms(df):
	for v in df.select_dtypes('number').columns:
		print(v)
		data = df[v]
		filename = latexPath(f"""hist-{v}.pdf""")
		if 'count' in v:
			data = np.log(data)
			filename = latexPath(f"""hist-{v}-log.pdf""")
		plt.hist(data, color='grey')
		xmin, xmax = data.min(), data.max()
		plt.xlim(xmin, xmax)
		plt.tight_layout()
		plt.savefig(filename, bbox_inches='tight')
		plt.close()

		# smaller bins
		# kernel density over log x axis

def kde(df):
	for v in df.select_dtypes('number').columns:
		print(v)
		data = df[v]
		filename = latexPath(f"""kde-{v}.pdf""")
		if 'count' in v:
			data = np.log(data)
			filename = latexPath(f"""kde-{v}-log.pdf""")
		sns.kdeplot(data, shade=True, color='grey', legend=False)
		xmin, xmax = data.min(), data.max()
		plt.xlim(xmin, xmax)
		plt.tight_layout()
		plt.savefig(filename, bbox_inches='tight')
		plt.close()

def settings():
	pd.set_option('precision', 2)
	plt.style.use(['seaborn-paper'])
	plt.rc('font', monospace='Computer Modern TypeWriter')
	plt.rc('figure', figsize=(3,3))
	plt.rc('font', size=20)

def plots(df):
	settings()
	histograms(df)
	kde(df)

def run():
	Path(f"""latex""").mkdir(exist_ok=True, parents=True)

	date = "2018-02"
	df = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0)
	df = df[~df['subreddit'].str.startswith('u_')] # dropping homepages

	df['entropy_max'] = df['author_count'].apply(lambda x: np.log(x))
	df['entropy_norm'] = df['entropy']/df['entropy_max'].fillna(0)

	df = addDefaults(df)
	defaults = df[df['default']==True].sort_values('author_count')

	subset = subsetDecile(df)

	table(df, caption="Descriptive Statistics for all Subreddits", label="table:all")
	print()
	print()
	table(defaults, caption="Descriptive Statistics for Default Subreddits", label="table:defaults")
	print()
	print()
	table(subset, caption="Descriptive Statistics for Top Decile of Subreddits by Author Count", label="table:active")

	settings()
	histograms(subset, date)


	deciles(subset['blau'])
