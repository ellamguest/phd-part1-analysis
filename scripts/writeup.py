#from scripts.dataAnalysis import *
import numpy as np
import pandas as pd	
import matplotlib.pyplot as plt
from scripts.tools import addDefaults, figurePath, outputPath

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

def sizePlots(date, df):
	for v in ['author_count','comment_count']:
			df[v].hist(log=True)
			plt.tight_layout()
			plt.savefig(figurePath(f"""{date}/hist-{v}-log.pdf"""))

			plt.close()

def histograms(df, date):
	for v in df.select_dtypes('number').columns:
		print(v)
		data = df[v]
		filename = figurePath(f"""{date}/hist-{v}.pdf""")
		if 'count' in v:
			print(v)
			data = np.log(data)
			filename = figurePath(f"""{date}/hist-{v}-log.pdf""")
		plt.hist(data)
		plt.xlim(0)
		plt.tight_layout()
		plt.savefig(filename)
		plt.close()

		# smaller bins
		# kernel density over log x axis
		
		
		# normalised entropy, bounded 1


def settings():
	pd.set_option('precision', 2)
	plt.style.use(['seaborn-paper'])
	plt.rc('font', family='serif', serif='Times')
	plt.rc('figure', figsize=(3.487,2))

def run():
	settings()

	date = "2018-02"
	df = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0)
	
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

	histograms(df, date)
