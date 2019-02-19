from scripts.dataAnalysis import *
import numpy as np

pd.set_option('precision', 2)
plt.style.use(['seaborn-paper'])

def sizePlots(date, df):
        for v in ['author_count','comment_count']:
                df[v].hist(log=True)
                plt.tight_layout()
                plt.savefig(figurePath(f"""{date}/hist-{v}-log.pdf"""))

                plt.close();

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

def diversityHistograms(df):
        for v in ["entropy","blau","gini"]:
                plt.hist(subset[v])
                plt.tight_layout()
                plt.savefig(figurePath(f"""{date}/hist-{v}.pdf"""))
                plt.close();


def run():
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
