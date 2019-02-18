from scripts.dataAnalysis import *
import numpy as np
pd.set_option('precision', 2)


date = "2018-02"
subdf = pd.read_csv(outputPath(f"""{date}/subredditLevelStats.csv"""), index_col=0)

num_subreddits = subdf.shape[0]

for v in ['author_count','comment_count']:
    subdf[v].hist(log=True)
    plt.tight_layout()
    plt.savefig(figurePath(f"""{date}/hist-{v}-log.pdf"""))

    plt.close();

# Summary of Author and Comments Counts Per Subreddit
desc = subdf.describe().T.drop('count',axis=1)
print(desc.to_latex())

# Subsetting Active Subreddits
corr = np.corrcoef(subdf['author_count'],subdf['comment_count'])[0][1]

subdf = subdf.sort_values('author_count', ascending=False).reset_index()

tenth = subdf.shape[0]/10
subset = subdf.loc[:np.round(tenth)-1]

subdf = subdf.sort_values('comment_count', ascending=False).reset_index()

tenth = subdf.shape[0]/10
comment_subset = subdf.loc[:np.round(tenth)-1]


# Diversity Histograms
plt.style.use(['seaborn-paper'])

for v in ["entropy","blau","gini"]:
    plt.hist(subset[v])
    plt.tight_layout()
    plt.savefig(figurePath(f"""{date}/hist-{v}.pdf"""))
    plt.close();




autdf = pd.read_csv(outputPath(f"""{date}/authorLevelStats.csv"""), index_col=0)
autdf = autdf[[i for i in a.columns if 'median' in i]]


s = subdf.describe().T