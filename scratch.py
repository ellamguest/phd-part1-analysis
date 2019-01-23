from tools import *
import pandas as pd

date = '2016-01'
df = pd.read_csv(outputPath(f"""{date}/fullStats.csv"""), index_col=0)
df = df.set_index('subreddit')


pairs = pd.read_csv(cachePath(f"""{date}/authorStats.gzip"""),compression='gzip')

singles = pairs[pairs['aut_sub_count']==1]
num_singles = singles['subreddit'].value_counts()
df['num_singles']=num_singles

df = getDefaults(df)

authors = pairs.drop_duplicates('author').set_index('author')
authors = authors[['author_id','aut_sub_count', 'aut_com_count', 'aut_com_entropy', 
                    'aut_com_gini','aut_com_blau', 'aut_insub']]


print(date)
print(df.shape[0], 'subreddits')
print(authors.shape[0], 'authors')