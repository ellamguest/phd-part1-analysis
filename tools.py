from pathlib import Path
import time
import datetime
import numpy as np

""" TOOLS """
getDate = lambda year, month: f"""{year}-{month}"""
cachePath = lambda filename: Path(f"""cache/{filename}""")
figurePath = lambda filename: Path(f"""figures/{filename}""")
outputPath = lambda filename: Path(f"""output/{filename}""")

def createDirectories(date):
    """creates sub-directories for monthly data, if they don't exist already"""
    Path(f"""cache/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""figures/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""output/{date}""").mkdir(exist_ok=True, parents=True)


elapsed = lambda start, end: print(f"""{(end-start)/60} minutes elapsed""") 





def segmentMonths(year):
    months = list(range(1,13))
    starts = [datetime.datetime(year,month,1,0,0,0) for month in months]
    
    following = starts[1:]+[(datetime.datetime(year+1,1,1,0,0,0))]
    delta = datetime.timedelta(seconds=1)
    
    ends = [date - delta for date in following]
    
    starts = [time.mktime(x.timetuple()) for x in starts]
    ends= [time.mktime(x.timetuple()) for x in ends]
    
    return list(zip(starts, ends))
    

def query(year, month):
    pairs = segmentMonths(year)
    
    start = time.mktime(datetime.datetime(year,month,1,0,0,0).timetuple())
    if month==12:
        end = time.mktime(datetime.datetime(year+1,1,month,0,0,0).timetuple()) - 1
    else:
        end = time.mktime(datetime.datetime(year,month+1,1,0,0,0).timetuple())
        
    return f"""
SELECT author, subreddit,
  COUNT(link_id) AS num_comments
FROM
  [fh-bigquery:reddit_comments:{year}]
WHERE (created_utc >= {np.int(start)}) & (created_utc <= {np.int(end)})
GROUP BY
  author,
  subreddit"""


      
def cleanDF(df):
    defaults = """Art+AskReddit+DIY+Documentaries+EarthPorn+Futurology+GetMotivated+IAmA+InternetIsBeautiful+Jokes+\
LifeProTips+Music+OldSchoolCool+Showerthoughts+TwoXChromosomes+UpliftingNews+WritingPrompts+\
announcements+askscience+aww+blog+books+creepy+dataisbeautiful+explainlikeimfive+food+funny+\
gadgets+gaming+gifs+history+listentothis+mildlyinteresting+movies+news+nosleep+nottheonion+\
personalfinance+philosophy+photoshopbattles+pics+science+space+sports+television+tifu+\
todayilearned+videos+worldnews""".split('+')
    defaults.append('politics')
    
    clean = df.astype({'author':str,'subreddit':str,'num_comments':int})
    return clean[(~clean['subreddit'].isin(defaults)) &
                (~clean['author'].isin(['[deleted]','AutoModerator'])) &
                (~clean['subreddit'].str.startswith('u_'))
                ]
