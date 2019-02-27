from pathlib import Path
import time
import datetime
import numpy as np
import os

""" TOOLS """
cachePath = lambda filename: Path(f"""cache/{filename}""")
figurePath = lambda filename: Path(f"""figures/{filename}""")
outputPath = lambda filename: Path(f"""output/{filename}""")

def createDirectories(date):
    """creates sub-directories for monthly data, if they don't exist already"""
    Path(f"""cache/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""figures/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""output/{date}""").mkdir(exist_ok=True, parents=True)

elapsed = lambda start, end: print(f"""{(end-start)/60} minutes elapsed""") 

getDates = lambda: sorted(next(os.walk("cache"))[1])

def getSubset(df):
    subs = ['The_Donald', 'Libertarian','Conservative', 'politics', 'changemyview','socialism','SandersForPresident','LateStageCapitalism']
    return df.loc[subs]

def addDefaults(df):
    defaults = """Art+AskReddit+DIY+Documentaries+EarthPorn+Futurology+GetMotivated+IAmA+InternetIsBeautiful+Jokes+\
LifeProTips+Music+OldSchoolCool+Showerthoughts+TwoXChromosomes+UpliftingNews+WritingPrompts+\
announcements+askscience+aww+blog+books+creepy+dataisbeautiful+explainlikeimfive+food+funny+\
gadgets+gaming+gifs+history+listentothis+mildlyinteresting+movies+news+nosleep+nottheonion+\
personalfinance+philosophy+photoshopbattles+pics+science+space+sports+television+tifu+\
todayilearned+videos+worldnews+Fitness""".split('+')
    df['default'] = df['subreddit'].apply(lambda x: True if x in defaults else False)

    return df


def deciles(series):
    return series.quantile([0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1])


""" GOOGLE CLOUD SERVICES """
from pathlib import Path
import pandas as pd
from google.cloud import storage
import gcsfs
import os


"""IMPORTING DATA FROM GOOGLE CLOUD STORAGE"""
storage_client = lambda: storage.Client.from_service_account_json('gcs_service_account.json')

def fetchBlob(date):
    bucket = storage_client().get_bucket('emg-author-subreddit-pairs')
    return bucket.blob(f"""{date}.gzip""")

def streamBlob(bucket_name, date):
    print(f"""opening GCS blob {date} from bucket {bucket_name}""")
    fs = gcsfs.GCSFileSystem(project='author-subreddit-counts',token='gcs_service_account.json')
    with fs.open(f"""{bucket_name}/{date}.gzip""") as f:
        return pd.read_csv(f, compression="gzip", index_col = 0)

def writeBlob(bucket_name, filename, date):
    bucket = storage_client().get_bucket(bucket_name)
    blob = bucket.blob(f"""{date}.gzip""")
    blob.upload_from_filename(filename)

def bucketDates(bucket_name):
    bucket = storage_client().get_bucket(bucket_name)
    result = []
    for blob in bucket.list_blobs():
        result.append(blob.name.strip('.gzip'))

    return result

def uploadCommands(filename, bucket_name, date):
    with open('uploadCommands.sh', 'a') as f:
        command = f"""gsutil cp {filename} gs://{bucket_name}/{date}.gzip"""
        f.write("%s\n" % command)

def runUploads():
    """Need to convert to shell script"""
    os.system("chmod u+x uploadCommands.sh")
    os.system("./uploadCommands.sh")
    os.system("rm uploadCommands.sh")

def uploadAuthorsStats():
	dates = getDates()
	bucket_name = 'emg-author-stats'

	for date in dates:
		files = os.listdir(cachePath(date))
		if 'authorStats.gzip' in files:
			filename = cachePath(f"""{date}/authorStats.gzip""")
			uploadCommands(filename, bucket_name, date)

	runUploads()

def uploadAuthorsLevelStats():
	dates = getDates()
	bucket_name = 'emg-author-level-stats'

	for date in dates:
		files = os.listdir(outputPath(date))
		if 'authorLevelStats.csv' in files:
			filename = outputPath(f"""{date}/authorLevelStats.csv""")
			uploadCommands(filename, bucket_name, date)

	runUploads()

def uploadSubredditLevelStats():
	dates = getDates()
	bucket_name = 'emg-subreddit-level-stats'

	for date in dates:
		files = os.listdir(outputPath(date))
		if 'subredditLevelStats.csv' in files:
			filename = outputPath(f"""{date}/subredditLevelStats.csv""")
			uploadCommands(filename, bucket_name, date)

	runUploads()

