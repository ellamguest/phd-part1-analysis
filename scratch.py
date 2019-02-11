from gcs import *
from tools import *
import matplotlib.pyplot as plt


def uploadCommands(filename, bucket_name, date):
    with open('uploadCommands.sh', 'a') as f:
        command = f"""gsutil cp {filename} gs://{bucket_name}/{date}.gzip"""
        f.write("%s\n" % command)

def runIDS(date):
    createDirectories(date)
    input_bucket = 'emg-author-subreddit-pairs'
    output_bucket = 'emg-author-subreddit-pairs-ids'
    df = streamBlob(input_bucket, date)
    df = df.reset_index().astype({'author':str,'subreddit':str,'num_comments':int})

    print("getting subreddit ids")
    subIds = sortedIds(df['subreddit'])
    df['subreddit_id'] = df['subreddit'].map(lambda x: subIds[x])

    print("getting author ids")
    authorIds = sortedIds(df['author'])
    df['author_id']=df['author'].map(lambda x: authorIds[x])

    print("storing dataset w/ ids")

    filename = cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip""")
    df.to_csv(cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip"""),compression='gzip')

    uploadCommands(filename, output_bucket, date)


