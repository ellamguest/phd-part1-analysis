
from scripts.tools import bucketDates

def checkDates():
	dataDates = bucketDates('emg-author-subreddit-pairs')
	idDates = bucketDates('emg-author-subreddit-pairs-ids')
	authorDates = bucketDates('emg-author-stats')
	subredditDates = bucketDates('emg-subreddit-level-stats')

	missingIds = sorted(list(set(dataDates)-set(idDates)))
	missingAuthors = sorted(list(set(dataDates)-set(authorDates)))
	missingSubreddits = sorted(list(set(dataDates)-set(subredditDates)))


for date in dataDates():
        print(date)
        runAuthorStats(date)
        runUploads()
        os.remove(cachePath(f"""{date}/authorStats.gzip"""))


# google vm instance
import googleapiclient.discovery
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/compute']
SERVICE_ACCOUNT_FILE = 'gcs_service_account.json'

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

compute = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)

project = "author-subreddit-counts"
zone = "us-east1-b"

def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None

name = 'part1'