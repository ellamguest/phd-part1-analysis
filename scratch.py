from gcs import *
from tools import *




uploaded = bucketDates("emg-author-stats")
dates = getDates()
toRun = [date for date in  dates if date not in uploaded]

for date in uploaded:
    if cachePath(f"""{date}/authorLevelStats.csv""").is_file():
        print(date, "DONE!")
    else:
        print(date)
        df = streamBlob('emg-author-stats', date)
        getAggAuthorStats(df, date)




