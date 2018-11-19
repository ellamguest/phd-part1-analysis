#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 16:29:59 2018

@author: emg
"""

query = f"""SELECT author, subreddit, count(link_id) as num_comments, sum(score) as total_score
FROM [fh-bigquery:reddit_comments.2018_05] 
GROUP BY author, subreddit"""

year, month = '2015', '01'

test = f"""
SELECT author, subreddit, link_id
FROM `reddit_comments.{year}.{month}`
LIMIT 10"""

def client():
    """REQUIRES A FILE CALLED 'bigquery.json' in credentials folder"""
    bigquery_credentials = credentialsPath('bigquery-public.json')
    return bigquery.Client.from_service_account_json(bigquery_credentials)

j = client().query(query=test, job_config=jobConfig())
df = j.to_dataframe()


""""
lookslike might only b albe to query public dataset through the console *face palm*

look into calling pushshift api?

ex:
    
http://api.pushshift.io/reddit/comment/search?created_utc%3E1542645838&created_utc%3C1542645839

call size limitied at 500 lines (size=500)
fields = "author subreddit created_utc body"
after = 
before = 
Epoch value or Integer + "s,m,h,d" (i.e. 30d for 30 days)
""""

from datetime import datetime, timedelta

first = datetime.datetime(2015, 1, 1,0,0,0)
nextMonth = datetime.datetime(2015, 2, 1,0,0,0)
delta = timedelta(seconds=1)
last = nextMonth - delta

import time 
utc = time.mktime(first.timetuple())
