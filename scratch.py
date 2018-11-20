#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import scipy as sp
from scipy import stats


date = getDate('2018', '01')
df = pd.read_csv(cachePath(f"""{date}/author-subreddit-counts-plus-author-stats.csv"""), index_col=0)



   
        
