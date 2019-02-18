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
