#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 27 12:08:20 2018

@author: emg
"""
from pathlib import Path
from time import time

""" TOOLS """
getDate = lambda year, month: f"""{year}-{month}"""
cachePath = lambda filename: Path(f"""cache/{filename}""")
figurePath = lambda filename: Path(f"""figures/{filename}""")
outputPath = lambda filename: Path(f"""output/{filename}""")

def createDirectories(date):
    """creates sub-directories for monthly data, if they don't exist already"""
    Path(f"""cache/{date}""").mkdir(exist_ok=True, parents=True)
    Path(f"""output/{date}""").mkdir(exist_ok=True, parents=True)


elapsed = lambda start, end: print(f"""{end-start} elapsed""")  