#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 26 16:20:55 2018

@author: emg
"""

def setupGCS():
    try:
     from google.cloud import bigquery
    except ImportError:
        import pip
        pip.main(['install', '--upgrade', 'google-cloud-bigquery'])
        
    try:
     from google.cloud import storage
    except ImportError:
        import pip
        pip.main(['install', '--upgrade', 'google-cloud-storage'])