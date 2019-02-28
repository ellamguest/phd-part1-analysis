import json
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def config(key):
    return json.load(open('config.json'))[key]