"""assumes you are in a subdirectory such as /script or /notebook"""

from pathlib import Path


def createDirectories():
    Path('../cache').mkdir(exist_ok=True, parents=True)
    Path('../output').mkdir(exist_ok=True, parents=True)
    Path('../figures').mkdir(exist_ok=True, parents=True)
    
def createMonthDirectories(date):
    Path('../cache/' + date).mkdir(exist_ok=True, parents=True)
    Path('../output/' + date).mkdir(exist_ok=True, parents=True)
    Path('../figures/' + date).mkdir(exist_ok=True, parents=True)
    
cachePath = lambda filename: Path('../cache/{}'.format(filename))
credentialsPath = lambda filename: Path('../credentials/{}'.format(filename))
outputPath = lambda filename: Path('../output/{}'.format(filename))
figurePath = lambda filename: Path('../figures/{}'.format(filename))

def main():
    createDirectories()
    
    