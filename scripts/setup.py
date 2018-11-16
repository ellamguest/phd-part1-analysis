"""assumes you are in a subdirectory such as /script or /notebook"""

from pathlib import Path

Path('../cache').mkdir(exist_ok=True, parents=True)
Path('../data').mkdir(exist_ok=True, parents=True)
Path('../figures').mkdir(exist_ok=True, parents=True)

cachePath = lambda filename: Path('../cache/{}'.format(filename))
credentialsPath = lambda filename: Path('../credentials/{}'.format(filename))
dataPath = lambda filename: Path('../data/{}'.format(filename))
figurePath = lambda filename: Path('../figures/{}'.format(filename))