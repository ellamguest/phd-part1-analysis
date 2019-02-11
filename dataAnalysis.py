from tools import *
import pandas as pd
import matplotlib.pyplot as plt

def getData(filename):
    dates = getDates()
    data = []
    for date in dates:
        if outputPath(f"""{date}/{filename}""").is_file():
            df = pd.read_csv(outputPath(f"""{date}/{filename}"""), index_col=0)
            df['date'] = date
            data.append(df)

    return pd.concat(data)

def subredditStats():
    df = getData("subredditLevelStats.csv")

    Path(f"""figures/subredditLevel""").mkdir(exist_ok=True, parents=True)
    variables = df.select_dtypes([int, float]).columns
    for variable in variables:
        log = "count" in variable
        df.hist(column=variable,by='date', figsize=(20,30), layout=(10,4), log=log)
        plt.suptitle(f"""{variable} monthly histograms""")
        plt.savefig(figurePath(f"""subredditLevel/{variable}-histograms.pdf"""))


def authorStats():
    df = getData("authorLevelStats.csv")

    Path(f"""figures/authorLevel""").mkdir(exist_ok=True, parents=True)
    variables = df.select_dtypes([int, float]).columns
    varibles = [v for v in variables is "median" in v]
    for variable in variables:
        log = "count" in variable
        df.hist(column=variable,by='date', figsize=(20,30), layout=(10,4), log=log)
        plt.suptitle(f"""{variable} monthly histograms""")
        plt.savefig(figurePath(f"""subredditLevel/{variable}-histograms.pdf"""))