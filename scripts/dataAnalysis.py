from bokeh.plotting import figure, output_notebook, show, ColumnDataSource
from bokeh.models import NumeralTickFormatter
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


"""PLOTTING"""
output_notebook(hide_banner=True)

def scatterplot(df, x, y):
    source = ColumnDataSource(data=dict(
        x=df[x],
        y=df[y],
        subreddit=df.index,
        default = df['default'],
        col = df['default'].map({True:'red',False:'blue'})
    ))

    TOOLTIPS = [
        ("subreddit", "@subreddit"),
        (x, "@x"),
        (y, "@y"),
        ("default", "@default")
    ]

    p = figure(plot_width=400, plot_height=400, tooltips=TOOLTIPS)

    p.circle('x', 'y', size=5, source=source, color='col')

    p.xaxis.axis_label = x
    p.yaxis.axis_label = y

    p.xaxis[0].formatter = NumeralTickFormatter(format="0")
    p.yaxis[0].formatter = NumeralTickFormatter(format="0")

    show(p)

def correlationHeatmap(df, date, save=False):
    # Compute the correlation matrix
    corr = df.corr()

    # Generate a mask for the upper triangle
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(220, 10, as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(corr, mask=mask, cmap=cmap, vmax=1, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    plt.title(date)
    plt.tight_layout()
    if save:
        plt.savefig('{}-corr-heatmap.pdf'.format(date))


"""DATA MANIPULATION"""
iqr = lambda df, column: df[df[column].between(df[column].quantile(.25), df[column].quantile(.75), inclusive=True)]


import statsmodels.api as sm
X = main["subreddit_comment_count"]

preds = {}
for variable in percentiles.drop('subreddit_comment_count').columns:
    y = main[variable]
    model = sm.OLS(y, X).fit()
    d[variable] = model.predict(X)

