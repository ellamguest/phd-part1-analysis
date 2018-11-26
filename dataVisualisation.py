#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bokeh.plotting import figure, output_notebook, show, ColumnDataSource
from bokeh.models import NumeralTickFormatter
import seaborn as sns

def annotatedHist(df, variable, log=False):
    x = df[variable].sort_values()
    if log:
        x = np.log(x)
    
    plt.hist(x, cumulative=True, color='grey')
    
    xmin, xmax = plt.xlim()
    tdX = x.loc['The_Donald']
    tdY = x.index.get_loc('The_Donald')
    
    plt.vlines(tdX, 0, tdY, color='red')
    plt.hlines(tdY, xmin, tdX,
               color='red', linestyles='dashed')
    
    cmvX = x.loc['changemyview']
    cmvY = x.index.get_loc('changemyview')
    
    plt.vlines(cmvX, 0, cmvY, color='green')
    plt.hlines(cmvY, xmin, cmvX,
               color='green', linestyles='dashed')
    
    plt.title(variable)
    plt.show()
    
def compareHists(df):
    for variable in df.columns:
        if variable in ['subreddit_comment_count', 'subreddit_author_count']:
            annotatedHist(df, variable, log=True)
        else:
            annotatedHist(df, variable)
            
def correlationHeatmap(df, date, save=False):
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