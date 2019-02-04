#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from tools import figurePath

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
    
def timePlot(main, variable):
    agg = main.groupby('month').median()
    td = main.loc['The_Donald'].select_dtypes(['int64','float64'])
    cmv = main.loc['changemyview'].select_dtypes(['int64','float64'])
    
    f, (ax1, ax2, ax3) = plt.subplots(3, 1, sharey=False, figsize=(8,10))
    f.suptitle(variable)
    
    ax1.plot(agg.index, agg[variable], color='grey')
    ax1.tick_params(axis='x',labelbottom=False)
    
    ax2.plot(agg.index, td[variable], color='red')
    ax2.tick_params(axis='x',labelbottom=False)
    
    ax3.plot(agg.index, cmv[variable], color='green')
    
    plt.xticks(rotation='vertical')
    plt.tight_layout()
    plt.savefig(figurePath(f"""{variable}.pdf"""))
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
        plt.savefig(figurePath('{}-corr-heatmap.pdf'.format(date)))
        
def correlationClustermap(df, date, save=False, metric='cosine'):
    corr = df.corr()

    # Set up the matplotlib figure
    #f, ax = plt.subplots(figsize=(11, 9))
    

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(220, 10, as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    sns.clustermap(corr, cmap=cmap, vmax=1, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5}, metric=metric)
    plt.title(date)
    if save:
        plt.savefig(figurePath('{}-corr-clustermap.pdf'.format(date)))
        
    