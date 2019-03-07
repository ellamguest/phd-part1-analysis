# phd-part1-analysis

The data pipeline for Part 1 (and the beginnings of Part 2) of my doctoral thesis.

## File Structure

### aws/
- scripts for running code on AWS vm instances
- currently unused
- also holds gcs/ with scratch code for moving over to GCS vm instances

### cache/
- locally stored files for dats recently processed or currently analysing
- otherwise data should be stored in GCS buckets
- git ignores

### figures/
- figures for data currently analysing

### latex/
- text (.tex) and image (.pdf) for compiling write up in latex
- figures/ : misc figure files, and .tex files with commands for structuing figure layouts
- hist/ : individual histogram images
- kde/ : individual kernel density estimation images
- matrix/ : heatmaps and clustermaps
- table/ : .tex table files
- text/ : main.tex and separate .tex section files

### notebooks/
- jupyter notebooks (.ipynb)
- mostly old/scratch files

### scripts/
- python scripts files (.py)
- writeup.py produces figures for latex/


## Data Collection

The data used in this research is publically avaiable in Google BigQuery tables...