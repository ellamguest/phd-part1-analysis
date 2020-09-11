# problems with conda not being recognised sometime with miniconda
wget https://repo.anaconda.com/archive/Anaconda3-2018.12-Linux-x86_64.sh

# if need bzip2
sudo apt-get update
sudo apt-get install bzip2

bash Anaconda3-2018.12-Linux-x86_64.sh

source ~/.bashrc
which conda # check installed correctly
pip install gcsfs google-cloud-storage

## jupyter configuration - don't think need right now

#jupyter notebook --generate-config
#vim ~/.jupyter/jupyter_notebook_config.py
# add to top of jupyter config file
#c = get_config()
#c.NotebookApp.ip = '*'
#c.NotebookApp.open_browser = False
#c.NotebookApp.port = 5000 #<Port Number>

gsutil cp -r gs://emg-scripts .
cd emg-scripts
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser &


# set up ssh tunnel in localhost
gcloud compute ssh instance-2 --project "author-subreddit-counts"  --zone "us-east1-b"  -- -L 2222:localhost:8888
# runAuthorStats working succesfully but not much faster - try much larger instance

mkdir cache
gsutil cp gs://emg-author-subreddit-pairs-ids/{date}.gzip cache/{date}.gzip # one date at a time


# create instance with more memory and processing power
# look into permission for uploading file to bucket
