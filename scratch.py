

def upload():
        dates = ['2015-11','2016-02','2016-11','2016-12','2017-12']
        for date in dates:
                uploadCommands(cachePath(f"""{date}/authorStats.gzip"""))

def uploadCommands(filename, bucket_name, date):
    with open('uploadCommands.sh', 'a') as f:
        command = f"""gsutil cp {filename} gs://{bucket_name}/{date}.gzip"""
        f.write("%s\n" % command)

def runUploads():
    """Need to convert to shell script"""
    os.system("chmod u+x uploadCommands.sh")
    os.system("./uploadCommands.sh")
    os.system("rm uploadCommands.sh")


