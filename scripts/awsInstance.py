#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 13:25:22 2018

@author: emg
"""

import aws

    

def test():
    # Set up your credentials.json and config.json file first. 
    # There are templates in this repo; copy them into your working dir
    # Then request a spot instance!
    instance = aws.request_spot('python', .15, script=aws.INITIAL_CONFIG)
    aws.await_boot(instance) 

    # Once you've got it, can check how boot is going with
    # aws.cloud_init_output(instance)
    # ssh(instance) # prints the SSH command needed to connect

    # Set up a tunnel to the remote kernel, set up the rsync, then start a remote console
    aws.tunnel(instance)
    aws.rsync(instance)
    aws.remote_console()
    """ In remote console install packages:
        ! conda install pandas pathlib scipy -y
        ! pip install google-cloud-bigquery
    """

    # Use the console and ! commands to install any packages you need. Then create an image with
    aws.create_image(instance, name='python-ec2') # will create image
    

    
    # to open saved image - not working now???
    instance = aws.request_spot('python', .15, script=aws.CONFIG, image='python-ec2')
    instance.terminate()
