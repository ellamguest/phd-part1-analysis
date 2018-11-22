from pathlib import Path
from io import BytesIO
import re
import time
import os
import json
import boto3
import subprocess
import shlex
import base64
import logging
from io import StringIO
import pandas as pd
from . import config

log = logging.getLogger(__name__)
log.setLevel('DEBUG')

KERNEL = """cd ~/code && nohup ipython kernel -f kernel.json >~/kernel.log 2>&1 &"""

CONFIG = f"""
su ec2-user -l -c '
{KERNEL}
'
"""

INITIAL_CONFIG = f"""
cd /home/ec2-user
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && chmod u+x miniconda.sh 
./miniconda.sh -b -p miniconda && chown -R ec2-user:ec2-user miniconda
rm miniconda.sh
echo 'export PATH="$HOME/miniconda/bin:$PATH"' >> .bashrc
conda install jupyter --yes
mkdir code && chown -R ec2-user:ec2-user code
{CONFIG}
"""


_ec2 = None
def ec2():
    global _ec2
    if _ec2 is None:
        _ec2 = boto3.resource('ec2', region_name=config('REGION')) 
    
    return _ec2

def instance_spec(image=None, script=None, **kwargs):
    defaults = {'ImageId': config('IMAGE'),
                'KeyName': config('KEYPAIR'),
                'SecurityGroups': [config('MUTUAL_ACCESS_GROUP'), config("SSH_GROUP")],
                'IamInstanceProfile': {'Name': config("IAM_ROLE")},
                'InstanceType': config('INSTANCE'),
                'Placement': {'AvailabilityZone': config('AVAILABILITY_ZONE')},
                'UserData': '#!/bin/bash\n'}
    
    if image:
        defaults['ImageId'] = images()[image].id
    if script:
        defaults['UserData'] = defaults['UserData'] + script

    return defaults

def set_name(obj, name):
    ec2().create_tags(Resources=[obj.id], Tags=[{'Key': 'Name', 'Value': name}])

def instances():
    return {as_dict(i.tags).get('Name', 'unnamed'): i for i in ec2().instances.all() if i.state['Name'] != 'terminated'}

def create_instance(name, **kwargs):
    assert name not in instances()

    spec = instance_spec(**kwargs)
    instance = ec2().create_instances(MinCount=1, MaxCount=1, **spec)[0]
    set_name(instance, name)
    return instance

def request_spot(name, bid, **kwargs):
    assert name not in instances()

    spec = instance_spec(**kwargs)
    spec['UserData'] = base64.b64encode(spec['UserData'].encode()).decode()
    requests = ec2().meta.client.request_spot_instances(
                    InstanceCount=1,
                    SpotPrice=str(bid),
                    LaunchSpecification=spec)
    
    request_ids = [r['SpotInstanceRequestId'] for r in requests['SpotInstanceRequests']]
    while True:
        try:
            desc = ec2().meta.client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
            states = [d['Status']['Code'] for d in desc['SpotInstanceRequests']]
            log.info('States: {}'.format(', '.join(states)))
            if all([s == 'fulfilled' for s in states]):
                break
        except boto3.exceptions.botocore.client.ClientError:
            log.info(f'Exception while waiting for spot requests')
            raise
        time.sleep(5)
    
    instance = ec2().Instance(desc['SpotInstanceRequests'][0]['InstanceId']) 
    set_name(instance, name)
    return instance
    
def create_image(instance, name='python-ec2'):
    if name in images():
        log.warn('Deleting old image')
        im = images()[name]
        devices = im.block_device_mappings
        im.deregister()
        for device in devices:
            ec2().Snapshot(device['Ebs']['SnapshotId']).delete()

    im = instance.create_image(Name=name, NoReboot=False)
    while True:
        im = ec2().Image(im.id)
        log.info(f'Image is {im.state}')
        if im.state == 'available':
            return im

        time.sleep(5)

def volumes():
    return {as_dict(i.tags).get('Name', 'unnamed'): i for i in ec2().volumes.all()}

def create_volume(name, size):
    assert name not in volumes(), 'Already created a volume with that name'
    assert size < 128
    volume = ec2().create_volume(AvailabilityZone=config('AVAILABILITY_ZONE'), Size=size)
    set_name(volume, name)
    return volume

def attach_volume(instance, volume, device='/dev/sdf'):
    if isinstance(volume, str):
        volume = volumes()[volume]
    instance.attach_volume(Device=device, VolumeId=volume.id)
    
def as_dict(tags):
    return {t['Key']: t['Value'] for t in tags} if tags else {}
    
def images():
    return {i.name: i for i in ec2().images.filter(Owners=['self']).all()}

def console_output(instance):
    print(instance.console_output().get('Output', 'No output yet'))

def collapse(s):
    return re.sub('\n\s+', ' ', s, flags=re.MULTILINE)

def ssh_options():
    return collapse(f"""
        -i "~/.ssh/{config('KEYPAIR')}.pem" 
        -o StrictHostKeyChecking=no 
        -o UserKnownHostsFile=/dev/null""")

def host(instance):
    return f"ec2-user@{instance.public_ip_address}"

def ssh(instance):
    return collapse(f"""ssh {ssh_options()} {host(instance)}""")

def command(instance, command):
    c = collapse(f"""ssh {ssh_options()} {host(instance)} {command}""")
    return subprocess.call(shlex.split(c))

def command_output(instance, command):
    c = collapse(f"""ssh {ssh_options()} {host(instance)} {command}""")
    return subprocess.check_output(shlex.split(c))

def cloud_init_output(instance):
    print(command_output(instance, 'tail -n100 /var/log/cloud-init-output.log').decode())

def await_boot(instance):
    while True:
        log.info('Awaiting boot')
        if command(instance, 'cat /var/lib/cloud/instance/boot-finished') == 0:
            log.info('Booted')
            return
        time.sleep(1)

def scp(instance, path):
    Path('./cache').mkdir(exist_ok=True, parents=True)
    command = collapse(f"""scp {ssh_options()} {host(instance)}:/{path} ./cache""")
    subprocess.check_call(shlex.split(command))

def rsync(instance):
    """Need to install fswatch with brew first"""
    log.info('Establishing rsync')
    subcommand = collapse(f"""
                rsync -az --progress
                -e "ssh {ssh_options()}"
                --filter=":- .gitignore"
                --exclude .git
                .
                {host(instance)}:code""")

    command = collapse(f"""
        {subcommand};
        fswatch -o . | while read f; do {subcommand}; done""")
    
    os.makedirs('logs', exist_ok=True)
    logs = open('logs/rsync.log', 'wb')
    p = subprocess.Popen(command, stdout=logs, stderr=subprocess.STDOUT, shell=True, executable='/bin/bash')
    return p

def kernel_config(instance):
    scp(instance, '/home/ec2-user/.local/share/jupyter/runtime/kernel.json')
    return json.loads(Path('cache/kernel.json').read_text())

def tunnel_alive(port):
    return subprocess.call(shlex.split(f"nc -z 127.0.0.1 {port}")) == 0

def tunnel(instance):
    log.info('Establishing tunnel')
    kernel = kernel_config(instance)
    ports = ' '.join(f'-L {v}:localhost:{v}' for k, v in kernel.items() if k.endswith('_port'))
    command = collapse(f"ssh -N {ports} {ssh_options()} {host(instance)}")

    if tunnel_alive(kernel['control_port']):
        log.info('Tunnel already created')
        return

    os.makedirs('logs', exist_ok=True)
    logs = open('logs/tunnel.log', 'wb')
    p = subprocess.Popen(shlex.split(command), stdout=logs, stderr=subprocess.STDOUT)
    for _ in range(20):
        time.sleep(1)
        if tunnel_alive(kernel['control_port']):
            log.info('Tunnel created')
            return p
    else:
        p.kill()
        raise IOError('Failed to establish tunnel; check logs/tunnel.log for details')

def remote_console():
    command = 'ipython qtconsole --existing ./cache/kernel.json'
    return subprocess.Popen(shlex.split(command))

def kill(processes):
    for k, p in processes.items():
        log.info(f'Killing {k}, {p.pid}')
        p.terminate()

def restart_session(sess):
    kill(sess['processes'])
    sess['processes'] = session(sess['instance'])['processes']

def interrupt_kernel(instance):
    command(instance, 'pkill -INT -f ipython')

def restart_kernel(instance):
    command(instance, 'pkill -9 -f ipython')
    command(instance, KERNEL)

def session(instance):
    log.info(f'SSH command is "{ssh(instance)}"')

    await_boot(instance) 

    processes = {}
    try:
        processes['tunnel'] = tunnel(instance)
        processes['rsync'] = rsync(instance)
        processes['console'] = remote_console()
    except:
        kill(processes)
    
    return {'instance': instance, 'processes': processes}

def example():
    from aws import ec2

    # Follow the readme and set up your config.json and credentials file first. 

    # Then request a spot instance!
    instance = ec2.request_spot('python', .25, script=ec2.INITIAL_CONFIG)

    # The command below will wait till the instance is booted, then start a rsync, SSH tunnel and remote console.
    # It'll also print a SSH command for the console if you want to remote in and check things yourself.
    session = ec2.session(instance)
    
    # in remote console install required packages
    # on local working directory copy over bigquery credentials with:
    # f"scp {ssh_options()} credentials/bigquery.json {host(instance)}:/home/ec2-user/code/credentials/bigquery.json"""

    # If the rsync/ssh/client fail, you can restart the session with
    ec2.restart_session(session)

    # There are also commands for interrupting/restarting the kernel rather than the session.
    ec2.restart_kernel(session)

    # If you don't want to set up the machine every time, use the console and ! commands to install any packages you need.
    # Then create an image with
    ec2.create_image(instance, name='python-ec2')
    # Be warned - this'll reboot the machine. Only use this for dependencies and the like; for workspace data, use S3.

    # In future, generate the instance from an image with
    instance = ec2.request_spot('python', .25, script=ec2.CONFIG, image='python-ec2-2xlarge')
    session = ec2.session(instance)

    # At the end, 
    ec2.kill(session['processes'])
    instance.terminate()