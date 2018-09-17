# HPC Pack MESOS Framework
[![Build Status](https://travis-ci.org/amat27/hpcpack-mesos.svg?branch=master)](https://travis-ci.org/amat27/hpcpack-mesos)

A MESOS scheduler framework, which accepts offers from MESOS master and builds HPC Pack compute nodes for existing HPC cluster.

## Why do I need HPC Pack MESOS Framework
With the help of HPC Pack MESOS Framework, resource allocation of Microsoft HPC Pack cluster can be managed by existing MESOS cluster, which increases resource utilization.

## What can HPC Pack MESOS Framework do
HPC Pack Mesos Framework
- Borrows HPC Pack compute nodes from Mesos cluster, if
    - HPC Pack has queueing tasks need more resource
    - Mesos cluster has available resource for HPC Pack

- Returns HPC Pack compute nodes to Mesos cluster, if
    - The node reached idle time out of Mesos framework

## Installation
#### Dependency
* Python 2.7
* pipenv

#### Installation Steps
1. Clone this repository
2. CD to the project folder
3. Run command `pipenv install`
4. (Optional) You can get Show help info by typing `pipenv run python hpcframework.py -h`

## Command-line usage
    usage: hpcframework.py [-h] [-g NODE_GROUP] script_path setup_path headnode ssl_thumbprint client_cert

    HPC Pack Mesos framework

    positional arguments:
    script_path           Path of HPC Pack Mesos slave setup script (e.g. setupscript.ps1)
    setup_path            Path of HPC Pack setup executable (e.g. setup.exe)
    headnode              Hostname of HPC Pack cluster head node
    ssl_thumbprint        Thumbprint of certificate which will be used in installation and communication with HPC Pack cluster
    client_cert           .pem file of client cert used for HPC Management REST API authentication

    optional arguments:
    -h, --help            show this help message and exit
    -g NODE_GROUP, --node_group NODE_GROUP
                            The node group in which we need to perform grow-shrink.


## FAQ
#### Convert .pfx to .pem using [OpenSSL](https://github.com/openssl/openssl)
```openssl pkcs12 -in file.pfx -out file.pem -nodes```