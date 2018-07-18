# HPC Pack MESOS framework
[![Build Status](https://travis-ci.org/amat27/hpcpack-mesos.svg?branch=master)](https://travis-ci.org/amat27/hpcpack-mesos)

A MESOS scheduler framework which accepts offers from MESOS master and builds HPC Pack compute nodes for existing HPC cluster.

## Install
### Dependency
* Python 2.7
* pipenv

### Steps
1. Clone this repository
2. CD to the project folder
3. `pipenv install`

### Show help info
``` pipenv run python hpcframework.py -h```


## FAQ
### Convert .pfx to .pem using [OpenSSL](https://github.com/openssl/openssl)
```openssl pkcs12 -in file.pfx -out file.pem -nodes```