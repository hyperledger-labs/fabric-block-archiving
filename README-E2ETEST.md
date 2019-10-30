# Hyperledger Fabric Block Archiving

## Running end-to-end tests

We are using [behave](https://behave.readthedocs.io/en/latest/) python package which is BDD (behaviour-driven development) for e2e testing and uses tests written in a natural language style, backed up by Python code. 

### Prerequisites

To run end-to-end test scenarios, you need to meet the following prerequisites. 

* Python 2.7+
* pip
* virtualenv (optional)
  * To isolate the test environment from other dev environment on your local

### Clone Test suites for Block Archiving feature

All scripts to run the end-2-end test are included https://github.com/nekia/fabric-block-archiving-testenv.git.

```
vagrant@ubuntu:~$ mkdir ~/dev
vagrant@ubuntu:~$ cd ~/dev
vagrant@ubuntu:~/dev$ git clone https://github.com/nekia/fabric-block-archiving-testenv.git
vagrant@ubuntu:~/dev$ cd fabric-block-archiving-testenv
```

### Download Hyperledger Fabric platform-specific binaries

In the end-2-end test, a simple Hyperledger Fabric network is actually deployed on your local environment. It's based on fabric-samples/first-network example. You need to download some binaries required for that.

```
vagrant@ubuntu:~/dev/fabric-block-archiving-testenv$ curl -sSL http://bit.ly/2ysbOFE | bash -s -- 2.0.0-alpha 2.0.0-alpha 0.4.15 -s -d
```
* You need to bypass docker image download(-d) and fabric-samples repo clone(-s)
* If you would download docker image by mistake, it would override image tag of fabric-peer container image which is built for Block Archiving feature with the original one. In that case, you need to assign tag manually as below:

  ```
  vagrant@ubuntu:~/dev/fabric-block-archiving-testenv$ docker tag hyperledger/fabric-peer:amd64-blkarchiver hyperledger/fabric-peer:2.0.0-alpha
  vagrant@ubuntu:~/dev/fabric-block-archiving-testenv$ docker tag hyperledger/fabric-peer:amd64-blkarchiver hyperledger/fabric-peer:latest
  ```

### Setup environment for end-to-end test

You need to install some python packages required for end-to-end testing via pip. If you don't use virualenv, you can skip the first 2 steps of the followings.

```
vagrant@ubuntu:~/dev/fabric-block-archiving-testenv$ virtualenv e2e-test
vagrant@ubuntu:~/dev/fabric-block-archiving-testenv$ . e2e-test/bin/activate
(e2e-test) vagrant@ubuntu:~/dev/fabric-block-archiving-testenv$ cd feature/
(e2e-test) vagrant@ubuntu:~/dev/fabric-block-archiving-testenv/feature$ pip install -r requirements.txt
```

### Run test scenario

By running the following command, the next some steps are automatically processed.

* Generating artifacts and credentials of Hyperledger Fabric network for every test scenario
* Launching Hyperledger Fabric network (2 organizations, 2 peers for each)
* Invoking a chaincode several times for generating blocks
* Validating block archiving status of each peer
* Verifying block data consisitency from genesis block to the latest one on each peer 

```
# Sanity check
(e2e-test) vagrant@ubuntu:~/dev/fabric-block-archiving-testenv/feature$ behave -t @dev -k blockArchiving.feature
# Full test (It takes about about 20 min to complete)
(e2e-test) vagrant@ubuntu:~/dev/fabric-block-archiving-testenv/feature$ behave -k blockArchiving.feature

(snip)

1 feature passed, 0 failed, 0 skipped
5 scenarios passed, 0 failed, 0 skipped
191 steps passed, 0 failed, 0 skipped, 0 undefined
Took 18m21.142s

vagrant@ubuntu:~/dev/fabric-block-archiving-testenv/feature$ deactivate
```
