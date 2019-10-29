#!/bin/bash
#
# COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
#

#
# This script includes all steps for applying your changes into your environment.
#
# Note:
#   - All blocks (including archived ones) are removed by running this script
#

buildall=0

# See if it's the first time for build after doing 'make clean' or not.
if [ ! -e "$GOPATH/src/github.com/hyperledger/fabric/.build/gotools.tar.bz2" ]; then
  # Missing 'gotools.tar.bz2' file means that it's right after doing 'make clean'
  echo "You need direct internet connection to build go tool set."
  read -p "Continue? [Y/n] " ans
  case "$ans" in
  y | Y | "")
    echo "proceeding ... (build from scratch)"
    buildall=1
    ;;
  n | N)
    echo "exiting..."
    exit 1
    ;;
  *)
    echo "invalid response"
    exit 1
    ;;
  esac
else
  echo "proceeding ... (rebuild)"
fi

# Stop and remove and containers
docker rm -f $(docker ps -aq)

# Rebuild the container images that are required for running peer node
pushd $GOPATH/src/github.com/hyperledger/fabric
if [ $buildall -eq 1 ]; then
  make peer-docker-clean && make docker && make native
else
  make peer-docker-clean && make peer-docker
fi
if [ $? -ne 0 ]; then
  echo "*** make docker failed!"
  exit 1
fi

popd

echo "Starting network for blockArchiver BDD..."
pushd ~/dev/fst-poc-fabric-env/feature
# This path depends on your choice when cloning repository
behave --no-skipped --tags=@dev ./blockArchiver.feature

popd
