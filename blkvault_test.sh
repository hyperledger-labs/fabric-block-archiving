#!/bin/bash

COV=covdir

if [ ! -d $COV ]; then
  mkdir $COV
fi

if [ $(which gover | wc -l) -eq 0 ]; then
  go get github.com/sozorogami/gover
fi

go test -coverprofile=${COV}/fsblkstorage.coverprofile -v github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage
go test -coverprofile=${COV}/blkstorage.coverprofile -v github.com/hyperledger/fabric/common/ledger/blkstorage
go test -coverprofile=${COV}/blockarchiver.coverprofile -v github.com/hyperledger/fabric/common/ledger/blockarchiver
go test -coverprofile=${COV}/ledgerfsck.coverprofile -v github.com/hyperledger/fabric/common/ledger/util/ledgerfsck
go test -coverprofile=${COV}/archiver.coverprofile -v github.com/hyperledger/fabric/core/archiver
go test -coverprofile=${COV}/kvledger.coverprofile -v github.com/hyperledger/fabric/core/ledger/kvledger
go test -coverprofile=${COV}/ascc.coverprofile -v github.com/hyperledger/fabric/core/scc/ascc
go test -coverprofile=${COV}/ledgerconfig.coverprofile -v github.com/hyperledger/fabric/core/ledger/ledgerconfig
go test -coverprofile=${COV}/ledger.coverprofile -v github.com/hyperledger/fabric/core/ledger
go test -coverprofile=${COV}/committer.coverprofile -v github.com/hyperledger/fabric/core/committer
go test -coverprofile=${COV}/archive.coverprofile -v github.com/hyperledger/fabric/gossip/archive
go test -coverprofile=${COV}/channel.coverprofile -v github.com/hyperledger/fabric/gossip/channel
go test -coverprofile=${COV}/privdata.coverprofile -v github.com/hyperledger/fabric/gossip/privdata
go test -coverprofile=${COV}/protoext.coverprofile -v github.com/hyperledger/fabric/gossip/protoext
go test -coverprofile=${COV}/service.coverprofile -v github.com/hyperledger/fabric/gossip/service
go test -coverprofile=${COV}/state.coverprofile -v github.com/hyperledger/fabric/gossip/state
go test -coverprofile=${COV}/util.coverprofile -v github.com/hyperledger/fabric/gossip/util

gover ./covdir
go tool cover -html=gover.coverprofile