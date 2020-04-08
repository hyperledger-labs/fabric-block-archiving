/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

// Package archiver initializes blockarchive package
package archiver

import (
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/gossip/service"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
)

var loggerArchive = flogging.MustGetLogger("archiver.common")

// InitBlockArchiver initializes the BlockArchiver functions
func InitBlockArchiver(gossipService *service.GossipService) {
	loggerArchive.Info("Archiver.InitBlockArchiver...")

	initBlockArchiverParams(gossipService)

	loggerArchive.Info("Archiver.InitBlockArchiver isArchiver=", blockarchive.IsArchiver, " isClient=", blockarchive.IsClient)
}

func initBlockArchiverParams(gossipService *service.GossipService) {
	blockarchive.IsArchiver = viper.GetBool("peer.archiver.enabled")
	if blockarchive.IsArchiver || blockarchive.IsClient {
		blockarchive.NumBlockfileEachArchiving, blockarchive.NumKeepLatestBlocks = ledgerconfig.GetArchivingParameters()
	} else {
		blockarchive.IsClient = viper.GetBool("peer.archiving.enabled")
	}

	blockarchive.BlockStorePath = ledgerconfig.GetBlockStorePath()
	blockarchive.GossipService = gossipService

}
