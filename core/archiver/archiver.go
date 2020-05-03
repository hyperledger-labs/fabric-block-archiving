/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

// Package archiver initializes blockarchive package
package archiver

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger"
	"github.com/hyperledger/fabric/gossip/service"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
)

var loggerArchive = flogging.MustGetLogger("archiver.common")

// The least number of data chunks which a peer node should keep on local file system
const confArchivingRoleArchiver = "ledger.archiving.archiver.enabled"

// The least number of data chunks which a peer node should keep on local file system
const confArchivingRoleClient = "ledger.archiving.client.enabled"

// The least number of data chunks which a peer node should keep on local file system
const confArchivingKeep = "ledger.archiving.keepblocks"

// InitBlockArchiver initializes the BlockArchiver functions
func InitBlockArchiver(config *ledger.Config, gossipService *service.GossipService) {
	loggerArchive.Info("Archiver.InitBlockArchiver...")

	if viper.IsSet(confArchivingRoleArchiver) {
		blockarchive.IsArchiver = viper.GetBool(confArchivingRoleArchiver)
	} else {
		blockarchive.IsArchiver = false
	}

	if viper.IsSet(confArchivingRoleClient) {
		blockarchive.IsClient = viper.GetBool(confArchivingRoleClient)
	} else {
		blockarchive.IsClient = false
	}

	if blockarchive.IsArchiver && blockarchive.IsClient {
		loggerArchive.Warning("Invalid configuration: archiver vs client is mutual exclusive. Both will be turned off")
		blockarchive.IsArchiver = false
		blockarchive.IsClient = false
	} else if blockarchive.IsArchiver || blockarchive.IsClient {
		blockarchive.NumKeepLatestBlocks = 100
		if viper.IsSet(confArchivingKeep) {
			blockarchive.NumKeepLatestBlocks = viper.GetInt(confArchivingKeep)
		}
	}

	blockarchive.BlockStorePath = kvledger.BlockStorePath(config.RootFSPath)
	blockarchive.GossipService = gossipService

	loggerArchive.Info("Archiver.InitBlockArchiver isArchiver=", blockarchive.IsArchiver, " isClient=", blockarchive.IsClient)
}
