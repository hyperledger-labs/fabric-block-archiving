/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package archiver

import (
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/hyperledger/fabric/common/metrics"
)

var logger_ar = flogging.MustGetLogger("archiver.common")

// InitBlockArchiver initializes the BlockArchiver functions
func InitBlockArchiver(metricsProvider metrics.Provider) {
	logger_ar.Info("Archiver.InitBlockArchiver...")

	initBlockArchiverParams()

	logger_ar.Info("Archiver.InitBlockArchiver isArchiver=", blockarchive.IsArchiver, " isClient-", blockarchive.IsClient)
}

func initBlockArchiverParams() {
	blockarchive.IsArchiver = viper.GetBool("peer.archiver.enabled")
	if blockarchive.IsArchiver {
		blockarchive.NumBlockfileEachArchiving, blockarchive.NumKeepLatestBlocks = ledgerconfig.GetArchivingParameters()
	} else {
		blockarchive.IsClient = viper.GetBool("peer.archiving.enabled")
	}

	blockarchive.BlockArchiverDir = ledgerconfig.GetBlockArchiverDir()
	blockarchive.BlockArchiverURL = ledgerconfig.GetBlockArchiverURL()
	blockarchive.BlockStorePath = ledgerconfig.GetBlockStorePath()

}
