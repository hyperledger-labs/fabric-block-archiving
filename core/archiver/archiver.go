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

// InitBlockVault initializes the blockVault functions
func InitBlockVault(metricsProvider metrics.Provider) {
	logger_ar.Info("Archiver.InitBlockVault...")

	archiverStats := blockarchive.NewBlockVaultStats(metricsProvider)
	initBlockVaultParams(archiverStats)

	logger_ar.Info("Archiver.InitBlockVault isArchiver=", blockarchive.IsArchiver, " isClient-", blockarchive.IsClient)
}

func initBlockVaultParams(s *blockarchive.BlockVaultStats) {
	blockarchive.IsArchiver = viper.GetBool("peer.archiver.enabled")
	if blockarchive.IsArchiver {
		blockarchive.NumBlockfileEachArchiving, blockarchive.NumKeepLatestBlocks = ledgerconfig.GetArchivingParameters()
	} else {
		blockarchive.IsClient = viper.GetBool("peer.archiving.enabled")
	}

	blockarchive.BlockVaultDir = ledgerconfig.GetBlockVaultDir()
	blockarchive.BlockVaultURL = ledgerconfig.GetBlockVaultURL()
	blockarchive.BlockStorePath = ledgerconfig.GetBlockStorePath()

	blockarchive.ArchiverStats = s
}
