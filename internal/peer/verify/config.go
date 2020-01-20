/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"path/filepath"

	coreconfig "github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/spf13/viper"
)

func ledgerConfig() *ledger.Config {
	// set defaults
	warmAfterNBlocks := 1
	if viper.IsSet("ledger.state.couchDBConfig.warmIndexesAfterNBlocks") {
		warmAfterNBlocks = viper.GetInt("ledger.state.couchDBConfig.warmIndexesAfterNBlocks")
	}
	internalQueryLimit := 1000
	if viper.IsSet("ledger.state.couchDBConfig.internalQueryLimit") {
		internalQueryLimit = viper.GetInt("ledger.state.couchDBConfig.internalQueryLimit")
	}
	maxBatchUpdateSize := 500
	if viper.IsSet("ledger.state.couchDBConfig.maxBatchUpdateSize") {
		maxBatchUpdateSize = viper.GetInt("ledger.state.couchDBConfig.maxBatchUpdateSize")
	}
	collElgProcMaxDbBatchSize := 5000
	if viper.IsSet("ledger.pvtdataStore.collElgProcMaxDbBatchSize") {
		collElgProcMaxDbBatchSize = viper.GetInt("ledger.pvtdataStore.collElgProcMaxDbBatchSize")
	}
	collElgProcDbBatchesInterval := 1000
	if viper.IsSet("ledger.pvtdataStore.collElgProcDbBatchesInterval") {
		collElgProcDbBatchesInterval = viper.GetInt("ledger.pvtdataStore.collElgProcDbBatchesInterval")
	}
	purgeInterval := 100
	if viper.IsSet("ledger.pvtdataStore.purgeInterval") {
		purgeInterval = viper.GetInt("ledger.pvtdataStore.purgeInterval")
	}

	blockArchiverURL := "ledger-bank:222"
	if viper.IsSet("ledger.blockArchiver.url") {
		blockArchiverURL = viper.GetString("ledger.blockArchiver.url")
	}
	blockArchiverDir := "/tmp"
	if viper.IsSet("ledger.blockArchiver.dir") {
		blockArchiverDir = viper.GetString("ledger.blockArchiver.dir")
	}
	archiverEach := 30
	if viper.IsSet("peer.archiver.each") {
		archiverEach = viper.GetInt("peer.archiver.each")
	}
	archiverKeep := 10
	if viper.IsSet("peer.archiver.keep") {
		archiverKeep = viper.GetInt("peer.archiver.keep")
	}

	rootFSPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "ledgersData")
	conf := &ledger.Config{
		RootFSPath: rootFSPath,
		StateDBConfig: &ledger.StateDBConfig{
			StateDatabase: viper.GetString("ledger.state.stateDatabase"),
			CouchDB:       &couchdb.Config{},
		},
		PrivateDataConfig: &ledger.PrivateDataConfig{
			MaxBatchSize:    collElgProcMaxDbBatchSize,
			BatchesInterval: collElgProcDbBatchesInterval,
			PurgeInterval:   purgeInterval,
		},
		HistoryDBConfig: &ledger.HistoryDBConfig{
			Enabled: viper.GetBool("ledger.history.enableHistoryDatabase"),
		},
		ArchiveConfig: &ledger.ArchiveConfig{
			BlockArchiverURL: blockArchiverURL,
			BlockArchiverDir: blockArchiverDir,
			ArchiverEach:     archiverEach,
			ArchiverKeep:     archiverKeep,
		},
	}

	if conf.StateDBConfig.StateDatabase == "CouchDB" {
		conf.StateDBConfig.CouchDB = &couchdb.Config{
			Address:                 viper.GetString("ledger.state.couchDBConfig.couchDBAddress"),
			Username:                viper.GetString("ledger.state.couchDBConfig.username"),
			Password:                viper.GetString("ledger.state.couchDBConfig.password"),
			MaxRetries:              viper.GetInt("ledger.state.couchDBConfig.maxRetries"),
			MaxRetriesOnStartup:     viper.GetInt("ledger.state.couchDBConfig.maxRetriesOnStartup"),
			RequestTimeout:          viper.GetDuration("ledger.state.couchDBConfig.requestTimeout"),
			InternalQueryLimit:      internalQueryLimit,
			MaxBatchUpdateSize:      maxBatchUpdateSize,
			WarmIndexesAfterNBlocks: warmAfterNBlocks,
			CreateGlobalChangesDB:   viper.GetBool("ledger.state.couchDBConfig.createGlobalChangesDB"),
			RedoLogPath:             filepath.Join(rootFSPath, "couchdbRedoLogs"),
			UserCacheSizeMBs:        viper.GetInt("ledger.state.couchDBConfig.cacheSize"),
		}
	}
	return conf
}
