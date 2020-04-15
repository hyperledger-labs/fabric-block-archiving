/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fsblkstorage

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	l "github.com/hyperledger/fabric/core/ledger"
	gossipcommon "github.com/hyperledger/fabric/gossip/common"
)

// fsBlockStore - filesystem based implementation for `BlockStore`
type fsBlockStore struct {
	id       string
	conf     *Conf
	fileMgr  *blockfileMgr
	stats    *ledgerStats
	archiver *blockfileArchiver
}

// NewFsBlockStore constructs a `FsBlockStore`
func newFsBlockStore(id string, conf *Conf, indexConfig *blkstorage.IndexConfig,
	dbHandle *leveldbhelper.DBHandle, stats *stats) *fsBlockStore {
	fileMgr := newBlockfileMgr(id, conf, indexConfig, dbHandle)

	// create ledgerStats and initialize blockchain_height stat
	ledgerStats := stats.ledgerStats(id)
	info := fileMgr.getBlockchainInfo()
	ledgerStats.updateBlockchainHeight(info.Height)

	return &fsBlockStore{id, conf, fileMgr, ledgerStats, newBlockArchivingRoutine(id, fileMgr)}
}

// AddBlock adds a new block
func (store *fsBlockStore) AddBlock(block *common.Block) error {
	// track elapsed time to collect block commit time
	startBlockCommit := time.Now()
	result := store.fileMgr.addBlock(block)
	elapsedBlockCommit := time.Since(startBlockCommit)

	store.updateBlockStats(block.Header.Number, elapsedBlockCommit)

	return result
}

// GetBlockchainInfo returns the current info about blockchain
func (store *fsBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return store.fileMgr.getBlockchainInfo(), nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (store *fsBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	loggerBlkStreamArchive.Info("To be supported")
	return store.fileMgr.retrieveBlocks(startNum)
}

// RetrieveBlockByHash returns the block for given block-hash
func (store *fsBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	loggerBlkStreamArchive.Info("To be supported")
	return store.fileMgr.retrieveBlockByHash(blockHash)
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (store *fsBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	loggerBlkStreamArchive.Infof("Retrieving block [%d] from local", blockNum)
	block, err := store.fileMgr.retrieveBlockByNumber(blockNum)
	if err != nil && blockarchive.IsClient {
		loggerBlkStreamArchive.Infof("Retrieving block [%d] from archiver", blockNum)
		return blockarchive.GossipService.RetrieveBlockFromArchiver(blockNum, gossipcommon.ChannelID(store.id))
	}
	return block, err
}

// RetrieveTxByID returns a transaction for given transaction id
func (store *fsBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	loggerBlkStreamArchive.Infof("Retrieving tx [%s] from local", txID)
	envelope, err := store.fileMgr.retrieveTransactionByID(txID)
	switch err.(type) {
	case nil, l.NotFoundInIndexErr:
		return envelope, err
	default:
		if blockarchive.IsClient {
			loggerBlkStreamArchive.Infof("Retrieving tx [%s] from archiver: %+v", txID, err)
			envelope, err = blockarchive.GossipService.RetrieveTxFromArchiver(txID, gossipcommon.ChannelID(store.id))
		}
		return envelope, err
	}
}

// RetrieveTxByID returns a transaction for given transaction id
func (store *fsBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	loggerBlkStreamArchive.Info("To be supported")
	return store.fileMgr.retrieveTransactionByBlockNumTranNum(blockNum, tranNum)
}

func (store *fsBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	loggerBlkStreamArchive.Info("To be supported")
	return store.fileMgr.retrieveBlockByTxID(txID)
}

func (store *fsBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	loggerBlkStreamArchive.Info("To be supported")
	return store.fileMgr.retrieveTxValidationCodeByTxID(txID)
}

// Shutdown shuts down the block store
func (store *fsBlockStore) Shutdown() {
	logger.Debugf("closing fs blockStore:%s", store.id)
	store.fileMgr.close()
}

func (store *fsBlockStore) updateBlockStats(blockNum uint64, blockstorageCommitTime time.Duration) {
	store.stats.updateBlockchainHeight(blockNum + 1)
	store.stats.updateBlockstorageCommitTime(blockstorageCommitTime)
}
