/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package fsblkstorage

import (
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var loggerArchive = flogging.MustGetLogger("archiver.archive")
var loggerArchiveClient = flogging.MustGetLogger("archiver.client")
var loggerArchiveCmn = flogging.MustGetLogger("archiver.common")

//
type blockfileArchiver struct {
	// Chain ID
	chainID string
	// Instance pointer to blockfile manager for the chain specified with chainID
	mgr *blockfileMgr
	// PATH to where blockfiles are stored on the local file system
	blockfileDir string
}

const (
	// Maximum of retries for catching up with the latest blockfile to be actually archived
	maxRetryForCatchUp = 1000
)

// newBlockArchivingRoutine create a blockfile archiver instance
// If peer runs in archiver mode, also do the following steps:
// - Create a channel to receive a notification when blockfile is finalized
// - Start go routine for listening to the notificationalso create a channel to receive a notification
func newBlockArchivingRoutine(id string, mgr *blockfileMgr) *blockfileArchiver {
	loggerArchiveCmn.Info("newBlockArchivingRoutine: ", id)

	blockfileDir := filepath.Join(blockarchive.BlockStorePath, ChainsDir, id)
	arch := &blockfileArchiver{id, mgr, blockfileDir}

	if blockarchive.IsArchiver || blockarchive.IsClient {
		loggerArchiveCmn.Info("newBlockArchivingRoutine - creating channel to notify the finalizing of each blockfile...")
		// Create a new channel to allow the blockfileMgr to send messages to the archiver
		blkFileFinalizingChan := make(chan blockarchive.ArchiverMessage, 5)
		arch.mgr.SetBlkFileFinalizingChan(blkFileFinalizingChan)
		// Start listening for messages from blockfileMgr
		loggerArchiveCmn.Info("newBlockArchivingRoutine - starting to listen the notification...")
		go arch.listenForBlkFileFinalizing(blkFileFinalizingChan)
	}

	return arch
}

// listenForBlkFileFinalizing listens to a notificationalso create a channel to receive a notification
// The check routine to see if archiving is necessary is triggered here.
func (arch *blockfileArchiver) listenForBlkFileFinalizing(blkFileFinalizingChan chan blockarchive.ArchiverMessage) {
	loggerArchiveCmn.Info("listenForBlkFileFinalizing...")

	for {
		select {
		case msg, ok := <-blkFileFinalizingChan:
			if !ok {
				loggerArchiveCmn.Info("listenForBlkFileFinalizing - channel closed")
				return
			}
			loggerArchiveCmn.Info("listenForBlkFileFinalizing - got message", msg)
			// Sanity check
			if arch.chainID != msg.ChainID {
				loggerArchiveCmn.Errorf("listenForBlkFileFinalizing - incorrect channel [%s] - [%s]! ", arch.chainID, msg.ChainID)
			} else {
				if blockarchive.IsArchiver {
					arch.archiveBlockfilelIfNecessary()

				} else if blockarchive.IsClient {
					arch.discardBlockfilelIfNecessary()

				}
			}
		}
	}

}

// discardBlockfilelIfNecessary is called every time a blockfile is finalized (reached the maximum size of data chunk) on client node.
// If there are enough amount of blockfiles on local file system to be discarded, the actual discarding routine will be triggered.
func (arch *blockfileArchiver) discardBlockfilelIfNecessary() {

	// Running in context of Client

	loggerArchiveClient.Infof("discardBlockfilelIfNecessary [%s]", arch.chainID)

	// get last archived block number from state info
	currentArchivedBlockHeight := blockarchive.GossipService.ReadArchivedBlockHeight(common.ChannelID(arch.chainID))

	// get last archived blockfile num
	var discardedBlockfileSuffix uint64
	var err error
	if discardedBlockfileSuffix, err = arch.mgr.index.getLastArchivedBlockfileIndexed(); err != nil {
		loggerArchiveClient.Info("Failed to get last archived blockfile suffix")
		discardedBlockfileSuffix = 0
	}

	// next target blockfile
	nextDiscardedBlockfileSuffix := discardedBlockfileSuffix + 1

	numKeepLatestBlocks := uint64(blockarchive.NumKeepLatestBlocks)

	var nextEndBlockNum uint64
	if nextEndBlockNum, err = arch.mgr.index.getEndBlockOfBlockfileIndexed(nextDiscardedBlockfileSuffix); err != nil {
		loggerArchiveClient.Error("Failed to get end block num")
		return
	}

	currentBlockHeight := arch.mgr.getBlockchainInfo().Height

	loggerArchiveClient.Infof("current ledger:%d  vs  current archived block:%d  vs  block discard next:%d ( keep:%d )",
		currentBlockHeight, currentArchivedBlockHeight, nextEndBlockNum, numKeepLatestBlocks)

	var newDiscardedBlockfileSuffix uint64
	for currentArchivedBlockHeight > nextEndBlockNum && (currentBlockHeight-nextEndBlockNum) > numKeepLatestBlocks {
		loggerArchiveClient.Infof("discarding blockfile_%06d (end with block #%d)", nextDiscardedBlockfileSuffix, nextEndBlockNum)
		if !arch.hasConfigBlockInBlockfile(nextDiscardedBlockfileSuffix) {
			// Delete nextDiscardedBlockfileSuffix
			if err := arch.deleteArchivedBlockfile(int(nextDiscardedBlockfileSuffix)); err != nil {
				loggerArchiveClient.Errorf("Failed to delete blockfile_%06d", nextDiscardedBlockfileSuffix)
				break
			}
		} else {
			loggerArchiveClient.Infof("Skip discarding blockfile_%06d as it includes config block", nextDiscardedBlockfileSuffix)
		}

		// After deleting
		newDiscardedBlockfileSuffix = nextDiscardedBlockfileSuffix
		nextDiscardedBlockfileSuffix = nextDiscardedBlockfileSuffix + 1
		if nextEndBlockNum, err = arch.mgr.index.getEndBlockOfBlockfileIndexed(nextDiscardedBlockfileSuffix); err != nil {
			loggerArchiveClient.Error("Failed to get end block num")
			break
		}
	}

	if newDiscardedBlockfileSuffix > 0 {
		arch.mgr.index.setLastArchivedBlockfileIndexed(newDiscardedBlockfileSuffix)
	}

}

func (arch *blockfileArchiver) hasConfigBlockInBlockfile(blockfileNum uint64) bool {
	configBlk, err := arch.getConfigBlockNum()
	if err != nil {
		return true
	}
	end, err := arch.mgr.index.getEndBlockOfBlockfileIndexed(blockfileNum)
	if err != nil {
		return true
	}
	start, err := arch.mgr.index.getEndBlockOfBlockfileIndexed(blockfileNum - 1)
	if err != nil {
		return true
	}
	return configBlk > start && configBlk <= end
}

func (arch *blockfileArchiver) getConfigBlockNum() (uint64, error) {
	latest, _ := arch.mgr.index.getLastBlockIndexed()
	block, err := arch.mgr.retrieveBlockByNumber(latest)
	if err != nil {
		loggerBlkStreamArchive.Error("Failed to retrieve block")
		return 0, errors.New("Failed to retrieve block")
	}
	configBlockNum, err := protoutil.GetLastConfigIndexFromBlock(block)
	if err != nil {
		loggerBlkStreamArchive.Error("Failed to retrieve info of config block")
		return 0, errors.New("Failed to retrieve info of config block")
	}
	return configBlockNum, nil
}

// archiveBlockfilelIfNecessary is called every time a blockfile is finalized (reached the maximum size of data chunk) on archiver node.
// If there are enough amount of blockfiles on local file system to be archived, the actual archiving routine will be triggered.
func (arch *blockfileArchiver) archiveBlockfilelIfNecessary() {

	// Running in context of Archiver

	loggerArchive.Infof("archiveBlockfilelIfNecessary [%s]", arch.chainID)

	var archivedBlockfileSuffix, newArchivedBlockfileSuffix, nextEndBlockNum, newEndBlockNum uint64
	var err error

	// get last archived blockfile num
	if archivedBlockfileSuffix, err = arch.mgr.index.getLastArchivedBlockfileIndexed(); err != nil {
		loggerArchive.Info("Failed to get last archived blockfile suffix")
		archivedBlockfileSuffix = 0
	}

	// next target blockfile
	nextArchivedBlockfileSuffix := archivedBlockfileSuffix + 1

	if nextEndBlockNum, err = arch.mgr.index.getEndBlockOfBlockfileIndexed(nextArchivedBlockfileSuffix); err != nil {
		loggerArchive.Error("Failed to get end block num")
		return
	}

	// Retrieve current ledger height from blockfile manager
	currentBlockHeight := arch.mgr.getBlockchainInfo().Height

	numKeepLatestBlocks := uint64(blockarchive.NumKeepLatestBlocks)

	loggerArchive.Infof("current block height:%d  vs  block archived in next archival:%d ( keep:%d )", currentBlockHeight, nextEndBlockNum, numKeepLatestBlocks)

	for (currentBlockHeight - nextEndBlockNum) > numKeepLatestBlocks {
		loggerArchive.Infof("archiving blockfile_%06d (end with block #%d)", nextArchivedBlockfileSuffix, nextEndBlockNum)

		// Values to be stored into index DB
		newEndBlockNum = nextEndBlockNum
		newArchivedBlockfileSuffix = nextArchivedBlockfileSuffix

		// Update for next iteration
		nextArchivedBlockfileSuffix = nextArchivedBlockfileSuffix + 1
		if nextEndBlockNum, err = arch.mgr.index.getEndBlockOfBlockfileIndexed(nextArchivedBlockfileSuffix); err != nil {
			loggerArchive.Error("Failed to get end block num")
			break
		}
		loggerArchive.Infof("continue... block archived in next archival:%d", nextEndBlockNum)
	}
	arch.mgr.index.setLastArchivedBlockfileIndexed(newArchivedBlockfileSuffix)
	arch.mgr.index.setLastArchivedBlockIndexed(newEndBlockNum)
	blockarchive.GossipService.UpdateArchivedBlockHeight(newEndBlockNum, common.ChannelID(arch.chainID))

}

// handleArchivedBlockfile - Called once a blockfile has been archived
func (arch *blockfileArchiver) handleArchivedBlockfile(fileNum int, deleteTheFile bool) error {

	loggerArchiveCmn.Info("blockfileArchiver.handleArchivedBlockfile...")

	// Delete the local blockfile if required
	if deleteTheFile {
		if err := arch.deleteArchivedBlockfile(fileNum); err != nil {
			return err
		}
	}

	return nil
}

// deleteArchivedBlockfile - Called once a blockfile has been archived to delete it from the local filesystem
func (arch *blockfileArchiver) deleteArchivedBlockfile(fileNum int) error {
	removeFilePath := deriveBlockfilePath(arch.blockfileDir, fileNum)
	err := os.Remove(removeFilePath)
	if err != nil {
		loggerArchiveCmn.Info("deleteArchivedBlockfile: Failed to remove: ", fileNum, " Error: ", err.Error())
		return err
	}

	loggerArchiveCmn.Info("deleteArchivedBlockfile - deleted local blockfile: ", fileNum)

	return nil
}
