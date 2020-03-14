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
	// Postfix number of the blockfile which should be archived next
	nextBlockfileNum int
	// Height of discarded block on the local ledger
	currDiscardedBlockHeigh uint64
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
	arch := &blockfileArchiver{id, mgr, blockfileDir, 1, 0}

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
	currentArchivedBlockHeigh := blockarchive.GossipService.ReadArchivedBlockHeight(common.ChannelID(arch.chainID))

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

	loggerArchiveClient.Infof("current archived block height:%d  vs  block discard next:%d ( keep:%d )", currentArchivedBlockHeigh, nextEndBlockNum, numKeepLatestBlocks)

	var newDiscardedBlockfileSuffix uint64
	for currentArchivedBlockHeigh > nextEndBlockNum && (currentArchivedBlockHeigh-nextEndBlockNum) > numKeepLatestBlocks {
		loggerArchiveClient.Infof("discarding blockfile_%06d (end with block #%d)", nextDiscardedBlockfileSuffix, nextEndBlockNum)

		// Delete nextDiscardedBlockfileSuffix
		if err := arch.deleteArchivedBlockfile(int(nextDiscardedBlockfileSuffix)); err != nil {
			loggerArchiveClient.Errorf("Failed to delete blockfile_%06d", nextDiscardedBlockfileSuffix)
			return
		}

		// After deleting
		newDiscardedBlockfileSuffix = nextDiscardedBlockfileSuffix
		nextDiscardedBlockfileSuffix = nextDiscardedBlockfileSuffix + 1
		if nextEndBlockNum, err = arch.mgr.index.getEndBlockOfBlockfileIndexed(nextDiscardedBlockfileSuffix); err != nil {
			loggerArchiveClient.Error("Failed to get end block num")
			return
		}
	}
	arch.mgr.index.setLastArchivedBlockfileIndexed(newDiscardedBlockfileSuffix)

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

		newEndBlockNum = nextEndBlockNum
		newArchivedBlockfileSuffix = nextArchivedBlockfileSuffix
		nextArchivedBlockfileSuffix = nextArchivedBlockfileSuffix + 1
		if newEndBlockNum, err = arch.mgr.index.getEndBlockOfBlockfileIndexed(nextArchivedBlockfileSuffix); err != nil {
			loggerArchive.Error("Failed to get end block num")
			break
		}
	}
	arch.mgr.index.setLastArchivedBlockfileIndexed(newArchivedBlockfileSuffix)
	arch.mgr.index.setLastArchivedBlockIndexed(newEndBlockNum)
	blockarchive.GossipService.UpdateArchivedBlockHeight(newEndBlockNum, common.ChannelID(arch.chainID))

}

// archiveBlockfile sends a blockfile to the Block Archiver repository and deletes it if required
func (arch *blockfileArchiver) archiveBlockfile(fileNum int, deleteTheFile bool) (bool, error) {

	loggerArchive.Info("Archiving: archiveBlockfile  deleteTheFile=", deleteTheFile)

	// Send the blockfile to the repository
	if alreadyArchived, err := sendBlockfileToRepo(arch.blockfileDir, fileNum); err != nil && alreadyArchived == false {
		loggerArchive.Error(err)
		return alreadyArchived, err
	} else if alreadyArchived == true {
		loggerArchive.Infof("[blockfile_%06d] Already archived. Skip...", fileNum)
		return alreadyArchived, nil
	}

	// Record the fact that the blockfile has been archived, and delete it locally if required
	if err := arch.SetBlockfileArchived(fileNum, deleteTheFile); err != nil {
		loggerArchive.Error(err)
		return false, err
	}

	return false, nil
}

// SetBlockfileArchived deletes a blockfile and records it as having been archived
func (arch *blockfileArchiver) SetBlockfileArchived(blockFileNo int, deleteTheFile bool) error {
	loggerArchiveCmn.Info("blockfileArchiver.SetBlockfileArchived... blockFileNo = ", blockFileNo)

	if blockarchive.IsClient || blockarchive.IsArchiver {
		arch.handleArchivedBlockfile(blockFileNo, deleteTheFile)
	}

	return nil
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

// isNeedArchiving - returns whether archiving should be triggered or not
func (arch *blockfileArchiver) isNeedArchiving() bool {

	var archivedBlockHeight uint64
	var err error
	// Retrieve current archived block height from index DB
	if archivedBlockHeight, err = arch.mgr.index.getLastArchivedBlockIndexed(); err != nil {
		loggerArchive.Error("Failed to get last archived block number")
		return false
	}

	// Retrieve current ledger height from blockfile manager
	currentBlockHeight := arch.mgr.getBlockchainInfo().Height

	numKeepLatestBlocks := blockarchive.NumKeepLatestBlocks
	loggerArchive.Infof("ledger height : %d  vs  last archived block : %d", currentBlockHeight, archivedBlockHeight)
	if currentBlockHeight-archivedBlockHeight > uint64(numKeepLatestBlocks) {
		return true
	} else {
		return false
	}
}

// isNeedArchiving - returns whether archiving should be triggered or not
func (arch *blockfileArchiver) isNeedDiscarding() bool {

	// Retrieve current archived block height from index DB
	remoteArchivedBlockHeight := blockarchive.GossipService.ReadArchivedBlockHeight(common.ChannelID(arch.chainID))
	localCurrentBlockHeigh := arch.mgr.getBlockchainInfo().Height
	localDiscardedBlockHeight := arch.currDiscardedBlockHeigh
	// numKeepLatestBlocks := blockarchive.NumKeepLatestBlocks
	loggerArchiveClient.Infof("ledger height : %d  vs  last archived block : %d  vs  discarded : %d", localCurrentBlockHeigh, remoteArchivedBlockHeight, localDiscardedBlockHeight)
	return false
}
