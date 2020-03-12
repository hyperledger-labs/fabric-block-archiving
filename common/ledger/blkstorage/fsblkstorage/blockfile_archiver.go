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

// newBlockfileArchiver create a blockfile archiver instance
// If peer runs in archiver mode, also do the following steps:
// - Create a channel to receive a notification when blockfile is finalized
// - Start go routine for listening to the notificationalso create a channel to receive a notification
func newBlockfileArchiver(id string, mgr *blockfileMgr) *blockfileArchiver {
	loggerArchive.Info("newBlockfileArchiver: ", id)

	blockfileDir := filepath.Join(blockarchive.BlockStorePath, ChainsDir, id)
	arch := &blockfileArchiver{id, mgr, blockfileDir, 1, 0}

	if blockarchive.IsArchiver || blockarchive.IsClient {
		loggerArchive.Info("newBlockfileArchiver - creating archiverChan...")
		// Create a new channel to allow the blockfileMgr to send messages to the archiver
		archiverChan := make(chan blockarchive.ArchiverMessage, 5)
		arch.mgr.SetArchiverChan(archiverChan)

		// Start listening for messages from blockfileMgr
		go arch.listenForBlockfiles(archiverChan)
	}

	return arch
}

// listenForBlockfiles listens to a notificationalso create a channel to receive a notification
// The check routine to see if archiving is necessary is triggered here.
func (arch *blockfileArchiver) listenForBlockfiles(archiverChan chan blockarchive.ArchiverMessage) {
	loggerArchive.Info("listenForBlockfiles...")

	for {
		select {
		case msg, ok := <-archiverChan:
			if !ok {
				loggerArchive.Info("listenForBlockfiles - channel closed")
				return
			}
			loggerArchive.Info("listenForBlockfiles - got message", msg)
			// Sanity check
			if arch.chainID != msg.ChainID {
				loggerArchive.Errorf("listenForBlockfiles - incorrect channel [%s] - [%s]! ", arch.chainID, msg.ChainID)
			}
			arch.archiveChannelIfNecessary()
		}
	}

}

// archiveChannelIfNecessary is called every time a blockfile is finalized (reached the maximum size of data chunk).
// If there are enough amount of blockfiles on local file system to be archived, the actual archiving routine will be triggered.
func (arch *blockfileArchiver) archiveChannelIfNecessary() {

	loggerArchive.Infof("ArchiveChannelIfNecessary [%s]", arch.chainID)

	// numBlockfileEachArchiving := blockarchive.NumBlockfileEachArchiving

	if arch.isNeedArchiving() {
		loggerArchive.Infof("Discarding blockfile [%s]", arch.chainID)

		// for i := 0; i < numBlockfileEachArchiving; i++ {
		// 	for j := 0; j < maxRetryForCatchUp; j++ {
		// 		// alreadyArchived == true means the blockfile has already been archived.
		// 		// When returning alreadyArchived = true, then retrying to the next blockfile
		// 		// until occuring the actual archiving within the maximum retry count
		// 		if alreadyArchived, err := arch.archiveBlockfile(arch.nextBlockfileNum); err != nil && alreadyArchived != true {
		// 			loggerArchive.Info("Failed: Archiver")
		// 			break
		// 		} else {
		// 			loggerArchive.Info("Succeeded: Archiver")
		// 			arch.nextBlockfileNum++
		// 			if alreadyArchived == false {
		// 				break
		// 			}
		// 		}
		// 	}
		// }
	} else {
		loggerArchive.Infof("[%s] There is no candidate to be deleted", arch.chainID)
	}
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

	remoteArchivedBlockHeight := blockarchive.GossipService.ReadArchivedBlockHeight(common.ChannelID(arch.chainID))
	localCurrentBlockHeigh := arch.mgr.getBlockchainInfo().Height
	localDiscardedBlockHeight := arch.currDiscardedBlockHeigh
	// numKeepLatestBlocks := blockarchive.NumKeepLatestBlocks
	loggerArchive.Infof("local : %d  vs  archived : %d  vs  discarded : %d", localCurrentBlockHeigh, remoteArchivedBlockHeight, localDiscardedBlockHeight)
	return false
}
