/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package fsblkstorage

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/hyperledger/fabric/gossip/service"
	gossip_proto "github.com/hyperledger/fabric/protos/gossip"
)

var logger_ar = flogging.MustGetLogger("archiver.archive")
var logger_ar_cmn = flogging.MustGetLogger("archiver.common")

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
	logger_ar.Info("newBlockfileArchiver: ", id)

	blockfileDir := filepath.Join(blockarchive.BlockStorePath, ChainsDir, id)
	arch := &blockfileArchiver{id, mgr, blockfileDir, 1}

	if blockarchive.IsArchiver {
		logger_ar.Info("newBlockfileArchiver - creating archiverChan...")
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
	logger_ar.Info("listenForBlockfiles...")

	for {
		select {
		case msg, ok := <-archiverChan:
			if !ok {
				logger_ar.Info("listenForBlockfiles - channel closed")
				return
			}
			logger_ar.Info("listenForBlockfiles - got message", msg)
			// Sanity check
			if arch.chainID != msg.ChainID {
				logger_ar.Errorf("listenForBlockfiles - incorrect channel [%s] - [%s]! ", arch.chainID, msg.ChainID)
			}
			arch.archiveChannelIfNecessary()
		}
	}

}

// archiveChannelIfNecessary is called every time a blockfile is finalized (reached the maximum size of data chunk).
// If there are enough amount of blockfiles on local file system to be archived, the actual archiving routine will be triggered.
func (arch *blockfileArchiver) archiveChannelIfNecessary() {

	chainID := arch.chainID
	logger_ar.Infof("ArchiveChannelIfNecessary [%s]", chainID)

	numBlockfileEachArchiving := blockarchive.NumBlockfileEachArchiving
	numKeepLatestBlocks := blockarchive.NumKeepLatestBlocks

	if isNeedArchiving(arch.blockfileDir, numBlockfileEachArchiving+numKeepLatestBlocks) {
		for i := 0; i < numBlockfileEachArchiving; i++ {
			for j := 0; j < maxRetryForCatchUp; j++ {
				// alreadyArchived == true means the blockfile has already been archived.
				// When returning alreadyArchived = true, then retrying to the next blockfile
				// until occuring the actual archiving within the maximum retry count
				if alreadyArchived, err := arch.archiveBlockfile(arch.nextBlockfileNum, true); err != nil && alreadyArchived != true {
					logger_ar.Info("Failed: Archiver")
					break
				} else {
					logger_ar.Info("Succeeded: Archiver")
					arch.nextBlockfileNum++
					if alreadyArchived == false {
						break
					}
				}
			}
		}
	} else {
		logger_ar.Infof("[%s] There is no candidate to be deleted", chainID)
	}
}

// archiveBlockfile sends a blockfile to the Block Archiver repository and deletes it if required
func (arch *blockfileArchiver) archiveBlockfile(fileNum int, deleteTheFile bool) (bool, error) {

	logger_ar.Info("Archiving: archiveBlockfile  deleteTheFile=", deleteTheFile)

	// Send the blockfile to the repository
	if alreadyArchived, err := sendBlockfileToRepo(arch.chainID, fileNum); err != nil && alreadyArchived == false {
		logger_ar.Error(err)
		return alreadyArchived, err
	} else if alreadyArchived == true {
		logger_ar.Infof("[blockfile_%06d] Already archived. Skip...", fileNum)
		return alreadyArchived, nil
	}

	// Initiate and send a gossip message to let the other peers know...
	arch.sendArchivedMessage(fileNum)

	// Record the fact that the blockfile has been archived, and delete it locally if required
	if err := arch.SetBlockfileArchived(fileNum, deleteTheFile); err != nil {
		logger_ar.Error(err)
		return false, err
	}

	return false, nil
}

// sendArchivedMessage initiates and sends a gossip message to let the other peers know...
func (arch *blockfileArchiver) sendArchivedMessage(fileNum int) {
	logger_ar.Info("sendArchivedMessage...")

	// Tell the other nodes about the archived blockfile
	gossipMsg := arch.createGossipMsg(fileNum)
	service.GetGossipService().Gossip(gossipMsg)
}

// Based on createGossipMsg @ blocksprovider.go
func (arch *blockfileArchiver) createGossipMsg(fileNum int) *gossip_proto.GossipMessage {
	fnum := uint64(fileNum)
	gossipMsg := &gossip_proto.GossipMessage{
		Nonce:   0,
		Tag:     gossip_proto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(arch.chainID),
		Content: &gossip_proto.GossipMessage_ArchivedBlockfile{
			ArchivedBlockfile: &gossip_proto.ArchivedBlockfile{
				BlockfileNo: fnum,
			},
		},
	}
	return gossipMsg
}

// SetBlockfileArchived deletes a blockfile and records it as having been archived
func (arch *blockfileArchiver) SetBlockfileArchived(blockFileNo int, deleteTheFile bool) error {
	logger_ar_cmn.Info("blockfileArchiver.SetBlockfileArchived... blockFileNo = ", blockFileNo)

	if blockarchive.IsClient || blockarchive.IsArchiver {
		arch.handleArchivedBlockfile(blockFileNo, deleteTheFile)
	}

	return nil
}

// handleArchivedBlockfile - Called once a blockfile has been archived
func (arch *blockfileArchiver) handleArchivedBlockfile(fileNum int, deleteTheFile bool) error {

	logger_ar_cmn.Info("blockfileArchiver.handleArchivedBlockfile...")

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
		logger_ar_cmn.Info("deleteArchivedBlockfile: Failed to remove: ", fileNum, " Error: ", err.Error())
		return err
	}

	logger_ar_cmn.Info("deleteArchivedBlockfile - deleted local blockfile: ", fileNum)

	return nil
}

// isNeedArchiving - returns whether archiving should be triggered or not
func isNeedArchiving(blockfileFolder string, keepFileNum int) bool {
	logger_ar.Debugf("blockfileFolder=%s, keepFileNum=%d", blockfileFolder, keepFileNum)

	files, err := ioutil.ReadDir(blockfileFolder)
	if err != nil {
		logger_ar.Error(err)
		return false
	}

	if len(files) > keepFileNum {
		logger_ar.Debugf("%d blockfile(s) should be archived", len(files)-keepFileNum)
		return true
	}

	logger_ar.Debug("There is no blockfile to be archived yet")
	return false
}
