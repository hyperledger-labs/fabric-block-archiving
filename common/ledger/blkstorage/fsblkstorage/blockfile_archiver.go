/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package fsblkstorage

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/hyperledger/fabric/gossip/service"
	gossip_proto "github.com/hyperledger/fabric/protos/gossip"
)

var logger_ar = flogging.MustGetLogger("archiver.archive")
var logger_ar_cmn = flogging.MustGetLogger("archiver.common")

type blockfileArchiver struct {
	chainID                    string
	mgr                        *blockfileMgr
	blockfileDir               string
	nextBlockfileNum           int
	totalArchivedBlockfileSize int64
}

func newBlockfileArchiver(id string, mgr *blockfileMgr) *blockfileArchiver {
	logger_ar.Info("newBlockfileArchiver: ", id)

	blockfileDir := filepath.Join(blockarchive.BlockStorePath, ChainsDir, id)
	arch := &blockfileArchiver{id, mgr, blockfileDir, 1, 0}

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

func (arch *blockfileArchiver) archiveChannelIfNecessary() {

	chainID := arch.chainID
	logger_ar.Infof("ArchiveChannelIfNecessary [%s]", chainID)

	numBlockfileEachArchiving := blockarchive.NumBlockfileEachArchiving
	numKeepLatestBlocks := blockarchive.NumKeepLatestBlocks

	// TODO: Refactor this as it's not using the list at all...
	deleteCandidateList := getOldFileList(arch.blockfileDir, numBlockfileEachArchiving+numKeepLatestBlocks)
	if len(deleteCandidateList) > 0 {
		var err error

		for i := 0; i < numBlockfileEachArchiving; i++ {
			// TODO: I think this is a potential bug. What if we stop and restart archiver
			// - won't the block numbers start back at 1???
			err = arch.archiveBlockfile(arch.nextBlockfileNum, true)
			if err != nil {
				logger_ar.Info("Failed: Archiver")
			} else {
				logger_ar.Info("Succeeded: Archiver")
				arch.nextBlockfileNum++
				blockarchive.ArchiverStats.LedgerStats(chainID).UpdateArchivedBlockfileSize(arch.totalArchivedBlockfileSize)
			}
		}
	} else {
		logger_ar.Infof("[%s] There is no candidate to be deleted", chainID)
	}
}

// archiveBlockfile sends a blockfile to the blockVault and deletes it if required
func (arch *blockfileArchiver) archiveBlockfile(fileNum int, deleteTheFile bool) error {

	logger_ar.Info("Archiving: archiveBlockfile  deleteTheFile=", deleteTheFile)

	// Send the blockfile to the vault
	if err := sendBlockfileToVault(arch.chainID, fileNum); err != nil {
		logger_ar.Error(err)
		return err
	}

	// Initiate a gossip message to let the other peers know...
	arch.sendArchivedMessage(fileNum)

	fileSize := arch.getBlockfileSize(fileNum)
	arch.totalArchivedBlockfileSize += fileSize

	// Record the fact that the blockfile has been archived, and delete it locally if required
	if err := arch.SetBlockfileArchived(fileNum, deleteTheFile); err != nil {
		logger_ar.Error(err)
		return err
	}

	return nil
}

func (arch *blockfileArchiver) sendArchivedMessage(fileNum int) {
	logger_ar.Info("sendArchivedMessage...")

	// Tell the other nodes about the archived blockfile
	gossipMsg := arch.createGossipMsg(fileNum)
	service.GetGossipService().Gossip(gossipMsg)
}

// From blocksprovider.go
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

// SetBlockfileArchived records a block as having been archived
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

	// TODO: Need to record the fact that the blockfile has been archived...

	// Delete the local blockfile if required
	if deleteTheFile {
		if err := arch.deleteArchivedBlockfile(fileNum); err != nil {
			return err
		}
	}

	return nil
}

func (arch *blockfileArchiver) getBlockfileSize(fileNum int) int64 {

	srcFilePath := deriveBlockfilePath(arch.blockfileDir, fileNum)

	file, err := os.OpenFile(srcFilePath, os.O_RDONLY, 0600)
	if err != nil {
		logger_ar.Error(err)
		return 0
	}
	defer file.Close()

	fileinfo, staterr := file.Stat()
	if staterr != nil {
		logger_ar.Error(err)
		return 0
	}

	fileSize := fileinfo.Size()
	logger_ar.Infof("%s : %d", srcFilePath, fileSize)

	return fileSize
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

func getFileList(folderPath string) []string {
	logger_ar.Debugf("folderPath=%s", folderPath)
	var fileList []string
	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		logger_ar.Error(err)
		return fileList
	}
	for _, f := range files {
		logger_ar.Debugf("Append file : %s", f.Name())
		fileList = append(fileList, f.Name())
	}
	return fileList
}

/**
Block file name like these:
	blockfile_000000
	blockfile_000001
	blockfile_000002
	blockfile_000003
	blockfile_000004
If keepFileNum = 3 then these file will be called old file:
	blockfile_000000
	blockfile_000001
*/
func getOldFileList(blockfileFolder string, keepFileNum int) []string {
	logger_ar.Debugf("blockfileFolder=%s, keepFileNum=%d", blockfileFolder, keepFileNum)
	var oldFileList []string
	var allFileList []string
	allFileList = getFileList(blockfileFolder)
	if len(allFileList) == 0 {
		logger_ar.Debug("There is no blockfile yet")
		return oldFileList
	}
	// Sort descending
	sort.Sort(sort.Reverse(sort.StringSlice(allFileList)))
	for i, fileName := range allFileList {
		cnt := i + 1
		if cnt > keepFileNum {
			logger_ar.Debugf("Oldest blockfile %d : %s", cnt, fileName)
			oldFileList = append(oldFileList, fileName)
		}
	}
	logger_ar.Debugf("Return oldFileList : %s", oldFileList)
	return oldFileList
}
