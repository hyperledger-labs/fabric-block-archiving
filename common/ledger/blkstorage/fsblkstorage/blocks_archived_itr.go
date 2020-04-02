package fsblkstorage

import (
	"sync"

	"github.com/hyperledger/fabric/common/ledger/blockarchive"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger"
	common "github.com/hyperledger/fabric/gossip/common"
)

// blocksArchivedItr - an iterator for iterating over a sequence of blocks
type blocksArchivedItr struct {
	mgr                  *blockfileMgr
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	// stream               *blockArchiveStream
	closeMarker     bool
	closeMarkerLock *sync.Mutex
}

var loggerBlkStreamArchive = flogging.MustGetLogger("fsblkstorage.archive")

// func newBlockItr(mgr *blockfileMgr, startBlockNum uint64) *blocksArchivedItr {
// 	mgr.cpInfoCond.L.Lock()
// 	defer mgr.cpInfoCond.L.Unlock()
// 	archivedBlockHeight, _ := mgr.index.getLastArchivedBlockIndexed()
// 	if startBlockNum <= archivedBlockHeight {
// 		logger.Infof("Referring archived block: block[%d]", startBlockNum)
// 		return &blocksArchivedItr{mgr, mgr.cpInfo.lastBlockNumber, startBlockNum, nil, false, &sync.Mutex{}}
// 	} else {
// 		logger.Infof("Referring local block: block[%d]", startBlockNum)
// 		return &blocksArchivedItr{mgr, mgr.cpInfo.lastBlockNumber, startBlockNum, nil, false, &sync.Mutex{}}
// 	}
// }

func (itr *blocksArchivedItr) waitForBlock(blockNum uint64) uint64 {
	loggerBlkStreamArchive.Info("")
	itr.mgr.cpInfoCond.L.Lock()
	defer itr.mgr.cpInfoCond.L.Unlock()
	for itr.mgr.cpInfo.lastBlockNumber < blockNum && !itr.shouldClose() {
		logger.Debugf("Going to wait for newer blocks. maxAvailaBlockNumber=[%d], waitForBlockNum=[%d]",
			itr.mgr.cpInfo.lastBlockNumber, blockNum)
		itr.mgr.cpInfoCond.Wait()
		logger.Debugf("Came out of wait. maxAvailaBlockNumber=[%d]", itr.mgr.cpInfo.lastBlockNumber)
	}
	return itr.mgr.cpInfo.lastBlockNumber
}

// func (itr *blocksArchivedItr) initStream() error {
// 	loggerBlkStreamArchive.Info("")
// 	// var lp *fileLocPointer
// 	var err error
// 	// if lp, err = itr.mgr.index.getBlockLocByBlockNum(itr.blockNumToRetrieve); err != nil {
// 	// 	return err
// 	// }
// 	if itr.stream, err = newBlockArchivedStream(); err != nil {
// 		return err
// 	}
// 	return nil
// }

func (itr *blocksArchivedItr) shouldClose() bool {
	loggerBlkStreamArchive.Info("")
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	return itr.closeMarker
}

// Next moves the cursor to next block and returns true iff the iterator is not exhausted
func (itr *blocksArchivedItr) Next() (ledger.QueryResult, error) {
	loggerBlkStreamArchive.Info("")
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		itr.maxBlockNumAvailable = itr.waitForBlock(itr.blockNumToRetrieve)
	}
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	if itr.closeMarker {
		return nil, nil
	}
	// if itr.stream == nil {
	// 	logger.Debugf("Initializing block stream for iterator. itr.maxBlockNumAvailable=%d", itr.maxBlockNumAvailable)
	// 	if err := itr.initStream(); err != nil {
	// 		return nil, err
	// 	}
	// }

	block, err := blockarchive.GossipService.RetrieveBlockFromArchiver(itr.blockNumToRetrieve, common.ChannelID(itr.mgr.chainID))
	// nextBlockBytes, err := itr.stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	itr.blockNumToRetrieve++
	// return deserializeBlock(nextBlockBytes)
	return block, err
}

// Close releases any resources held by the iterator
func (itr *blocksArchivedItr) Close() {
	loggerBlkStreamArchive.Info("")
	itr.mgr.cpInfoCond.L.Lock()
	defer itr.mgr.cpInfoCond.L.Unlock()
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	itr.closeMarker = true
	itr.mgr.cpInfoCond.Broadcast()
	// if itr.stream != nil {
	// 	itr.stream.close()
	// }
}
