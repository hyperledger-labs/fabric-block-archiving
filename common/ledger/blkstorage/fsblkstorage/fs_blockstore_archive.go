/*
Copyright FST & FAST (2018)

Provides public entry points to some previously private methods
*/

package fsblkstorage

import (
	"fmt"

	"github.com/hyperledger/fabric/common/ledger/blockarchive"
)

// notifyArchiver notifies the finalization of blockfile via channel. It's called blockfile manager.
func (mgr *blockfileMgr) notifyArchiver(fileNum int) {
	loggerArchive.Info("mgr.notifyArchiver...")
	arChan := mgr.archiverChan
	if arChan != nil {
		loggerArchive.Info("mgr.notifyArchiver - sending message...")
		msg := blockarchive.ArchiverMessage{ChainID: mgr.chainID, BlockfileNum: fileNum}
		select {
		case arChan <- msg:
		default:
			loggerArchive.Warning("mgr.notifyArchiver - message not sent!!!")
		}
	}
}

func (mgr *blockfileMgr) SetBlkFileFinalizingChan(ch chan blockarchive.ArchiverMessage) {
	mgr.archiverChan = ch
}

func (store *fsBlockStore) DumpBlockfileInfo(blockNum uint64) string {
	flp, _ := store.fileMgr.index.getBlockLocByBlockNum(blockNum)
	fmt.Println(flp.String())
	return fmt.Sprintf(`{ "fileSuffixNum": %d, "offset": %d, "bytesLength": %d }`, flp.fileSuffixNum, flp.offset, flp.bytesLength)
}
