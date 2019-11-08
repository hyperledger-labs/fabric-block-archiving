/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package kvledger

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
)

var loggerArchive = flogging.MustGetLogger("archiver.archive")

// SetArchived deletes a blockfile and records it as archived
func (l *kvLedger) SetArchived(blockFileNo int, deleteTheFile bool) error {
	loggerArchive.Info("kvledger.SetArchived... blockFileNo = ", blockFileNo)

	if blockarchive.IsClient || blockarchive.IsArchiver {
		return l.blockStore.SetBlockArchived(blockFileNo, deleteTheFile)
	}

	return nil
}
