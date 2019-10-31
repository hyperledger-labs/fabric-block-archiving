/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package kvledger

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockarchive"
)

var logger_ar = flogging.MustGetLogger("archiver.archive")

// SetArchived deletes a blockfile and records it as archived
func (l *kvLedger) SetArchived(blockFileNo int, deleteTheFile bool) error {
	logger_ar.Info("kvledger.SetArchived... blockFileNo = ", blockFileNo)

	if blockarchive.IsClient || blockarchive.IsArchiver {
		return l.blockStore.SetBlockArchived(blockFileNo, deleteTheFile)
	}

	return nil
}
