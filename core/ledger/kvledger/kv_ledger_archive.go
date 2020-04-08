/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package kvledger

import (
	"github.com/hyperledger/fabric/common/flogging"
)

var loggerArchive = flogging.MustGetLogger("archiver.archive")

func (l *kvLedger) DumpBlockInfo(blockNum uint64) string {
	return l.blockStore.DumpBlockfileInfo(blockNum)
}
