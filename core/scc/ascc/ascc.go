/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

/*
Package ascc implements handlers for our newly created system chaincode
(ascc: archiving system chaincode) of archiving features.

Functions

The following feature are currently implemented.

	VerifyBlockfile

Monitoring threads

This package also starts 2 Go routine to monitor local disk usage for archiver feature
and archiving (discarding) feature.
*/
package ascc

import (
	"fmt"
	"sync"

	proto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/ledgerfsck"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/peer"
	pbl "github.com/hyperledger/fabric/protos/ledger/archive"
	pb "github.com/hyperledger/fabric/protos/peer"

	"github.com/hyperledger/fabric/core/archiver"
)

var logger_ar = flogging.MustGetLogger("archiver.cc")
var once sync.Once

// These are function names from Invoke first parameter
const (
	VerifyBlockfile string = "VerifyBlockfile"
)

type scc struct{}

// New returns an implementation of the chaincode interface
func New(metricsProvider metrics.Provider) *ArchiveSysCC {
	logger_ar.Info("New")

	ascc := &ArchiveSysCC{}

	once.Do(func() {
		archiver.InitBlockArchiver(metricsProvider)
	})

	return ascc
}

func (s *ArchiveSysCC) Name() string              { return "ascc" }
func (s *ArchiveSysCC) Path() string              { return "github.com/hyperledger/fabric/core/scc/ascc" }
func (s *ArchiveSysCC) InitArgs() [][]byte        { return nil }
func (s *ArchiveSysCC) Chaincode() shim.Chaincode { return s }
func (s *ArchiveSysCC) InvokableExternal() bool   { return true }
func (s *ArchiveSysCC) InvokableCC2CC() bool      { return true }
func (s *ArchiveSysCC) Enabled() bool             { return true }

type ArchiveSysCC struct {
}

// Init implements the chaincode shim interface
func (s *ArchiveSysCC) Init(stub shim.ChaincodeStubInterface) pb.Response {
	logger_ar.Info("Init")
	return shim.Success(nil)
}

// Invoke implements the chaincode shim interface
func (s *ArchiveSysCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	logger_ar.Info("Invoke")
	args := stub.GetArgs()

	if len(args) < 2 {
		logger_ar.Error("Incorrect number of arguments")
		return shim.Error(fmt.Sprintf("Incorrect number of arguments, %d", len(args)))
	}
	fname := string(args[0])
	cid := string(args[1])

	targetLedger := peer.GetLedger(cid)
	if targetLedger == nil {
		logger_ar.Error("Invalid chain ID")
		return shim.Error(fmt.Sprintf("Invalid chain ID, %s", cid))
	}

	logger_ar.Info("Invoke function: %s on chain: %s", fname, cid)

	// Handle ACL:
	// 1. get the signed proposal
	_, err := stub.GetSignedProposal()
	if err != nil {
		return shim.Error(fmt.Sprintf("Failed getting signed proposal from stub, %s: %s", cid, err))
	}

	// // 2. check the channel reader policy
	// res := getACLResource(fname)
	// if err = e.aclProvider.CheckACL(res, cid, sp); err != nil {
	// 	return shim.Error(fmt.Sprintf("access denied for [%s][%s]: [%s]", fname, cid, err))
	// }

	switch fname {
	case VerifyBlockfile:
		logger_ar.Info("VerifyBlockfile")
		fsck := &ledgerfsck.LedgerFsck{}
		fsck.ChannelName = cid
		fsck.MspConfigPath = string(args[2])
		fsck.MspID = string(args[3])
		fsck.MspType = string(args[4])
		fsck.Ledger = targetLedger
		return verifyBlockfile(fsck)
	}

	return shim.Error(fmt.Sprintf("Requested function %s not found.", fname))
}

func verifyBlockfile(fsck *ledgerfsck.LedgerFsck) pb.Response {

	var result bool
	var err error
	if result, err = fsck.InitAndVerify(); err != nil {
		logger_ar.Error("Failed to execute initAndVerify()")
		return shim.Error(err.Error())
	}

	ret := &pbl.BlockfileVerifyResponse{}
	ret.Pass = result
	ret.ChannelID = fsck.ChannelName

	var retByte []byte
	if retByte, err = proto.Marshal(ret); err != nil {
		logger_ar.Error("Failed to marshal result")
		return shim.Error(err.Error())
	}

	return shim.Success(retByte)

}
