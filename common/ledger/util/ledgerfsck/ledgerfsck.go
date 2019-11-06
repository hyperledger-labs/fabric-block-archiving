// +build blkarchivedbg

/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package ledgerfsck

/*
This file includes implementaions for verification of blocks.
This implementation is based on https://github.com/C0rWin/ledgerfsck.
*/

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	gossipCommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/internal/peer/gossip"
	"github.com/hyperledger/fabric/msp/mgmt"
	pb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protoutil"
	utils "github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("archiver.ledgerfsck")

// LedgerFsck is arguments data for verification of blocks
type LedgerFsck struct {
	ChannelName   string
	MspConfigPath string
	MspID         string
	MspType       string

	Ledger ledger.PeerLedger
	bundle *channelconfig.Bundle
}

func (fsck *LedgerFsck) Manager(channelID string) (policies.Manager, bool) {
	return fsck.bundle.PolicyManager(), true
}

// // Initialize
// func (fsck *LedgerFsck) Initialize() error {
// 	// Initialize viper configuration
// 	viper.SetEnvPrefix("core")
// 	viper.AutomaticEnv()
// 	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

// 	err := common.InitConfig("core")
// 	if err != nil {
// 		logger.Errorf("failed to initialize configuration, because of %s", err)
// 		return err
// 	}
// 	return nil
// }

// ReadConfiguration read configuration parameters
func (fsck *LedgerFsck) ReadConfiguration() error {
	// Read configuration parameters
	// flag.StringVar(&fsck.ChannelName, "ChannelName", "testChannel", "channel name to check the integrity")
	// flag.StringVar(&fsck.MspConfigPath, "mspPath", "", "path to the msp folder")
	// flag.StringVar(&fsck.MspID, "MspID", "", "the MSP identity of the organization")
	// flag.StringVar(&fsck.MspType, "MspType", "bccsp", "the type of the MSP provider, default bccsp")
	// flag.Parse()

	if fsck.MspConfigPath == "" {
		errMsg := "MSP folder not configured"
		logger.Error(errMsg)
		return errors.New(errMsg)
	}

	if fsck.MspID == "" {
		errMsg := "MSPID was not provided"
		logger.Error(errMsg)
		return errors.New(errMsg)
	}

	logger.Infof("channel name = %s", fsck.ChannelName)
	logger.Infof("MSP folder path = %s", fsck.MspConfigPath)
	logger.Infof("MSPID = %s", fsck.MspID)
	logger.Infof("MSP type = %s", fsck.MspType)
	return nil
}

// InitCrypto
func (fsck *LedgerFsck) InitCrypto() error {
	// Next need to init MSP
	err := common.InitCrypto(fsck.MspConfigPath, fsck.MspID, fsck.MspType)
	if err != nil {
		logger.Errorf("failed to initialize MSP related configuration, failure %s", err)
		return err
	}
	return nil
}

// OpenLedger
func (fsck *LedgerFsck) OpenLedger() error {
	ledgerIds, err := ledgermgmt.GetLedgerIDs()
	if err != nil {
		errMsg := fmt.Sprintf("failed to read ledger, because of %s", err)
		logger.Errorf(errMsg)
		return errors.New(errMsg)
	}

	// Check whenever channel name has corresponding ledger
	var found = false
	for _, name := range ledgerIds {
		if name == fsck.ChannelName {
			found = true
		}
	}

	if !found {
		errMsg := fmt.Sprintf("there is no ledger corresponding to the provided channel name %s. Exiting...", fsck.ChannelName)
		logger.Errorf(errMsg)
		return errors.New(errMsg)
	}

	if fsck.Ledger, err = ledgermgmt.OpenLedger(fsck.ChannelName); err != nil {
		errMsg := fmt.Sprintf("failed to open ledger %s, because of the %s", fsck.ChannelName, err)
		logger.Errorf(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

// GetLatestChannelConfigBundle
func (fsck *LedgerFsck) GetLatestChannelConfigBundle() error {
	var cb *pb.Block
	var err error
	if cb, err = getCurrConfigBlockFromLedger(fsck.Ledger); err != nil {
		logger.Warningf("Failed to find config block on ledger %s(%s)", fsck.ChannelName, err)
		return err
	}

	qe, err := fsck.Ledger.NewQueryExecutor()
	defer qe.Done()
	if err != nil {
		logger.Errorf("failed to obtain query executor, error is %s", err)
		return err
	}

	logger.Info("reading configuration from state DB")
	confBytes, err := qe.GetState("", "resourcesconfigtx.CHANNEL_CONFIG_KEY")
	if err != nil {
		logger.Errorf("failed to read channel config, error %s", err)
		return err
	}
	conf := &pb.Config{}
	err = proto.Unmarshal(confBytes, conf)
	if err != nil {
		logger.Errorf("could not read configuration, due to %s", err)
		return err
	}

	if conf != nil {
		logger.Info("initialize channel config bundle")
		fsck.bundle, err = channelconfig.NewBundle(fsck.ChannelName, conf)
		if err != nil {
			return err
		}
	} else {
		// Config was only stored in the statedb starting with v1.1 binaries
		// so if the config is not found there, extract it manually from the config block
		logger.Info("configuration wasn't stored in state DB retrieving config envelope from ledger")
		envelopeConfig, err := utils.ExtractEnvelope(cb, 0)
		if err != nil {
			return err
		}

		logger.Info("initialize channel config bundle from config transaction")
		fsck.bundle, err = channelconfig.NewBundleFromEnvelope(envelopeConfig)
		if err != nil {
			return err
		}
	}

	capabilitiesSupportedOrPanic(fsck.bundle)

	channelconfig.LogSanityChecks(fsck.bundle)

	return nil
}

func (fsck *LedgerFsck) Verify() bool {
	blockchainInfo, err := fsck.Ledger.GetBlockchainInfo()
	if err != nil {
		logger.Errorf("could not obtain blockchain information "+
			"channel name %s, due to %s", fsck.ChannelName, err)
		return false
	}

	logger.Infof("ledger height of channel %s, is %d\n", fsck.ChannelName, blockchainInfo.Height)

	signer := mgmt.GetLocalSigningIdentityOrPanic()

	mcs := gossip.NewMCS(
		fsck,
		signer,
		mgmt.NewDeserializersManager())

	block, err := fsck.Ledger.GetBlockByNumber(uint64(0))
	if err != nil {
		logger.Errorf("failed to read genesis block number, with error", err)
		return false
	}

	// Get hash of genesis block
	prevHash := protoutil.BlockHeaderHash(block.Header)

	// complete full scan and check over ledger blocks
	for blockIndex := uint64(1); blockIndex < blockchainInfo.Height; blockIndex++ {
		block, err := fsck.Ledger.GetBlockByNumber(blockIndex)
		if err != nil {
			logger.Errorf("failed to read block number %d from ledger, with error", blockIndex, err)
			return false
		}

		if !bytes.Equal(prevHash, block.Header.PreviousHash) {
			logger.Errorf("block number [%d]: hash comparison has failed, previous block hash %x doesn't"+
				" equal to hash claimed within block header %x", blockIndex, prevHash, block.Header.PreviousHash)
			return false
		} else {
			logger.Infof("block number [%d]: previous hash matched", blockIndex)
		}

		signedBlock, err := proto.Marshal(block)
		if err != nil {
			logger.Errorf("failed marshaling block, due to", err)
			return false
		}

		if err := mcs.VerifyBlock(gossipCommon.ChainID(fsck.ChannelName), block.Header.Number, signedBlock); err != nil {
			logger.Errorf("failed to verify block with sequence number %d. %s", blockIndex, err)
			return false
		} else {
			logger.Infof("Block [seq = %d], hash = [%x], previous hash = [%x], VERIFICATION PASSED",
				blockIndex, prevHash, block.Header.PreviousHash)
		}
		prevHash = protoutil.BlockHeaderHash(block.Header)
	}
	return true
}

func (fsck *LedgerFsck) InitAndVerify() (bool, error) {
	// fsck := &LedgerFsck{}
	// Initialize configuration
	// if err := fsck.Initialize(); err != nil {
	// 	os.Exit(-1)
	// }
	// Read configuration parameters
	if err := fsck.ReadConfiguration(); err != nil {
		return false, err
	}
	// Init crypto & MSP
	if err := fsck.InitCrypto(); err != nil {
		return false, err
	}
	// OpenLedger
	// if err := fsck.OpenLedger(); err != nil {
	// 	return err
	// }
	// GetLatestChannelConfigBundle
	if err := fsck.GetLatestChannelConfigBundle(); err != nil {
		return false, err
	}

	ret := fsck.Verify()

	return ret, nil
}

// getCurrConfigBlockFromLedger read latest configuratoin block from the ledger
func getCurrConfigBlockFromLedger(ledger ledger.PeerLedger) (*pb.Block, error) {
	logger.Debugf("Getting config block")

	// get last block.  Last block number is Height-1
	blockchainInfo, err := ledger.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	lastBlock, err := ledger.GetBlockByNumber(blockchainInfo.Height - 1)
	if err != nil {
		return nil, err
	}

	// get most recent config block location from last block metadata
	configBlockIndex, err := utils.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		return nil, err
	}

	// get most recent config block
	configBlock, err := ledger.GetBlockByNumber(configBlockIndex)
	if err != nil {
		return nil, err
	}

	logger.Debugf("Got config block[%d]", configBlockIndex)
	return configBlock, nil
}

func capabilitiesSupportedOrPanic(res channelconfig.Resources) {
	ac, ok := res.ApplicationConfig()
	if !ok {
		logger.Panicf("[channel %s] does not have application config so is incompatible", res.ConfigtxValidator().ChainID())
	}

	if err := ac.Capabilities().Supported(); err != nil {
		logger.Panicf("[channel %s] incompatible %s", res.ConfigtxValidator(), err)
	}

	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		logger.Panicf("[channel %s] incompatible %s", res.ConfigtxValidator(), err)
	}
}
