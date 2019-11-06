// +build blkarchivedbg

/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

/*
Package blockfile implements a new subcommand (blockfile) for manipulating
blockfiles to peer command.

Subcommand

'blockfile' subcommand includes the following subcommands:

	verify

Refer detail to the usage which is provided by running 'peer blockfile help'.
*/
package blockfile

import (
	"strings"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/msp"
	cb "github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var logger = flogging.MustGetLogger("archiver.cmd")

const (
	EndorserRequired       bool = true
	EndorserNotRequired    bool = false
	OrdererRequired        bool = true
	OrdererNotRequired     bool = false
	PeerDeliverRequired    bool = true
	PeerDeliverNotRequired bool = false
)

var (
	channelID     string
	mspConfigPath string
	mspID         string
	mspType       string
)

// Cmd returns the cobra command for Node
func Cmd(cf *BlockfileCmdFactory) *cobra.Command {
	AddFlags(blockfileCmd)

	blockfileCmd.AddCommand(verifyBlockfileCmd(cf))

	return blockfileCmd
}

// AddFlags adds flags for create and join
func AddFlags(cmd *cobra.Command) {
	common.AddOrdererFlags(cmd)
}

var flags *pflag.FlagSet

func init() {
	resetFlags()
}

// Explicitly define a method to facilitate tests
func resetFlags() {
	flags = &pflag.FlagSet{}

	flags.StringVarP(&channelID, "channelID", "c", common.UndefinedParamValue,
		`In case of a newChain command, the channel ID to create. It must be all lower case,
		less than 250 characters long and match the regular expression: [a-z][a-z0-9.-]*`)
	flags.StringVarP(&mspConfigPath, "mspPath", "p", "", "path to the msp folder")
	flags.StringVarP(&mspID, "mspID", "i", "", "the MSP identity of the organization")
	flags.StringVarP(&mspType, "mspType", "t", "bccsp", "the type of the MSP provider, default bccsp")
}

func attachFlags(cmd *cobra.Command, names []string) {
	cmdFlags := cmd.Flags()
	for _, name := range names {
		if flag := flags.Lookup(name); flag != nil {
			cmdFlags.AddFlag(flag)
		} else {
			logger.Fatalf("Could not find flag '%s' to attach to commond '%s'", name, cmd.Name())
		}
	}
}

var blockfileCmd = &cobra.Command{
	Use:              "blockfile",
	Short:            "Operate a blockfile: verify.",
	Long:             "Operate a blockfile: verify.",
	PersistentPreRun: common.InitCmd,
}

type BroadcastClientFactory func() (common.BroadcastClient, error)

type deliverClientIntf interface {
	GetSpecifiedBlock(num uint64) (*cb.Block, error)
	GetOldestBlock() (*cb.Block, error)
	GetNewestBlock() (*cb.Block, error)
	Close() error
}

// BlockfileCmdFactory holds the clients used by BlockfileCmdFactory
type BlockfileCmdFactory struct {
	EndorserClient   pb.EndorserClient
	Signer           msp.SigningIdentity
	BroadcastClient  common.BroadcastClient
	DeliverClient    deliverClientIntf
	BroadcastFactory BroadcastClientFactory
}

// InitCmdFactory init the BlockfileCmdFactory with clients to endorser and orderer according to params
func InitCmdFactory(isEndorserRequired, isPeerDeliverRequired, isOrdererRequired bool) (*BlockfileCmdFactory, error) {
	if isPeerDeliverRequired && isOrdererRequired {
		// this is likely a bug during development caused by adding a new cmd
		return nil, errors.New("ERROR - only a single deliver source is currently supported")
	}

	var err error
	cf := &BlockfileCmdFactory{}

	cf.Signer, err = common.GetDefaultSignerFnc()
	if err != nil {
		return nil, errors.WithMessage(err, "error getting default signer")
	}

	cf.BroadcastFactory = func() (common.BroadcastClient, error) {
		return common.GetBroadcastClientFnc()
	}

	// for join and list, we need the endorser as well
	if isEndorserRequired {
		// creating an EndorserClient with these empty parameters will create a
		// connection using the values of "peer.address" and
		// "peer.tls.rootcert.file"
		cf.EndorserClient, err = common.GetEndorserClientFnc(common.UndefinedParamValue, common.UndefinedParamValue)
		if err != nil {
			return nil, errors.WithMessage(err, "error getting endorser client for channel")
		}
	}

	// for fetching blocks from a peer
	if isPeerDeliverRequired {
		cf.DeliverClient, err = common.NewDeliverClientForPeer(channelID, cf.Signer)
		if err != nil {
			return nil, errors.WithMessage(err, "error getting deliver client for channel")
		}
	}

	// for create and fetch, we need the orderer as well
	if isOrdererRequired {
		if len(strings.Split(common.OrderingEndpoint, ":")) != 2 {
			return nil, errors.Errorf("ordering service endpoint %s is not valid or missing", common.OrderingEndpoint)
		}
		cf.DeliverClient, err = common.NewDeliverClientForOrderer(channelID, cf.Signer)
		if err != nil {
			return nil, err
		}
	}
	logger.Infof("Endorser and orderer connections initialized")
	return cf, nil
}
