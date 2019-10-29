/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

/*
This file implements issuing an action for verifying data consitency of
existing blocks, including archived ones on BlockArchiver, to archiving
system chaincode.
*/

package blockfile

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/internal/peer/common"
	cb "github.com/hyperledger/fabric/protos/common"
	pbl "github.com/hyperledger/fabric/protos/ledger/archive"
	pb "github.com/hyperledger/fabric/protos/peer"
	utils "github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type endorserClient struct {
	cf *BlockfileCmdFactory
}

func verifyBlockfileCmd(cf *BlockfileCmdFactory) *cobra.Command {
	verifyBlockfileCmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify blockfile of a specified channel.",
		Long:  "Verify blockfile of a specified channel. Requires '-c','-p' and '-i'.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return verifyBlockfile(cmd, cf)
		},
	}

	flagList := []string{
		"channelID",
		"mspPath",
		"mspID",
		"mspType",
	}

	attachFlags(verifyBlockfileCmd, flagList)

	return verifyBlockfileCmd
}

func (cc *endorserClient) verifyBlockfile() (*pbl.BlockfileVerifyResponse, error) {
	var err error

	invocation := &pb.ChaincodeInvocationSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{
			Type:        pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]),
			ChaincodeId: &pb.ChaincodeID{Name: "ascc"},
			Input: &pb.ChaincodeInput{Args: [][]byte{
				[]byte("VerifyBlockfile"),
				[]byte(channelID),
				[]byte(mspConfigPath),
				[]byte(mspID),
				[]byte(mspType),
			}},
		},
	}

	var prop *pb.Proposal
	c, _ := cc.cf.Signer.Serialize()
	prop, _, err = utils.CreateProposalFromCIS(cb.HeaderType_ENDORSER_TRANSACTION, "", invocation, c)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot create proposal")
	}

	var signedProp *pb.SignedProposal
	signedProp, err = utils.GetSignedProposal(prop, cc.cf.Signer)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot create signed proposal")
	}

	proposalResp, err := cc.cf.EndorserClient.ProcessProposal(context.Background(), signedProp)
	if err != nil {
		return nil, errors.WithMessage(err, "failed sending proposal")
	}

	if proposalResp.Response == nil || proposalResp.Response.Status != 200 {
		return nil, errors.Errorf("received bad response, status %d: %s", proposalResp.Response.Status, proposalResp.Response.Message)
	}

	verifyResult := &pbl.BlockfileVerifyResponse{}
	err = proto.Unmarshal(proposalResp.Response.Payload, verifyResult)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read qscc response")
	}

	return verifyResult, nil

}

func verifyBlockfile(cmd *cobra.Command, cf *BlockfileCmdFactory) error {
	//the global chainID filled by the "-c" command
	if channelID == common.UndefinedParamValue {
		return errors.New("Must supply channel ID")
	}

	if mspConfigPath == "" {
		return errors.New("MSP folder not configured")
	}

	if mspID == "" {
		return errors.New("MSPID was not provided")
	}

	logger.Infof("channel name = %s", channelID)
	logger.Infof("MSP folder path = %s", mspConfigPath)
	logger.Infof("MSPID = %s", mspID)
	logger.Infof("MSP type = %s", mspType)

	// Parsing of the command line is done so silence cmd usage
	cmd.SilenceUsage = true

	var err error
	if cf == nil {
		cf, err = InitCmdFactory(EndorserRequired, PeerDeliverNotRequired, OrdererNotRequired)
		if err != nil {
			return err
		}
	}

	client := &endorserClient{cf}

	verifyResult, err := client.verifyBlockfile()
	if err != nil {
		return err
	}
	jsonBytes, err := json.Marshal(verifyResult)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", string(jsonBytes))

	return nil
}
