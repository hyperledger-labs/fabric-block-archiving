/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

// Package archive implements a hendler for ArchivedBlockfile gossip message from other peer(archiver)
package archive

import (
	"bytes"
	"sync"

	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/protoext"
	"github.com/hyperledger/fabric/gossip/util"

	proto "github.com/hyperledger/fabric-protos-go/gossip"
)

type gossip interface {
	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	// If passThrough is false, the messages are processed by the gossip layer beforehand.
	// If passThrough is true, the gossip layer doesn't intervene and the messages
	// can be used to send a reply back to the sender
	// (Interface defined in gossip/gossip/gossip.go)
	Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *proto.GossipMessage, <-chan protoext.ReceivedMessage)
}

// ledgerResources defines abilities that the ledger provides
type ledgerResources interface {
	// Delete a blockfile and record it as archived
	SetArchived(fileNum int, deleteTheFile bool) error
}

// Msg describes an ArchivedBlockfile message sent from a remote peer
type Msg interface {
	// BlockfileNo returns the blockfile number that was archived
	BlockfileNo() uint64
}

// msgImpl An implementation of an ArchivedBlockfile gossip message
type msgImpl struct {
	msg *proto.GossipMessage
}

func (mi *msgImpl) BlockfileNo() uint64 {
	return mi.msg.GetArchivedBlockfile().BlockfileNo
}

// Service is the object that controls the Archive gossip service
type Service interface {
	// Stop stops the Service
	Stop()
}

// NewService returns a new Service and starts message handler go routine
func NewService(gossip gossip, channelID common.ChannelID, ledger ledgerResources) Service {

	ar := &archiveSvcImpl{
		stopChan: make(chan struct{}, 1),
		logger:   util.GetLogger(util.ArchiveLogger, ""),
		ledger:   ledger,
		gossip:   gossip,
		channel:  channelID,
	}

	// Start the service
	go ar.start()
	return ar
}

// archiveSvcImpl is an implementation of an Service
type archiveSvcImpl struct {
	sync.Mutex
	stopChan chan struct{}
	stopWG   sync.WaitGroup
	logger   util.Logger
	ledger   ledgerResources
	gossip   gossip
	channel  common.ChannelID
}

func (ar *archiveSvcImpl) start() {
	ar.startHandlingMessages()
}

func (ar *archiveSvcImpl) startHandlingMessages() {
	ar.logger.Debug("startHandlingMessages - enter...")
	defer ar.logger.Debug("startHandlingMessages - exit...")

	// Create a read-only channel that accepts only ArchivedBlockfile messages
	// for the current blockchain channel
	adapterCh, _ := ar.gossip.Accept(func(message interface{}) bool {
		// Get only ArchivedBlockfile org and channel messages
		return message.(*proto.GossipMessage).Tag == proto.GossipMessage_CHAN_AND_ORG &&
			protoext.IsArchivedBlockfileMsg(message.(*proto.GossipMessage)) &&
			bytes.Equal(message.(*proto.GossipMessage).Channel, ar.channel)
	}, false)

	ar.stopWG.Add(1)
	go ar.handleMessages(adapterCh)
}

// handleMessages waits for ArchivedBlockfile messages and processes them
// It will quit processing when a message is received on stopChan
func (ar *archiveSvcImpl) handleMessages(inCh <-chan *proto.GossipMessage) {

	defer ar.stopWG.Done()

	for {
		ar.logger.Debug("AcceptAndHandleMessages - in for loop...")

		// Wait for the next ArchivedBlockfile gossip message (or Stop message)
		select {
		case <-ar.stopChan:
			// We've been asked to stop...
			return
		case gossipMsg, ok := <-inCh:
			ar.logger.Debug("AcceptAndHandleMessages - GotMessage ok=", ok)
			if ok {
				mPtr := &msgImpl{gossipMsg}
				ar.handleMessage(mPtr)
			} else {
				return
			}
		}
	}
}

func (ar *archiveSvcImpl) handleMessage(msg Msg) {
	ar.Lock()
	defer ar.Unlock()

	ar.logger.Debug("handleMessage: Got message: ", msg)

	fileNum := msg.BlockfileNo()
	ar.logger.Infof("handleMessage: BlockfileNo = %d", fileNum)

	ar.ledger.SetArchived(int(fileNum), true)
}

// Stop stops the Service
func (ar *archiveSvcImpl) Stop() {
	ar.logger.Debug("Stop - Entering")
	defer ar.logger.Debug("Stop - Exiting")

	// Send a message to the Stop channel
	ar.stopChan <- struct{}{}

	// Wait for all goroutines to finish
	ar.stopWG.Wait()
}
