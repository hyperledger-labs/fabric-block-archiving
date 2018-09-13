package archive

import (
	"runtime"
	"sync"
	"testing"

	"github.com/hyperledger/fabric/common/diag"
	"github.com/hyperledger/fabric/common/flogging/floggingtest"
	"github.com/hyperledger/fabric/gossip/common"
	gossipCommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/protoext"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/stretchr/testify/assert"
)

type clusterOfPeers struct {
	peersGossip map[string]*peerMockGossip
	peersLock   *sync.RWMutex
	id          string
}

type mockAcceptor struct {
	ch       chan *proto.GossipMessage
	acceptor common.MessageAcceptor
}

type peerMockGossip struct {
	cluster      *clusterOfPeers
	member       *discovery.NetworkMember
	acceptors    []*mockAcceptor
	acceptorLock *sync.RWMutex
	clusterLock  *sync.RWMutex
	id           string
}

func (g *peerMockGossip) Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *proto.GossipMessage, <-chan protoext.ReceivedMessage) {
	return nil, nil
}

func newGossip(peerID string, member *discovery.NetworkMember) *peerMockGossip {
	return &peerMockGossip{
		id:           peerID,
		member:       member,
		acceptorLock: &sync.RWMutex{},
		clusterLock:  &sync.RWMutex{},
		acceptors:    make([]*mockAcceptor, 0),
	}
}

type resource struct{}

func (r *resource) SetArchived(fileNum int, deleteTheFile bool) error {
	return nil
}

func TestNewArchiveService(t *testing.T) {
	selfNetworkMember := &discovery.NetworkMember{
		Endpoint: "p0",
		Metadata: []byte{},
		PKIid:    []byte{byte(0)},
	}

	mockGossip := newGossip("peer0", selfNetworkMember)
	resource := &resource{}
	chainid := gossipCommon.ChainID("mychannel")

	_ = NewArchiveService(mockGossip, chainid, resource)

	runtime.Gosched()

	logger, recorder := floggingtest.NewTestLogger(t, floggingtest.Named("goroutine"))
	diag.LogGoRoutines(logger)
	assert.Contains(t, string(recorder.Buffer().Contents()), "created by github.com/hyperledger/fabric/gossip/archive.(*archiveSvcImpl).startHandlingMessages")

}
