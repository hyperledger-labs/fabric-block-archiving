/*
Copyright FST & FAST (2018)

Provides public entry points to some previously private methods
*/

package fsblkstorage

import (
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/stretchr/testify/assert"
)

func TestSendBlockfileToRepo(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()

	provider := env.provider
	store, _ := provider.OpenBlockStore("testLedger")
	defer store.Shutdown()

	blocks := testutil.ConstructTestBlocks(t, 5)
	for i := 0; i < 3; i++ {
		err := store.AddBlock(blocks[i])
		assert.NoError(t, err)
	}

	sendBlockfileToRepo("testLedger", 0)
}
