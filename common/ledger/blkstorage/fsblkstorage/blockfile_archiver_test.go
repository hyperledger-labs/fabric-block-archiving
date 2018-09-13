package fsblkstorage

import (
	"testing"

	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/stretchr/testify/assert"
)

// TestAttrs tests attributes
func TestBlockfileArchiver(t *testing.T) {
	blockarchive.IsArchiver = true
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	ledgerid := "testledger"
	w := newTestBlockfileWrapper(env, ledgerid)

	ar := newBlockfileArchiver(ledgerid, w.blockfileMgr)
	// defer ar.close()
	assert.NotNil(t, ar.mgr.archiverChan)
	// assert.NoError(t, err, "Error in constructing blockfile stream")

	// provider := testutilConstructMetricProvider()
	// viper.Set("peer.archiver.enabled", true)
	// viper.Set("peer.archiving.enabled", false)

	// InitBlockVault(provider.fakeProvider)
}
