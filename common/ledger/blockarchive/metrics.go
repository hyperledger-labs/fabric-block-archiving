/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/
package blockarchive

import "github.com/hyperledger/fabric/common/metrics"

var (
	ArchivedBlockfileSizeOpts = metrics.GaugeOpts{
		Namespace:  "blockarchive",
		Subsystem:  "",
		Name:       "archived_size",
		Help:       "Total size of archived blockfiles.",
		LabelNames: []string{"channel"},
		// StatsdFormat: "%{#fqname}.%{channel}",
	}
)

type BlockVaultStats struct {
	archivedBlockfileSize metrics.Gauge
}

func NewBlockVaultStats(metricsProvider metrics.Provider) *BlockVaultStats {
	stats := &BlockVaultStats{}
	stats.archivedBlockfileSize = metricsProvider.NewGauge(ArchivedBlockfileSizeOpts)
	return stats
}

type ledgerStats struct {
	stats    *BlockVaultStats
	ledgerid string
}

func (s *BlockVaultStats) LedgerStats(ledgerid string) *ledgerStats {
	return &ledgerStats{
		s, ledgerid,
	}
}

func (s *ledgerStats) UpdateArchivedBlockfileSize(size int64) {
	// casting uint64 to float64 guarentees precision for the numbers upto 9,007,199,254,740,992 (1<<53)
	// since, we are not expecting the blockchains of this scale anytime soon, we go ahead with this for now.
	s.stats.archivedBlockfileSize.With("channel", s.ledgerid).Set(float64(size))
}
