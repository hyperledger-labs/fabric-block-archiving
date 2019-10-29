package archiver

import (
	"testing"

	"github.com/hyperledger/fabric/common/ledger/blockarchive"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/metrics/metricsfakes"
	"github.com/spf13/viper"
)

// TestAttrs tests attributes
func TestInitblockArchiverArchiver(t *testing.T) {
	provider := testutilConstructMetricProvider()
	viper.Set("peer.archiver.enabled", true)
	viper.Set("peer.archiving.enabled", false)

	InitblockArchiver(provider.fakeProvider)
}

func TestInitblockArchiverArchiving(t *testing.T) {
	provider := testutilConstructMetricProvider()
	viper.Set("peer.archiver.enabled", false)
	viper.Set("peer.archiving.enabled", true)

	InitblockArchiver(provider.fakeProvider)
}

func TestInitblockArchiverBoth(t *testing.T) {
	provider := testutilConstructMetricProvider()
	viper.Set("peer.archiver.enabled", true)
	viper.Set("peer.archiving.enabled", true)

	InitblockArchiver(provider.fakeProvider)
}

func TestInitblockArchiverNone(t *testing.T) {
	provider := testutilConstructMetricProvider()
	viper.Set("peer.archiver.enabled", false)
	viper.Set("peer.archiving.enabled", false)

	InitblockArchiver(provider.fakeProvider)
}

type testMetricProvider struct {
	fakeProvider                  *metricsfakes.Provider
	fakeArchiveBlockfileSizeGauge *metricsfakes.Gauge
}

func testutilConstructMetricProvider() *testMetricProvider {
	fakeProvider := &metricsfakes.Provider{}
	fakeArchiveBlockfileSizeGauge := testutilConstructGuage()
	fakeProvider.NewGaugeStub = func(opts metrics.GaugeOpts) metrics.Gauge {
		switch opts.Name {
		case blockarchive.ArchivedBlockfileSizeOpts.Name:
			return fakeArchiveBlockfileSizeGauge
		}
		return nil
	}
	return &testMetricProvider{
		fakeProvider,
		fakeArchiveBlockfileSizeGauge,
	}
}

func testutilConstructGuage() *metricsfakes.Gauge {
	fakeGauge := &metricsfakes.Gauge{}
	fakeGauge.WithStub = func(lableValues ...string) metrics.Gauge {
		return fakeGauge
	}
	return fakeGauge
}
