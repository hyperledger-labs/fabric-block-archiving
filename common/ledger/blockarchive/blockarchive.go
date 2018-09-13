/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

package blockarchive

var IsArchiver bool
var IsClient bool
var BlockStorePath string
var BlockVaultDir string
var BlockVaultURL string
var NumBlockfileEachArchiving int
var NumKeepLatestBlocks int
var ArchiverStats *BlockVaultStats

type ArchiverMessage struct {
	ChainID      string
	BlockfileNum int
}
