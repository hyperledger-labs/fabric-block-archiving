/*
COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
*/

// Package blockarchive manage all configuration required for archiver peer node and
// client peer node to running a network with archiving feature.
package blockarchive

// IsArchiver indicates whether archiver mode is enabled or not.
// Archiver mode and client mode are mutually exclusive.
var IsArchiver bool

// IsClient indicates whether client mode is enabled or not.
var IsClient bool

// BlockStorePath is the absolute path to the root directory
// where blockfiles of all channels are stored.
var BlockStorePath string

// BlockArchiverDir is the absolute path to the root directory
// where archived blockfiles of all channels are stored on the repository.
var BlockArchiverDir string

// BlockArchiverURL is URL of the repository
var BlockArchiverURL string

// NumBlockfileEachArchiving is the number of data chunks archived
// on each archiving opportunity at once
var NumBlockfileEachArchiving int

// NumKeepLatestBlocks is the least number of data chunks
// which a peer node should keep on local file system
var NumKeepLatestBlocks int

// ArchiverMessage is the message that contains which blockfile is archived
type ArchiverMessage struct {
	ChainID      string
	BlockfileNum int
}
