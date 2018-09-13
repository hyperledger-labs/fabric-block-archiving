/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsblkstorage

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// ErrUnexpectedEndOfBlockfile error used to indicate an unexpected end of a file segment
// this can happen mainly if a crash occurs during appening a block and partial block contents
// get written towards the end of the file
var ErrUnexpectedEndOfBlockfile = errors.New("unexpected end of blockfile")

// blockfileStream reads blocks sequentially from a single file.
// It starts from the given offset and can traverse till the end of the file
type blockfileStream struct {
	fileNum       int
	file          *os.File
	sftpConnInfo  *sftpConnInfo
	reader        *bufio.Reader
	currentOffset int64
}

// blockStream reads blocks sequentially from multiple files.
// it starts from a given file offset and continues with the next
// file segment until the end of the last segment (`endFileNum`)
type blockStream struct {
	rootDir           string
	currentFileNum    int
	endFileNum        int
	currentFileStream *blockfileStream
}

// blockPlacementInfo captures the information related
// to block's placement in the file.
type blockPlacementInfo struct {
	fileNum          int
	blockStartOffset int64
	blockBytesOffset int64
}

type sftpConnInfo struct {
	file   *sftp.File
	client *ssh.Client
}

func OpenFileThroughSFTP(path string) (*sftpConnInfo, error) {

	logger.Info("OpenFileThroughSFTP")
	config := &ssh.ClientConfig{
		User: "root",
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		Auth: []ssh.AuthMethod{
			ssh.Password("blkstore"),
		},
	}
	config.SetDefaults()
	sshConn, err := ssh.Dial("tcp", ledgerconfig.GetBlockVaultURL(), config)
	if err != nil {
		return nil, err
	}
	// defer sshConn.Close()

	client, err := sftp.NewClient(sshConn)
	if err != nil {
		return nil, err
	}
	// defer client.Close()

	dstFilePath := filepath.Join(ledgerconfig.GetBlockVaultDir(), path)
	dstFile, err := client.Open(dstFilePath)
	if err != nil {
		return nil, err
	}
	// defer dstFile.Close()

	return &sftpConnInfo{dstFile, sshConn}, nil
}

func fileSeek(s io.Seeker, startOffset int64) (int64, error) {
	return s.Seek(startOffset, 0)
}

///////////////////////////////////
// blockfileStream functions
////////////////////////////////////
func newBlockfileStream(rootDir string, fileNum int, startOffset int64) (*blockfileStream, error) {
	filePath := deriveBlockfilePath(rootDir, fileNum)
	logger.Debugf("newBlockfileStream(): filePath=[%s], startOffset=[%d]", filePath, startOffset)
	var file *os.File
	var connInfo *sftpConnInfo
	var err error
	if file, err = os.OpenFile(filePath, os.O_RDONLY, 0600); err != nil {
		if connInfo, err = OpenFileThroughSFTP(filePath); err != nil {
			logger.Error(err)
			return nil, errors.Wrapf(err, "error opening block file %s", filePath)
		}
	}

	var newPosition int64
	var seeker io.Seeker
	if file != nil {
		seeker = file
	} else {
		seeker = connInfo.file
	}
	if newPosition, err = fileSeek(seeker, startOffset); err != nil {
		return nil, errors.Wrapf(err, "error seeking block file [%s] to startOffset [%d]", filePath, startOffset)
	}
	if newPosition != startOffset {
		panic(fmt.Sprintf("Could not seek block file [%s] to startOffset [%d]. New position = [%d]",
			filePath, startOffset, newPosition))
	}
	var s *blockfileStream
	if file != nil {
		s = &blockfileStream{fileNum, file, nil, bufio.NewReader(file), startOffset}
	} else {
		s = &blockfileStream{fileNum, nil, connInfo, bufio.NewReader(connInfo.file), startOffset}
	}
	return s, nil
}

func (s *blockfileStream) nextBlockBytes() ([]byte, error) {
	blockBytes, _, err := s.nextBlockBytesAndPlacementInfo()
	return blockBytes, err
}

// nextBlockBytesAndPlacementInfo returns bytes for the next block
// along with the offset information in the block file.
// An error `ErrUnexpectedEndOfBlockfile` is returned if a partial written data is detected
// which is possible towards the tail of the file if a crash had taken place during appending of a block
func (s *blockfileStream) nextBlockBytesAndPlacementInfo() ([]byte, *blockPlacementInfo, error) {
	var lenBytes []byte
	var err error
	var fileInfo os.FileInfo
	moreContentAvailable := true

	if s.sftpConnInfo != nil {
		// Access the blockfile via blockVault
		fileInfo, err = s.sftpConnInfo.file.Stat()
	} else {
		fileInfo, err = s.file.Stat()
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error getting block file stat")
	}
	if s.currentOffset == fileInfo.Size() {
		logger.Debugf("Finished reading file number [%d]", s.fileNum)
		return nil, nil, nil
	}
	remainingBytes := fileInfo.Size() - s.currentOffset
	// Peek 8 or smaller number of bytes (if remaining bytes are less than 8)
	// Assumption is that a block size would be small enough to be represented in 8 bytes varint
	peekBytes := 8
	if remainingBytes < int64(peekBytes) {
		peekBytes = int(remainingBytes)
		moreContentAvailable = false
	}
	logger.Debugf("Remaining bytes=[%d], Going to peek [%d] bytes", remainingBytes, peekBytes)
	if lenBytes, err = s.reader.Peek(peekBytes); err != nil {
		return nil, nil, errors.Wrapf(err, "error peeking [%d] bytes from block file", peekBytes)
	}
	length, n := proto.DecodeVarint(lenBytes)
	if n == 0 {
		// proto.DecodeVarint did not consume any byte at all which means that the bytes
		// representing the size of the block are partial bytes
		if !moreContentAvailable {
			return nil, nil, ErrUnexpectedEndOfBlockfile
		}
		panic(errors.Errorf("Error in decoding varint bytes [%#v]", lenBytes))
	}
	bytesExpected := int64(n) + int64(length)
	if bytesExpected > remainingBytes {
		logger.Debugf("At least [%d] bytes expected. Remaining bytes = [%d]. Returning with error [%s]",
			bytesExpected, remainingBytes, ErrUnexpectedEndOfBlockfile)
		return nil, nil, ErrUnexpectedEndOfBlockfile
	}
	// skip the bytes representing the block size
	if _, err = s.reader.Discard(n); err != nil {
		return nil, nil, errors.Wrapf(err, "error discarding [%d] bytes", n)
	}
	blockBytes := make([]byte, length)
	if _, err = io.ReadAtLeast(s.reader, blockBytes, int(length)); err != nil {
		logger.Errorf("Error reading [%d] bytes from file number [%d], error: %s", length, s.fileNum, err)
		return nil, nil, errors.Wrapf(err, "error reading [%d] bytes from file number [%d]", length, s.fileNum)
	}
	blockPlacementInfo := &blockPlacementInfo{
		fileNum:          s.fileNum,
		blockStartOffset: s.currentOffset,
		blockBytesOffset: s.currentOffset + int64(n)}
	s.currentOffset += int64(n) + int64(length)
	logger.Debugf("Returning blockbytes - length=[%d], placementInfo={%s}", len(blockBytes), blockPlacementInfo)
	return blockBytes, blockPlacementInfo, nil
}

func (s *blockfileStream) close() error {
	if s.sftpConnInfo != nil {
		// Close the blockVault connection
		if err := errors.WithStack(s.sftpConnInfo.file.Close()); err != nil {
			logger.Error(err.Error())
			return err
		}
		return errors.WithStack(s.sftpConnInfo.client.Close())
	} else {
		return errors.WithStack(s.file.Close())
	}
}

///////////////////////////////////
// blockStream functions
////////////////////////////////////
func newBlockStream(rootDir string, startFileNum int, startOffset int64, endFileNum int) (*blockStream, error) {
	startFileStream, err := newBlockfileStream(rootDir, startFileNum, startOffset)
	if err != nil {
		return nil, err
	}
	return &blockStream{rootDir, startFileNum, endFileNum, startFileStream}, nil
}

func (s *blockStream) moveToNextBlockfileStream() error {
	var err error
	if err = s.currentFileStream.close(); err != nil {
		return err
	}
	s.currentFileNum++
	if s.currentFileStream, err = newBlockfileStream(s.rootDir, s.currentFileNum, 0); err != nil {
		return err
	}
	return nil
}

func (s *blockStream) nextBlockBytes() ([]byte, error) {
	blockBytes, _, err := s.nextBlockBytesAndPlacementInfo()
	return blockBytes, err
}

func (s *blockStream) nextBlockBytesAndPlacementInfo() ([]byte, *blockPlacementInfo, error) {
	var blockBytes []byte
	var blockPlacementInfo *blockPlacementInfo
	var err error
	if blockBytes, blockPlacementInfo, err = s.currentFileStream.nextBlockBytesAndPlacementInfo(); err != nil {
		logger.Errorf("Error reading next block bytes from file number [%d]: %s", s.currentFileNum, err)
		return nil, nil, err
	}
	logger.Debugf("blockbytes [%d] read from file [%d]", len(blockBytes), s.currentFileNum)
	if blockBytes == nil && (s.currentFileNum < s.endFileNum || s.endFileNum < 0) {
		logger.Debugf("current file [%d] exhausted. Moving to next file", s.currentFileNum)
		if err = s.moveToNextBlockfileStream(); err != nil {
			return nil, nil, err
		}
		return s.nextBlockBytesAndPlacementInfo()
	}
	return blockBytes, blockPlacementInfo, nil
}

func (s *blockStream) close() error {
	return s.currentFileStream.close()
}

func (i *blockPlacementInfo) String() string {
	return fmt.Sprintf("fileNum=[%d], startOffset=[%d], bytesOffset=[%d]",
		i.fileNum, i.blockStartOffset, i.blockBytesOffset)
}
