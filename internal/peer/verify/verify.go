/*
Copyright IBM Corp. 2016 All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/spf13/cobra"
)

const (
	verifyFuncName = "verify"
	verifyCmdDes   = "Operate a peer verify: start|reset|rollback|pause|resume|rebuild-dbs|upgrade-dbs."
)

var logger = flogging.MustGetLogger("verifyCmd")
var loggerResult = flogging.MustGetLogger("verifyResult")

// Cmd returns the cobra command for Node
func Cmd() *cobra.Command {
	verifyCmd.AddCommand(ledgerCmd())
	verifyCmd.AddCommand(txCmd())
	verifyCmd.AddCommand(blockCmd())
	// verifyCmd.AddCommand(resetCmd())
	// verifyCmd.AddCommand(rollbackCmd())
	// verifyCmd.AddCommand(pauseCmd())
	// verifyCmd.AddCommand(resumeCmd())
	// verifyCmd.AddCommand(rebuildDBsCmd())
	// verifyCmd.AddCommand(upgradeDBsCmd())
	return verifyCmd
}

var verifyCmd = &cobra.Command{
	Use:              verifyFuncName,
	Short:            fmt.Sprint(verifyCmdDes),
	Long:             fmt.Sprint(verifyCmdDes),
	PersistentPreRun: common.InitCmd,
}
