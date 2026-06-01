package fsm

import "errors"

type CommandType uint8

const (
	CmdPutConfig       CommandType = iota + 1
	CmdDeleteConfig
	CmdCreateNamespace
	CmdDeleteNamespace
)

type Command struct {
	Type        CommandType
	Namespace   string
	Environment string
	Key         string
	Value       string
	UpdatedBy   string
	Comment     string
}

type CommandResponse struct {
	Version uint64
	Error   error
}

var ErrUnknownCommand = errors.New("unknown command type")
