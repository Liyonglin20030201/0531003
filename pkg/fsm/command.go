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
	Type          CommandType
	Namespace     string
	Environment   string
	Key           string
	Value         string
	UpdatedBy     string
	Comment       string
	ExpectVersion uint64
}

type CommandResponse struct {
	Version        uint64
	CurrentVersion uint64
	Error          error
}

var (
	ErrUnknownCommand  = errors.New("unknown command type")
	ErrVersionConflict = errors.New("version conflict: current version does not match expected version")
)
