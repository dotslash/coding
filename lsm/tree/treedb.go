package tree

import (
	"errors"
	"fmt"
)

type DbErrorType string

const (
	KeyNotExists       DbErrorType = "KeyNotExists"
	KeyMarkedAsDeleted DbErrorType = "KeyMarkedAsDeleted"
	ROTable            DbErrorType = "ROTable"
	Success            DbErrorType = "Success"
	InternalError      DbErrorType = "InternalError"
)

type KVStoreMode string

const (
	ModeInMem  KVStoreMode = "ModeInMem"
	ModeInFile KVStoreMode = "ModeInFile"
	ModeHybrid KVStoreMode = "ModeHybrid"
)

type DbError struct {
	ErrorType DbErrorType
	error
}

func (err *DbError) Error() string {
	if err.error == nil {
		return "No Error"
	}
	return fmt.Sprintf("%s: %s", err.ErrorType, err.error.Error())
}

func (err *DbError) Success() bool {
	return err.ErrorType == Success
}

func (err *DbError) GetError() error {
	return err.error
}

var NoError = DbError{
	ErrorType: Success,
	error:     nil,
}

func NewError(message string, errorType DbErrorType) DbError {
	return NewRawError(errors.New(message), errorType)
}

func NewRawError(err error, errorType DbErrorType) DbError {
	return DbError{
		ErrorType: errorType,
		error:     err,
	}
}

func NewInternalError(err error) DbError {
	return NewRawError(err, InternalError)
}

type KVStore interface {
	Mode() KVStoreMode
	Put(key, value []byte) DbError
	Delete(key []byte) DbError
	Get(key []byte) (value []byte, err DbError)
}
