package tree

import (
	"errors"
	"fmt"
)

type DbErrorType string

const (
	KeyNotExists  DbErrorType = "KeyNotExists"
	ROTable       DbErrorType = "ROTable"
	Success       DbErrorType = "Success"
	InternalError DbErrorType = "InternalError"
)

type DbError struct {
	ErrorType DbErrorType
	error
}

func (err *DbError) Error() string {
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

type treedb interface {
	Put(key, value string) DbError
	Delete(key string) DbError
	Get(key string) (value string, err DbError)
}
