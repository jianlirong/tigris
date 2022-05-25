package search

import (
	"fmt"
	"net/http"
)

type ErrCode byte

const (
	ErrCodeInvalid           ErrCode = 0x00
	ErrCodeDuplicate         ErrCode = 0x01
	ErrCodeNotFound          ErrCode = 0x02
	ErrCodeIndexingDocuments ErrCode = 0x03
	ErrCodeUnhandled         ErrCode = 0x04
)

var (
	ErrDuplicateEntity = NewSearchError(http.StatusConflict, ErrCodeDuplicate, "entity already exists")
	ErrNotFound        = NewSearchError(http.StatusNotFound, ErrCodeNotFound, "not found")
)

type Error struct {
	httpCode int
	code     ErrCode
	msg      string
	wrapped  error
}

func NewSearchError(httpCode int, code ErrCode, msg string, args ...interface{}) error {
	return Error{httpCode: httpCode, code: code, msg: fmt.Sprintf(msg, args...)}
}

func (se Error) Error() string {
	return se.msg
}

func IsSearchError(err error) bool {
	switch err.(type) {
	case *Error:
		return true
	}
	return false
}
