package jsonrpc

import (
	"fmt"
	"regexp"
	"strings"
)

type Message struct {
	ID      uint32      `json:"id"`
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

func NewMessage(id uint32, method string, params interface{}) *Message {
	return &Message{
		ID:      id,
		Version: "2.0",
		Method:  method,
		Params:  params,
	}
}

type RespErrorMsg string

type RespErrorCode int32

const (
	RespErrorCodeNoSuchProcess     = -3
	RespErrorCodeConnectionTimeout = -110
	RespErrorCodeNoFileExists      = -17
	RespErrorCodeNoSuchDevice      = -19
	// RespErrorCodeAlreadyExists is EALREADY. bdev_nvme_attach_controller
	// returns it ("A controller named ... already exists and multipath is
	// disabled") when the controller is still present from a prior attach —
	// SPDK keeps it reconnecting past a failed/slow connect.
	RespErrorCodeAlreadyExists = -114
)

type Response struct {
	ID        uint32         `json:"id"`
	Version   string         `json:"jsonrpc"`
	Result    interface{}    `json:"result,omitempty"`
	ErrorInfo *ResponseError `json:"error,omitempty"`
}

func (re ResponseError) Error() string {
	return fmt.Sprintf("{\"code\": %d,\"message\": \"%s\"}", re.Code, re.Message)
}

type ResponseError struct {
	Code    RespErrorCode `json:"code"`
	Message RespErrorMsg  `json:"message"`
}

type JSONClientError struct {
	ID          uint32
	Method      string
	Params      interface{}
	ErrorDetail error
}

func (re JSONClientError) Error() string {
	return fmt.Sprintf("error sending message, id %d, method %s, params %+v: %v",
		re.ID, re.Method, re.Params, re.ErrorDetail)
}

func IsJSONRPCRespErrorNoSuchProcess(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeNoSuchProcess
}

func IsJSONRPCRespErrorNoSuchDevice(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeNoSuchDevice
}

// IsJSONRPCRespErrorConnectionTimeout reports whether err is an SPDK JSON-RPC
// error with code -110 (ETIMEDOUT). This is returned when an NVMe-oF operation
// (e.g. bdev_nvme_detach_controller) times out against an unreachable or
// stalled peer; callers tearing down an already-broken connection can treat it
// as "the peer is gone" rather than a hard failure.
func IsJSONRPCRespErrorConnectionTimeout(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeConnectionTimeout
}

// IsJSONRPCRespErrorAlreadyExists reports whether err is an SPDK JSON-RPC error
// with code -114 (EALREADY) — the controller already exists. Callers retrying a
// bdev_nvme_attach_controller should not spin on this: the controller is
// present, so adopt it or clear it, don't re-attach.
func IsJSONRPCRespErrorAlreadyExists(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeAlreadyExists
}

func IsJSONRPCRespErrorFileExists(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeNoFileExists
}

func IsJSONRPCRespErrorBrokenPipe(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	_, ok = jsonRPCError.ErrorDetail.(*ResponseError)
	return !ok && strings.Contains(jsonRPCError.ErrorDetail.Error(), "broken pipe")
}

func IsJSONRPCRespErrorInvalidCharacter(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	_, ok = jsonRPCError.ErrorDetail.(*ResponseError)
	return !ok && strings.Contains(jsonRPCError.ErrorDetail.Error(), "invalid character")
}

func IsJSONRPCRespErrorTransportTypeAlreadyExists(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	_, ok = jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}
	matched, _ := regexp.MatchString("Transport type .* already exists", jsonRPCError.ErrorDetail.Error())
	return matched
}
