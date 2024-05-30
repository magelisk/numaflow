package types

import "github.com/numaproj/numaflow/pkg/isb"

type ResponseFlatmap struct {
	ParentMessage *isb.ReadMessage
	Uid           string
	RespMessage   *isb.WriteMessage
}

type WriteMsgFlatmap struct {
	Message *ResponseFlatmap
	AckIt   bool
}

type AckMsgFlatmap struct {
	Message *isb.ReadMessage
	AckIt   bool
}