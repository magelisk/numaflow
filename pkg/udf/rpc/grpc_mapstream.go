/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rpc

import (
	"context"
	"fmt"
	"log"
	"time"

	mapstreampb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapstreamer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedMapStream is a map stream applier that uses gRPC client to invoke the map stream UDF. It implements the applier.MapStreamApplier interface.
type GRPCBasedMapStream struct {
	client mapstreamer.Client
}

func NewUDSgRPCBasedMapStream(client mapstreamer.Client) *GRPCBasedMapStream {
	return &GRPCBasedMapStream{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedMapStream) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map stream udf is healthy.
func (u *GRPCBasedMapStream) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map stream udf is connected.
func (u *GRPCBasedMapStream) WaitUntilReady(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				log.Infof("waiting for map stream udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (u *GRPCBasedMapStream) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	defer close(writeMessageCh)

	keys := message.Keys
	payload := message.Body.Payload
	offset := message.ReadOffset
	parentMessageInfo := message.MessageInfo

	var d = &mapstreampb.MapStreamRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(message.Watermark),
		Headers:   message.Headers,
	}

	responseCh := make(chan *mapstreampb.MapStreamResponse)
	errs, ctx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		err := u.client.MapStreamFn(ctx, d, responseCh)
		if err != nil {
			err = &ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapStreamFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
			return err
		}
		return nil
	})

	i := 0
	for response := range responseCh {
		result := response.Result
		i++
		keys := result.GetKeys()
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", offset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: result.GetValue(),
				},
			},
			Tags: result.GetTags(),
		}
		writeMessageCh <- *taggedMessage
	}

	return errs.Wait()
}

func (u *GRPCBasedMapStream) ApplyMapStreamBatch(ctx context.Context, messages []*isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	defer close(writeMessageCh)

	// errs := make([]error, len(messages))

	globalParentMessageInfo := messages[0].MessageInfo
	globalOffset := messages[0].ReadOffset

	requests := make([]*mapstreampb.MapStreamRequest, len(messages))
	log.Printf("MDW: Got %d messages -- %+v", len(messages), messages)
	for idx, msg := range messages {
		keys := msg.Keys
		payload := msg.Body.Payload
		// offset := msg.ReadOffset
		parentMessageInfo := msg.MessageInfo

		var d = &mapstreampb.MapStreamRequest{
			Keys:      keys,
			Value:     payload,
			EventTime: timestamppb.New(parentMessageInfo.EventTime),
			Watermark: timestamppb.New(msg.Watermark),
			Headers:   msg.Headers,
		}
		requests[idx] = d
	}

	// log.Printf("MDW: Length of requests = %d -- %+v", len(requests), requests)
	// responseCh := make(chan *mapstreampb.MapStreamResponseBatch)
	responseCh := make(chan *mapstreampb.MapStreamResponse)

	errs2, ctx2 := errgroup.WithContext(ctx)
	errs2.Go(func() error {
		// Ensure closes so read loop can have an end
		defer close(responseCh)
		// log.Printf("MDW: Call MapStreamBatchFn %s", requests)
		err := u.client.MapStreamBatchFn(ctx2, requests, responseCh)

		if err != nil {
			err = &ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ApplyMapStreamBatch failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
			return err
		}
		return nil
	})

	i := 0
	log.Printf("MDW: START listening to responseCh")
	for response := range responseCh {
		log.Printf("MDW: LOOP on responseCh")
		result := response.GetResult()
		// for _, result := range results {
		// result := result.Result
		i++
		keys := result.GetKeys()
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: globalParentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", globalOffset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: result.GetValue(),
				},
			},
			Tags: result.GetTags(),
		}
		log.Printf("MDW: taggedMessage %+v", taggedMessage)
		writeMessageCh <- *taggedMessage
		log.Printf("MDW: finished pushing to writeMessageCh")
		// }
	}
	log.Printf("MDW: END with listening to responseCh")
	return errs2.Wait()

	// --------
	// if err != nil {
	// 	for i := range requests {
	// 		errs[i] = &ApplyUDFErr{
	// 			UserUDFErr: false,
	// 			Message:    fmt.Sprintf("gRPC client.MapStreamBatchFn failed, %s", err),
	// 			InternalErr: InternalErr{
	// 				Flag:        true,
	// 				MainCarDown: false,
	// 			},
	// 		}
	// 	}
	// 	return errs
	// }
	// --------
	// TODO: Capture errors

	// Use ID to map the response messages, so that there's no strict requirement for the user-defined sink to return the response in order.
	// resMap := make(map[string]*mapstreampb.MapStreamResponseBatch_Result)
	// for _, res := range response.GetResults() {
	// 	resMap[res.GetId()] = res
	// }
	// for i, m := range requests {
	// 	if r, existing := resMap[m.GetId()]; !existing {
	// 		errs[i] = &NotFoundErr
	// 	} else {
	// 		if r.GetStatus() == sinkpb.Status_FAILURE {
	// 			if r.GetErrMsg() != "" {
	// 				errs[i] = &ApplyUDSinkErr{
	// 					UserUDSinkErr: true,
	// 					Message:       r.GetErrMsg(),
	// 				}
	// 			} else {
	// 				errs[i] = &UnknownUDSinkErr
	// 			}
	// 		} else if r.GetStatus() == sinkpb.Status_FALLBACK {
	// 			errs[i] = &WriteToFallbackErr
	// 		} else {
	// 			errs[i] = nil
	// 		}
	// 	}
	// }
	// return errs
}
