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

/*
Package forward does the Read (fromBufferPartition) -> Process (map UDF) -> Forward (toBuffers) -> Ack (fromBufferPartition) loop.
*/
package forward

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/flatmap/forward/applier"
	"github.com/numaproj/numaflow/pkg/flatmap/types"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// InterStepDataForward forwards the data from previous step to the current step via inter-step buffer.
type InterStepDataForward struct {
	// I have my reasons for overriding the default principle https://github.com/golang/go/issues/22602
	ctx context.Context
	// cancelFn cancels our new context, our cancellation is little more complex and needs to be well orchestrated, hence
	// we need something more than a cancel().
	cancelFn            context.CancelFunc
	fromBufferPartition isb.BufferReader
	// toBuffers is a map of toVertex name to the toVertex's owned buffers.
	toBuffers    map[string][]isb.BufferWriter
	FSD          forwarder.ToWhichStepDecider
	flatmapUDF   applier.MapApplier
	mapStreamUDF applier.MapStreamApplier
	wmFetcher    fetch.Fetcher
	// wmPublishers stores the vertex to publisher mapping
	wmPublishers  map[string]publish.Publisher
	opts          options
	vertexName    string
	pipelineName  string
	vertexReplica int32
	// idleManager manages the idle watermark status.
	idleManager wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid when the len(readMessage) is 0.
	wmbChecker     wmb.WMBChecker
	whereToDecider forwarder.ToWhichStepDecider
	Shutdown
}

// NewInterStepDataForward creates an inter-step forwarder.
func NewInterStepDataForward(
	vertexInstance *dfv1.VertexInstance,
	fromStep isb.BufferReader,
	toSteps map[string][]isb.BufferWriter,
	fsd forwarder.ToWhichStepDecider,
	applyUDF applier.MapApplier,
	applyUDFStream applier.MapStreamApplier,
	fetchWatermark fetch.Fetcher,
	publishWatermark map[string]publish.Publisher,
	idleManager wmb.IdleManager,
	whereToDecider forwarder.ToWhichStepDecider,
	opts ...Option) (*InterStepDataForward, error) {

	optsDef := DefaultOptions()
	for _, o := range opts {
		if err := o(optsDef); err != nil {
			return nil, err
		}
	}

	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	var isdf = InterStepDataForward{
		ctx:                 ctx,
		cancelFn:            cancel,
		fromBufferPartition: fromStep,
		toBuffers:           toSteps,
		FSD:                 fsd,
		flatmapUDF:          applyUDF,
		mapStreamUDF:        applyUDFStream,
		wmFetcher:           fetchWatermark,
		wmPublishers:        publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		idleManager:   idleManager,
		wmbChecker:    wmb.NewWMBChecker(2), // TODO: make configurable
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		whereToDecider: whereToDecider,
		opts:           *optsDef,
	}

	// Add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, optsDef.logger)

	//if isdf.opts.enableMapUdfStream && isdf.opts.readBatchSize != 1 {
	//	return nil, fmt.Errorf("batch size is not 1 with map UDF streaming")
	//}

	return &isdf, nil
}

// readData is the asynchronous reader which would constantly keep reading from the ISB and stream
// the messages on the input channel.
// Channel owned: inputMessages channel
// Error condition: If there is an error while reading, keep trying
// Shutdown flow: On receiving a done signal, we stop the reading process, close the inputMessages channel and return
func (isdf *InterStepDataForward) readData() (<-chan *isb.ReadMessage, chan struct{}) {
	log := isdf.opts.logger

	// inputMessages is the channel on which the data read from the ISB will be written and then
	// this channel will be consumed by the producer.
	inputMessages := make(chan *isb.ReadMessage, isdf.opts.readBatchSize)
	// stopChan is the done channel which is used to stop the processing for the reader goroutine
	stopChan := make(chan struct{})
	go func() {
		//log.Info("MYDEBUG: I'm in read function")
		defer close(inputMessages)
		defer close(stopChan)
		for {
			select {
			// TODO(stream) : should we check for a context done also here?
			//case <-isdf.ctx.Done():
			//	ok, err := isdf.IsShuttingDown()
			//	if err != nil {
			//		// ignore the error for now.
			//		log.Errorw("Failed to check if it can shutdown", zap.Error(err))
			//	}
			//	if ok {
			//		log.Info("Shutting down...")
			//		return
			//	}
			case <-stopChan:
				//log.Error("MYDEBUG: Stop reading from ISB")
				return
			default:
				ctx := isdf.ctx
				// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
				// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
				// responsibility of the Read function to do that.
				readMessages, err := isdf.fromBufferPartition.Read(ctx, isdf.opts.readBatchSize)
				isdf.opts.logger.Debugw("Read from buffer", zap.String("bufferFrom", isdf.fromBufferPartition.GetName()), zap.Int64("length", int64(len(readMessages))))
				if err != nil {
					isdf.opts.logger.Warnw("failed to read fromBufferPartition", zap.Error(err))
					metrics.ReadMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Inc()
				}
				// Keep streaming the messages read to the inputMessages channel
				for _, msg := range readMessages {
					log.Info("MYDEBUG: streaming in read function ", msg.ReadOffset.String(), " ", time.Now().UnixNano())
					inputMessages <- msg
				}
			}
		}
	}()
	return inputMessages, stopChan
}

func (isdf *InterStepDataForward) processUdf(inputMessageChan <-chan *isb.ReadMessage) <-chan *types.WriteMsgFlatmap {
	ctx := isdf.ctx
	// process only if we have any read messages. There is a natural looping here if there is an internal error while
	// reading, and we are not able to proceed.

	logger := isdf.opts.logger
	// TODO(stream) : enable idle watermark publishing

	//if len(readMessages) == 0 {
	//	// When the read length is zero, the write length is definitely zero too,
	//	// meaning there's no data to be published to the next vertex, and we consider this
	//	// situation as idling.
	//	// In order to continue propagating watermark, we will set watermark idle=true and publish it.
	//	// We also publish a control message if this is the first time we get this idle situation.
	//	// We compute the HeadIdleWMB using the given partition as the idle watermark
	//	var processorWMB = isdf.wmFetcher.ComputeHeadIdleWMB(isdf.fromBufferPartition.GetPartitionIdx())
	//	if !isdf.wmbChecker.ValidateHeadWMB(processorWMB) {
	//		// validation failed, skip publishing
	//		isdf.opts.logger.Debugw("skip publishing idle watermark",
	//			zap.Int("counter", isdf.wmbChecker.GetCounter()),
	//			zap.Int64("offset", processorWMB.Offset),
	//			zap.Int64("watermark", processorWMB.Watermark),
	//			zap.Bool("idle", processorWMB.Idle))
	//		return
	//	}
	//
	//	// if the validation passed, we will publish the watermark to all the toBuffer partitions.
	//	for toVertexName, toVertexBuffer := range isdf.toBuffers {
	//		for _, partition := range toVertexBuffer {
	//			if p, ok := isdf.wmPublishers[toVertexName]; ok {
	//				idlehandler.PublishIdleWatermark(ctx, isdf.fromBufferPartition.GetPartitionIdx(), partition, p, isdf.idleManager, isdf.opts.logger, isdf.vertexName, isdf.pipelineName, dfv1.VertexTypeMapUDF, isdf.vertexReplica, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
	//			}
	//		}
	//	}
	//	return
	//}
	//logger.Info("MYDEBUG: I'm processing here")

	// Send thr requests to the grpc server for results, which are received in the udfRespChan
	udfRespChan, err := isdf.flatmapUDF.ApplyMap(ctx, inputMessageChan)
	// TODO(stream): check error handling
	if err != nil {

	}
	writeChan := make(chan *types.WriteMsgFlatmap)

	// create a channel which would be passed to the next buffers for writing,
	// errors in messages/no-acks will be propagated from this as well

	go func() {
		defer close(writeChan)
		for msg := range udfRespChan {
			//logger.Info("MYDEBUG: Let's send to resp Chan here")
			select {
			// TODO(stream): add error handling and shutdown here
			default:
				d := isdf.processWriteMessage(msg, true)
				logger.Info("MYDEBUG: Sending to write ", msg.ParentMessage.ReadOffset.String(), " ", time.Now().UnixNano())
				//logger.Info("MYDEBUG: Sending to write", string(msg.RespMessage.Payload), "Sending to write", msg.Uid)
				writeChan <- d
			}
		}
	}()
	return writeChan
}

// Start starts reading the buffer and forwards to the next buffers. Call `Stop` to stop.
func (isdf *InterStepDataForward) Start() <-chan struct{} {
	log := logging.FromContext(isdf.ctx)
	stopped := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Info("Starting forwarder...")
		// with wg approach can do more cleanup in case we need in the future.
		defer wg.Done()
		for {
			select {
			// TODO(stream): check shutdown path
			case <-isdf.ctx.Done():
				ok, err := isdf.IsShuttingDown()
				if err != nil {
					// ignore the error for now.
					log.Errorw("Failed to check if it can shutdown", zap.Error(err))
				}
				if ok {
					log.Info("Shutting down...")
					return
				}
			default:
				// once context.Done() is called, we still have to try to forwardAChunk because in graceful
				// shutdown the fromBufferPartition should be empty.
			}
			// keep doing what you are good at
			isdf.forwardAChunk(isdf.ctx)
		}
	}()

	go func() {
		wg.Wait()
		// Clean up resources for buffer reader and all the writers if any.
		if err := isdf.fromBufferPartition.Close(); err != nil {
			log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed buffer reader", zap.String("bufferFrom", isdf.fromBufferPartition.GetName()))
		}
		for _, buffer := range isdf.toBuffers {
			for _, partition := range buffer {
				if err := partition.Close(); err != nil {
					log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", partition.GetName()))
				} else {
					log.Infow("Closed partition writer", zap.String("bufferTo", partition.GetName()))
				}
			}
		}

		close(stopped)
	}()

	return stopped
}

// readWriteMessagePair represents a read message and its processed (via map UDF) write messages.
type readWriteMessagePair struct {
	readMessage   *isb.ReadMessage
	writeMessages []*isb.WriteMessage
	udfError      error
}

func (isdf *InterStepDataForward) writeRoutine(ctx context.Context, writeMessageCh <-chan *types.WriteMsgFlatmap, ackChan chan<- *types.AckMsgFlatmap) {
	flushTimer := time.NewTicker(isdf.opts.flushDuration)
	writeMessages := make([]*isb.WriteMessage, 0, isdf.opts.batchSize)
	readOffsets := make([]*isb.ReadMessage, 0, isdf.opts.batchSize)
	logger := isdf.opts.logger

	// should we flush?
	var flush bool
	//logger.Info("MYDEBUG: I'm writing to buffer here")

	// error != nil only when the context is closed, so we can safely return (our write loop will try indefinitely
	// unless ctx.Done() happens)
	flush = true
forwardLoop:
	for {
		select {
		case response, ok := <-writeMessageCh:
			if !ok {
				// TODO(stream): check the error logic here
				break forwardLoop
			}
			if !response.AckIt {
				// TODO(stream): if no ack directly set let's short circuit?
			}

			// append the write message to the array
			writeMessages = append(writeMessages, response.Message.RespMessage)
			readOffsets = append(readOffsets, response.Message.ParentMessage)

			// if the batch size is reached, let's flush
			if len(writeMessages) >= isdf.opts.batchSize {
				flush = true
			}

		case <-flushTimer.C:
			// if there are no messages to write, continue
			if len(writeMessages) == 0 {
				continue
			}

			// Since flushTimer is triggered, it is time to flush
			flush = true
		}

		if flush {
			if err := isdf.forwardToBuffers(ctx, &writeMessages); err != nil {
				// TODO(stream): mark as no ack directly or retry
			}

			for _, offset := range readOffsets {
				logger.Info("MYDEBUG: Sending to ack ", offset.ReadOffset.String(), " ", time.Now().UnixNano())
				//logger.Info("MYDEBUG: STEP 1 at ACKING ", offset.ReadOffset)
				ackChan <- &types.AckMsgFlatmap{
					Message: offset,
					// TODO(stream): should we write for no-ack?
					AckIt: true,
				}
			}
			// clear the writeMessages
			writeMessages = make([]*isb.WriteMessage, 0, isdf.opts.batchSize)
			readOffsets = make([]*isb.ReadMessage, 0, isdf.opts.batchSize)
			//flush = false
		}
	}

	// if there are any messages left, forward them to the ISB
	if len(writeMessages) > 0 {
		if err := isdf.forwardToBuffers(ctx, &writeMessages); err != nil {
			// TODO(stream): mark as no ack directly or retry
		}
		for _, offset := range readOffsets {
			//logger.Info("MYDEBUG: STEP 1 at ACKING ", offset.ReadOffset)
			ackChan <- &types.AckMsgFlatmap{
				Message: offset,
				// TODO(stream): should we write for no-ack?
				AckIt: true,
			}
		}
		// clear the writeMessages
		writeMessages = make([]*isb.WriteMessage, 0, isdf.opts.batchSize)
		readOffsets = make([]*isb.ReadMessage, 0, isdf.opts.batchSize)
		//flush = false
	}
}

func (isdf *InterStepDataForward) writeAhead(writeMessageCh <-chan *types.WriteMsgFlatmap) <-chan *types.AckMsgFlatmap {
	// TODO(stream): check buffered channel size
	//logger := isdf.opts.logger
	ackChan := make(chan *types.AckMsgFlatmap)
	ctx := isdf.ctx

	go func() {
		defer close(ackChan)
		group := sync.WaitGroup{}
		for i := 0; i < isdf.opts.batchSize; i++ {
			group.Add(1)
			go isdf.writeRoutine(ctx, writeMessageCh, ackChan)
		}
		group.Wait()
	}()
	return ackChan

	//go func() {
	//	defer close(ackChan)
	//	flushTimer := time.NewTicker(isdf.opts.flushDuration)
	//	writeMessages := make([]*isb.WriteMessage, 0, isdf.opts.batchSize)
	//	readOffsets := make([]*isb.ReadMessage, 0, isdf.opts.batchSize)
	//
	//	// should we flush?
	//	var flush bool
	//	//logger.Info("MYDEBUG: I'm writing to buffer here")
	//
	//	// error != nil only when the context is closed, so we can safely return (our write loop will try indefinitely
	//	// unless ctx.Done() happens)
	//	flush = true
	//forwardLoop:
	//	for {
	//		select {
	//		case response, ok := <-writeMessageCh:
	//			if !ok {
	//				// TODO(stream): check the error logic here
	//				break forwardLoop
	//			}
	//			if !response.AckIt {
	//				// TODO(stream): if no ack directly set let's short circuit?
	//			}
	//
	//			// append the write message to the array
	//			writeMessages = append(writeMessages, response.Message.RespMessage)
	//			readOffsets = append(readOffsets, response.Message.ParentMessage)
	//
	//			// if the batch size is reached, let's flush
	//			if len(writeMessages) >= isdf.opts.batchSize {
	//				flush = true
	//			}
	//
	//		case <-flushTimer.C:
	//			// if there are no messages to write, continue
	//			if len(writeMessages) == 0 {
	//				continue
	//			}
	//
	//			// Since flushTimer is triggered, it is time to flush
	//			flush = true
	//		}
	//
	//		if flush {
	//			if err := isdf.forwardToBuffers(ctx, &writeMessages); err != nil {
	//				// TODO(stream): mark as no ack directly or retry
	//			}
	//
	//			for _, offset := range readOffsets {
	//				logger.Info("MYDEBUG: Sending to ack ", offset.ReadOffset.String(), " ", time.Now().UnixNano())
	//				//logger.Info("MYDEBUG: STEP 1 at ACKING ", offset.ReadOffset)
	//				ackChan <- &types.AckMsgFlatmap{
	//					Message: offset,
	//					// TODO(stream): should we write for no-ack?
	//					AckIt: true,
	//				}
	//			}
	//			// clear the writeMessages
	//			writeMessages = make([]*isb.WriteMessage, 0, isdf.opts.batchSize)
	//			readOffsets = make([]*isb.ReadMessage, 0, isdf.opts.batchSize)
	//			//flush = false
	//		}
	//	}
	//
	//	// if there are any messages left, forward them to the ISB
	//	if len(writeMessages) > 0 {
	//		if err := isdf.forwardToBuffers(ctx, &writeMessages); err != nil {
	//			// TODO(stream): mark as no ack directly or retry
	//		}
	//		for _, offset := range readOffsets {
	//			//logger.Info("MYDEBUG: STEP 1 at ACKING ", offset.ReadOffset)
	//			ackChan <- &types.AckMsgFlatmap{
	//				Message: offset,
	//				// TODO(stream): should we write for no-ack?
	//				AckIt: true,
	//			}
	//		}
	//		// clear the writeMessages
	//		writeMessages = make([]*isb.WriteMessage, 0, isdf.opts.batchSize)
	//		readOffsets = make([]*isb.ReadMessage, 0, isdf.opts.batchSize)
	//		//flush = false
	//	}
	//}()
	//go func() {
	//	logger.Info("MYDEBUG: I'm writing to buffer here")
	//	defer close(ackChan)
	//	ctx := isdf.ctx
	//	// TODO: check pnf.forwardResponses
	//	// create space for writeMessages specific to each step as we could forward to all the steps too.
	//	// these messages are for per partition (due to round-robin writes) for load balancing
	//	var messageToStep = make(map[string][][]isb.Message)
	//	var writeOffsets = make(map[string][][]isb.Offset)
	//
	//	for toVertex := range isdf.toBuffers {
	//		// over allocating to have a predictable pattern
	//		messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
	//		writeOffsets[toVertex] = make([][]isb.Offset, len(isdf.toBuffers[toVertex]))
	//	}
	//	// Stream the message to the next vertex. First figure out which vertex
	//	// to send the result to. Then update the toBuffer(s) with writeMessage.
	//	msgIndex := 0
	//	for msg := range writeMessageCh {
	//		logger.Info("MYDEBUG: Got a msg on writeMessageCh ", msg.Message.Uid)
	//		if !msg.AckIt {
	//			// TODO(stream): if no ack directly set let's short circuit?
	//		}
	//		parentMessage := msg.Message.ParentMessage
	//		writeMessage := msg.Message.RespMessage
	//
	//		writeMessage.Headers = parentMessage.Headers
	//		// add vertex name to the ID, since multiple vertices can publish to the same vertex and we need uniqueness across them
	//		// TODO(stream): check if this ID is correct for DEDUP?
	//		logger.Info("MYDEBUG: DEDUP ID ", writeMessage.ID)
	//		//writeMessage.ID = fmt.Sprintf("%s-%s-%d", parentMessage.ReadOffset.String(), isdf.vertexName, msgIndex)
	//		msgIndex += 1
	//		metrics.UDFWriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(1))
	//
	//		// update toBuffers
	//		if err := isdf.whereToStep(writeMessage, messageToStep, parentMessage); err != nil {
	//			// TODO(stream): mark as no ack directly or retry
	//			//return nil, fmt.Errorf("failed at whereToStep, error: %w", err)
	//		}
	//		logger.Info("MYDEBUG: STEP 2 at ACKING ", msg.Message.Uid)
	//
	//		// Forward the message to the edge buffer (could be multiple edges)
	//		curWriteOffsets, err := isdf.writeToBuffers(ctx, messageToStep)
	//		if err != nil {
	//			// TODO(stream): mark as no ack directly or retry
	//			//return nil, fmt.Errorf("failed to write to toBuffers, error: %w", err)
	//		}
	//		logger.Info("MYDEBUG: STEP 3 at ACKING ", msg.Message.Uid)
	//		// Merge curWriteOffsets into writeOffsets
	//		for vertexName, toVertexBufferOffsets := range curWriteOffsets {
	//			for index, offsets := range toVertexBufferOffsets {
	//				writeOffsets[vertexName][index] = append(writeOffsets[vertexName][index], offsets...)
	//			}
	//		}
	//		logger.Info("MYDEBUG: STEP 4 at ACKING ", len(curWriteOffsets))
	//
	//		// TODO(stream): Message written successfully -> send for ack
	//		ackChan <- &types.AckMsgFlatmap{
	//			Message: parentMessage,
	//			AckIt:   msg.AckIt,
	//		}
	//		logger.Info("MYDEBUG: WRITE sent a msg for ACK ", parentMessage.ReadOffset)
	//		// TODO(stream): publish new watermark
	//	}
	//}()
	//return ackChan

}

func (isdf *InterStepDataForward) ackRoutine(ctx context.Context, ackMsgChan <-chan *types.AckMsgFlatmap) {
	logger := isdf.opts.logger
forwardLoop:
	for {
		select {
		case response, ok := <-ackMsgChan:
			if !ok {
				break forwardLoop
			}
			if response.AckIt {
				ackMessages := []isb.Offset{response.Message.ReadOffset}
				if err := isdf.ackFromBuffer(ctx, ackMessages); err != nil {
					// TODO(stream): we have retried in the ackFromBuffer, should we trigger
					// shutdown here then?
				}
				logger.Info("MYDEBUG: Done with ack ", ackMessages, " ", time.Now().UnixNano())
			} else {
				noAckMessages := []isb.Offset{response.Message.ReadOffset}
				isdf.noAckMessages(ctx, noAckMessages)
			}
		}

	}

}

//func (isdf *InterStepDataForward) ackPrevBuffer(ackMsgChan <-chan *types.AckMsgFlatmap) {
//	ctx := isdf.ctx
//	flushTimer := time.NewTicker(isdf.opts.flushDuration)
//	ackMessages := make([]isb.Offset, 0, isdf.opts.batchSize)
//	noAckMessages := make([]isb.Offset, 0, isdf.opts.batchSize)
//	logger := isdf.opts.logger
//
//	// should we flush?
//	var flushAck bool
//
//	// should we flush?
//	var flushNoAck bool
//
//	flushAck = true
//	flushNoAck = true
//	// error != nil only when the context is closed, so we can safely return (our write loop will try indefinitely
//	// unless ctx.Done() happens)
//forwardLoop:
//	for {
//		select {
//		case response, ok := <-ackMsgChan:
//			if !ok {
//				break forwardLoop
//			}
//
//			if response.AckIt {
//				// append the ack message to the array
//				ackMessages = append(ackMessages, response.Message.ReadOffset)
//			} else {
//				// append the ack message to the array
//				noAckMessages = append(noAckMessages, response.Message.ReadOffset)
//
//			}
//
//			// if the batch size is reached, let's flush
//			if len(ackMessages) >= isdf.opts.batchSize {
//				flushAck = true
//			}
//
//			// if the batch size is reached, let's flush
//			if len(noAckMessages) >= isdf.opts.batchSize {
//				flushNoAck = true
//			}
//
//		case <-flushTimer.C:
//			// if there are no messages to write, continue
//			if len(ackMessages) == 0 {
//				continue
//			}
//
//			// Since flushTimer is triggered, it is time to flush
//			flushAck = true
//			flushNoAck = true
//		}
//
//		if flushAck {
//			if err := isdf.ackFromBuffer(ctx, ackMessages); err != nil {
//				// TODO(stream): we have retried in the ackFromBuffer, should we trigger
//				// shutdown here then?
//			}
//			logger.Info("MYDEBUG: Sending to ack ", ackMessages, " ", time.Now().UnixNano())
//			ackMessages = make([]isb.Offset, 0, isdf.opts.batchSize)
//			//flushAck = false
//		}
//
//		if flushNoAck {
//			isdf.noAckMessages(ctx, noAckMessages)
//			noAckMessages = make([]isb.Offset, 0, isdf.opts.batchSize)
//			//flushNoAck = false
//		}
//	}
//
//	// if there are any messages left, forward them to the ISB
//	if len(ackMessages) > 0 {
//		if err := isdf.ackFromBuffer(ctx, ackMessages); err != nil {
//			// TODO(stream): we have retried in the ackFromBuffer, should we trigger
//			// shutdown here then?
//			//return
//		}
//		ackMessages = make([]isb.Offset, 0, isdf.opts.batchSize)
//	}
//
//	// if there are any messages left, forward them to the ISB
//	if len(noAckMessages) > 0 {
//		isdf.noAckMessages(ctx, noAckMessages)
//		noAckMessages = make([]isb.Offset, 0, isdf.opts.batchSize)
//	}

//}

// forwardAChunk forwards a chunk of message from the fromBufferPartition to the toBuffers. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but does not include errors due to user code UDFs, WhereTo, etc.
func (isdf *InterStepDataForward) forwardAChunk(ctx context.Context) {
	//start := time.Now()

	// TODO(stream): check buffered channel size for each
	// reading data
	inputMessagesChan, doneReadChan := isdf.readData()

	// processing data
	writeMsgChan := isdf.processUdf(inputMessagesChan)

	// writing data to next ISB buffer
	ackMsgChan := isdf.writeAhead(writeMsgChan)

	// Ack to previous ISB
	go func() {
		group := sync.WaitGroup{}
		for i := 0; i < isdf.opts.batchSize; i++ {
			group.Add(1)
			go isdf.ackRoutine(isdf.ctx, ackMsgChan)
		}
		group.Wait()
	}()

	// TODO(stream): check ideal way to wait here
	<-doneReadChan
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (isdf *InterStepDataForward) whereToStep(writeMessage *isb.WriteMessage, messageToStep map[string][][]isb.Message, readMessage *isb.ReadMessage) error {
	// call WhereTo and drop it on errors
	to, err := isdf.FSD.WhereTo(writeMessage.Keys, writeMessage.Tags, writeMessage.ID)
	if err != nil {
		isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("WhereTo failed, %s", err)}))
		// a shutdown can break the blocking loop caused due to InternalErr
		if ok, _ := isdf.IsShuttingDown(); ok {
			err := fmt.Errorf("whereToStep, Stop called while stuck on an internal error, %v", err)
			metrics.PlatformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica))}).Inc()
			return err
		}
		return err
	}

	for _, t := range to {
		if _, ok := messageToStep[t.ToVertexName]; !ok {
			isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("no such destination (%s)", t.ToVertexName)}))
		}
		messageToStep[t.ToVertexName][t.ToVertexPartitionIdx] = append(messageToStep[t.ToVertexName][t.ToVertexPartitionIdx], writeMessage.Message)
	}
	return nil
}

// writeToBuffers is a blocking call until all the messages have be forwarded to all the toBuffers, or a shutdown
// has been initiated while we are stuck looping on an InternalError.
func (isdf *InterStepDataForward) writeToBuffers(
	ctx context.Context, messageToStep map[string][][]isb.Message,
) (writeOffsets map[string][][]isb.Offset, err error) {
	// messageToStep contains all the to buffers, so the messages could be empty (conditional forwarding).
	// So writeOffsets also contains all the to buffers, but the returned offsets might be empty.
	writeOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexMessages := range messageToStep {
		writeOffsets[toVertexName] = make([][]isb.Offset, len(toVertexMessages))
	}
	for toVertexName, toVertexBuffer := range isdf.toBuffers {
		for index, partition := range toVertexBuffer {
			writeOffsets[toVertexName][index], err = isdf.writeToBuffer(ctx, partition, messageToStep[toVertexName][index])
			if err != nil {
				return nil, err
			}
		}
	}
	return writeOffsets, nil
}

// writeToBuffer forwards an array of messages to a single buffer and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
	var (
		totalCount int
		writeCount int
		writeBytes float64
	)
	totalCount = len(messages)
	writeOffsets = make([]isb.Offset, 0, totalCount)

	for {
		_writeOffsets, errs := toBufferPartition.Write(ctx, messages)
		// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation since using failedMessages is an unlikely path.
		var failedMessages []isb.Message
		needRetry := false
		for idx, msg := range messages {
			if err = errs[idx]; err != nil {
				// ATM there are no user-defined errors during write, all are InternalErrors.
				// Non retryable error, drop the message. Non retryable errors are only returned
				// when the buffer is full and the user has set the buffer full strategy to
				// DiscardLatest or when the message is duplicate.
				if errors.As(err, &isb.NonRetryableBufferWriteErr{}) {
					metrics.DropMessagesCount.With(map[string]string{
						metrics.LabelVertex:             isdf.vertexName,
						metrics.LabelPipeline:           isdf.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
						metrics.LabelPartitionName:      toBufferPartition.GetName(),
						metrics.LabelReason:             err.Error(),
					}).Inc()

					metrics.DropBytesCount.With(map[string]string{
						metrics.LabelVertex:             isdf.vertexName,
						metrics.LabelPipeline:           isdf.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
						metrics.LabelPartitionName:      toBufferPartition.GetName(),
						metrics.LabelReason:             err.Error(),
					}).Add(float64(len(msg.Payload)))

					isdf.opts.logger.Infow("Dropped message", zap.String("reason", err.Error()), zap.String("partition", toBufferPartition.GetName()), zap.String("vertex", isdf.vertexName), zap.String("pipeline", isdf.pipelineName))
				} else {
					needRetry = true
					// we retry only failed messages
					failedMessages = append(failedMessages, msg)
					metrics.WriteMessagesError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: toBufferPartition.GetName()}).Inc()
					// a shutdown can break the blocking loop caused due to InternalErr
					if ok, _ := isdf.IsShuttingDown(); ok {
						metrics.PlatformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica))}).Inc()
						return writeOffsets, fmt.Errorf("writeToBuffer failed, Stop called while stuck on an internal error with failed messages:%d, %v", len(failedMessages), errs)
					}
				}
			} else {
				writeCount++
				writeBytes += float64(len(msg.Payload))
				// we support write offsets only for jetstream
				if _writeOffsets != nil {
					writeOffsets = append(writeOffsets, _writeOffsets[idx])
				}
			}
		}

		if needRetry {
			isdf.opts.logger.Errorw("Retrying failed messages",
				zap.Any("errors", errorArrayToMap(errs)),
				zap.String(metrics.LabelPipeline, isdf.pipelineName),
				zap.String(metrics.LabelVertex, isdf.vertexName),
				zap.String(metrics.LabelPartitionName, toBufferPartition.GetName()),
			)
			// set messages to failed for the retry
			messages = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
		} else {
			break
		}
	}

	metrics.WriteMessagesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(float64(writeCount))
	metrics.WriteBytesCount.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: toBufferPartition.GetName()}).Add(writeBytes)
	return writeOffsets, nil
}

// errorArrayToMap summarizes an error array to map
func errorArrayToMap(errs []error) map[string]int64 {
	result := make(map[string]int64)
	for _, err := range errs {
		if err != nil {
			result[err.Error()]++
		}
	}
	return result
}

// ackFromBuffer acknowledges an array of offsets back to fromBufferPartition and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) ackFromBuffer(ctx context.Context, offsets []isb.Offset) error {
	var ackRetryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0.1,
		Steps:    math.MaxInt,
		Duration: time.Millisecond * 10,
	}
	var ackOffsets = offsets
	attempt := 0

	ctxClosedErr := wait.ExponentialBackoff(ackRetryBackOff, func() (done bool, err error) {
		errs := isdf.fromBufferPartition.Ack(ctx, ackOffsets)
		attempt += 1
		summarizedErr := errorArrayToMap(errs)
		var failedOffsets []isb.Offset
		if len(summarizedErr) > 0 {
			isdf.opts.logger.Errorw("Failed to ack from buffer, retrying", zap.Any("errors", summarizedErr), zap.Int("attempt", attempt))
			// no point retrying if ctx.Done has been invoked
			select {
			case <-ctx.Done():
				// no point in retrying after we have been asked to stop.
				return false, ctx.Err()
			default:
				// retry only the failed offsets
				for i, offset := range ackOffsets {
					if errs[i] != nil {
						failedOffsets = append(failedOffsets, offset)
					}
				}
				ackOffsets = failedOffsets
				if ok, _ := isdf.IsShuttingDown(); ok {
					ackErr := fmt.Errorf("AckFromBuffer, Stop called while stuck on an internal error, %v", summarizedErr)
					return false, ackErr
				}
				return false, nil
			}
		} else {
			return true, nil
		}
	})

	if ctxClosedErr != nil {
		isdf.opts.logger.Errorw("Context closed while waiting to ack messages inside forward", zap.Error(ctxClosedErr))
	}

	return ctxClosedErr
}

// noAckMessages no-acks all the read offsets of failed messages.
func (isdf *InterStepDataForward) noAckMessages(ctx context.Context, failedMessages []isb.Offset) {
	isdf.fromBufferPartition.NoAck(ctx, failedMessages)
}

func (isdf *InterStepDataForward) processWriteMessage(msg *types.ResponseFlatmap, ackIt bool) *types.WriteMsgFlatmap {
	return &types.WriteMsgFlatmap{
		Message: msg,
		AckIt:   ackIt,
	}
}

// forwardToBuffers writes the messages to the ISBs concurrently for each partition.
func (isdf *InterStepDataForward) forwardToBuffers(ctx context.Context, writeMessages *[]*isb.WriteMessage) error {
	if len(*writeMessages) == 0 {
		return nil
	}
	messagesToStep := isdf.whereToStepNew(*writeMessages)
	// parallel writes to each ISB
	var mu sync.Mutex
	// use error group
	var eg errgroup.Group
	for key, values := range messagesToStep {
		for index, messages := range values {
			if len(messages) == 0 {
				continue
			}

			func(toVertexName string, toVertexPartitionIdx int32, resultMessages []isb.Message) {
				eg.Go(func() error {
					_, err := isdf.writeToBufferNew(ctx, toVertexName, toVertexPartitionIdx, resultMessages)
					if err != nil {
						return err
					}
					mu.Lock()
					// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
					//isdf.latestWriteOffsets[toVertexName][toVertexPartitionIdx] = offsets
					mu.Unlock()
					return nil
				})
			}(key, int32(index), messages)
		}
	}

	// wait until all the writer go routines return
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

// whereToStep assigns a message to the ISBs based on the Message.Keys.
func (isdf *InterStepDataForward) whereToStepNew(writeMessages []*isb.WriteMessage) map[string][][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][][]isb.Message)

	var to []forwarder.VertexBuffer
	var err error
	for _, msg := range writeMessages {
		to, err = isdf.whereToDecider.WhereTo(msg.Keys, msg.Tags, msg.ID)
		if err != nil {
			metrics.PlatformError.With(map[string]string{
				metrics.LabelVertex:             isdf.vertexName,
				metrics.LabelPipeline:           isdf.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
			}).Inc()
			isdf.opts.logger.Errorw("Got an error while invoking WhereTo, dropping the message", zap.Strings("keys", msg.Keys), zap.Error(err))
			continue
		}

		if len(to) == 0 {
			continue
		}

		for _, step := range to {
			if _, ok := messagesToStep[step.ToVertexName]; !ok {
				messagesToStep[step.ToVertexName] = make([][]isb.Message, len(isdf.toBuffers[step.ToVertexName]))
			}
			messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx] = append(messagesToStep[step.ToVertexName][step.ToVertexPartitionIdx], msg.Message)
		}

	}
	return messagesToStep
}

// writeToBuffer writes to the ISBs.
func (isdf *InterStepDataForward) writeToBufferNew(ctx context.Context, edgeName string, partition int32, resultMessages []isb.Message) ([]isb.Offset, error) {
	var (
		writeCount int
		writeBytes float64
	)

	var ISBWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	writeMessages := resultMessages

	// write to isb with infinite exponential backoff (until shutdown is triggered)
	var offsets []isb.Offset
	ctxClosedErr := wait.ExponentialBackoff(ISBWriteBackoff, func() (done bool, err error) {
		var writeErrs []error
		var failedMessages []isb.Message
		offsets, writeErrs = isdf.toBuffers[edgeName][partition].Write(ctx, writeMessages)
		for i, message := range writeMessages {
			writeErr := writeErrs[i]
			if writeErr != nil {
				// Non retryable error, drop the message. Non retryable errors are only returned
				// when the buffer is full and the user has set the buffer full strategy to
				// DiscardLatest or when the message is duplicate.
				if errors.As(writeErr, &isb.NonRetryableBufferWriteErr{}) {
					metrics.DropMessagesCount.With(map[string]string{
						metrics.LabelVertex:             isdf.vertexName,
						metrics.LabelPipeline:           isdf.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
						metrics.LabelPartitionName:      isdf.toBuffers[edgeName][partition].GetName(),
						metrics.LabelReason:             writeErr.Error(),
					}).Inc()

					metrics.DropBytesCount.With(map[string]string{
						metrics.LabelVertex:             isdf.vertexName,
						metrics.LabelPipeline:           isdf.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
						metrics.LabelPartitionName:      isdf.toBuffers[edgeName][partition].GetName(),
						metrics.LabelReason:             writeErr.Error(),
					}).Add(float64(len(message.Payload)))

					isdf.opts.logger.Infow("Dropped message", zap.String("reason", writeErr.Error()), zap.String("vertex", isdf.vertexName), zap.String("pipeline", isdf.pipelineName))
				} else {
					failedMessages = append(failedMessages, message)
				}
			} else {
				writeCount++
				writeBytes += float64(len(message.Payload))
			}
		}
		// retry only the failed messages
		if len(failedMessages) > 0 {
			isdf.opts.logger.Warnw("Failed to write messages to isb inside pnf", zap.Errors("errors", writeErrs))
			writeMessages = failedMessages
			metrics.WriteMessagesError.With(map[string]string{
				metrics.LabelVertex:             isdf.vertexName,
				metrics.LabelPipeline:           isdf.pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
				metrics.LabelPartitionName:      isdf.toBuffers[edgeName][partition].GetName()}).Add(float64(len(failedMessages)))

			if ctx.Err() != nil {
				// no need to retry if the context is closed
				return false, ctx.Err()
			}
			// keep retrying...
			return false, nil
		}
		return true, nil
	})

	if ctxClosedErr != nil {
		isdf.opts.logger.Errorw("Ctx closed while writing messages to ISB", zap.Error(ctxClosedErr))
		return nil, ctxClosedErr
	}

	metrics.WriteMessagesCount.With(map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
		metrics.LabelPartitionName:      isdf.toBuffers[edgeName][partition].GetName()}).Add(float64(writeCount))

	metrics.WriteBytesCount.With(map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
		metrics.LabelPartitionName:      isdf.toBuffers[edgeName][partition].GetName()}).Add(writeBytes)
	return offsets, nil
}
