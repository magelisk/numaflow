package flatmapper

import (
	"errors"
	"io"

	flatmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/flatmap/v1"
	"github.com/numaproj/numaflow-go/pkg/info"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt flatmappb.FlatmapClient
}

func (c client) CloseConn(ctx context.Context) error {
	return c.conn.Close()
}

func (c client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}
func (c client) MapFn(ctx context.Context, datumStreamCh <-chan *flatmappb.MapRequest) (<-chan *flatmappb.MapResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *flatmappb.MapResponse)
	)

	// stream the messages to server
	stream, err := c.grpcClt.MapFn(ctx)

	if err != nil {
		go func(sErr error) {
			errCh <- sdkerr.ToUDFErr("c.grpcClt.MapFn", sErr)
		}(err)
	}

	// read from the datumStreamCh channel and send it to the server stream
	go func() {
		var sendErr error
	outerLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case datum, ok := <-datumStreamCh:
				if !ok {
					break outerLoop
				}
				// TODO: figure out why send is getting EOF (could be because the client has already handled SIGTERM)
				if sendErr = stream.Send(datum); sendErr != nil && !errors.Is(sendErr, io.EOF) {
					errCh <- sdkerr.ToUDFErr("MapFn stream.Send()", sendErr)
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
			sendErr = stream.CloseSend()
			if sendErr != nil && !errors.Is(sendErr, io.EOF) {
				errCh <- sdkerr.ToUDFErr("MapFn stream.CloseSend()", sendErr)
			}
		}
	}()

	// read the response from the server stream and send it to responseCh channel
	// any error is sent to errCh channel
	go func() {
		var resp *flatmappb.MapResponse
		var recvErr error
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				resp, recvErr = stream.Recv()
				// if the stream is closed, close the responseCh return
				if errors.Is(recvErr, io.EOF) {
					// nil channel will never be selected
					errCh = nil
					close(responseCh)
					return
				}
				if recvErr != nil {
					errCh <- sdkerr.ToUDFErr("MapFn stream.Recv()", recvErr)
				}
				responseCh <- resp
			}
		}
	}()

	return responseCh, errCh
}

// New creates a new client object.
func New(serverInfo *info.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.FlatmapAddr)

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	// Connect to the server
	conn, err := grpcutil.ConnectToServer(opts.UdsSockAddr(), serverInfo, opts.MaxMessageSize())
	if err != nil {
		return nil, err
	}

	c := new(client)
	c.conn = conn
	c.grpcClt = flatmappb.NewFlatmapClient(conn)
	return c, nil
}
