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

package mapbatch

import (
	"context"
	"log"
	"time"

	mapbpb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapbatch/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt mapbpb.MapBatchClient
}

// New creates a new client object.
func New(inputOptions ...Option) (Client, error) {
	var opts = &options{
		maxMessageSize:             sdkclient.DefaultGRPCMaxMessageSize, // 64 MB
		serverInfoFilePath:         sdkclient.ServerInfoFilePath,
		tcpSockAddr:                sdkclient.TcpAddr,
		udsSockAddr:                sdkclient.MapAddr,
		serverInfoReadinessTimeout: 120 * time.Second, // Default timeout is 120 seconds
	}

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	// Wait for server info to be ready
	serverInfo, err := util.WaitForServerInfo(opts.serverInfoReadinessTimeout, opts.serverInfoFilePath)
	if err != nil {
		return nil, err
	}

	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}

	// Connect to the server
	conn, err := util.ConnectToServer(opts.udsSockAddr, opts.tcpSockAddr, serverInfo, opts.maxMessageSize)
	if err != nil {
		return nil, err
	}

	c := new(client)
	c.conn = conn
	c.grpcClt = mapbpb.NewMapBatchClient(conn)
	return c, nil
}

// NewFromClient creates a new client object from a grpc client. This is used for testing.
func NewFromClient(c mapbpb.MapBatchClient) (Client, error) {
	return &client{
		grpcClt: c,
	}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// IsReady returns true if the grpc connection is ready to use.
func (c *client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

// MapFn applies a function to each datum element.
func (c *client) MapBatchFn(ctx context.Context, request *mapbpb.MapBatchRequest) (*mapbpb.MapBatchResponse, error) {
	mapResponse, err := c.grpcClt.MapBatchFn(ctx, request)
	err = util.ToUDFErr("c.grpcClt.MapBatchFn", err)
	if err != nil {
		return nil, err
	}
	return mapResponse, nil
}