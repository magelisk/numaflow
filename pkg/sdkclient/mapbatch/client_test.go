package mapbatch

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mapbpb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapbatch/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/mapbatch/v1/mapbatchmock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapbatchmock.NewMockMapBatchClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mapbpb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&mapbpb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	ready, err := testClient.IsReady(ctx, &emptypb.Empty{})
	assert.True(t, ready)
	assert.NoError(t, err)

	ready, err = testClient.IsReady(ctx, &emptypb.Empty{})
	assert.False(t, ready)
	assert.EqualError(t, err, "mock connection refused")
}

func TestClient_MapFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mapbatchmock.NewMockMapBatchClient(ctrl)
	mockClient.EXPECT().MapBatchFn(gomock.Any(), gomock.Any()).Return(&mapbpb.MapBatchResponse{Results: []*mapbpb.MapBatchResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, nil)
	mockClient.EXPECT().MapFn(gomock.Any(), gomock.Any()).Return(&mapbpb.MapBatchResponse{Results: []*mapbpb.MapBatchResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, fmt.Errorf("mock connection refused"))

	testClient, err := NewFromClient(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	result, err := testClient.MapBatchFn(ctx, &mapbpb.MapBatchRequest{})
	assert.Equal(t, &mapbpb.MapBatchResponse{Results: []*mapbpb.MapBatchResponse_Result{
		{
			Keys:  []string{"temp-key"},
			Value: []byte("mock result"),
			Tags:  nil,
		},
	}}, result)
	assert.NoError(t, err)

	_, err = testClient.MapBatchFn(ctx, &mapbpb.MapBatchRequest{})
	assert.EqualError(t, err, "NonRetryable: mock connection refused")
}
