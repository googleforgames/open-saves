// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	pb "github.com/googleforgames/triton/api"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const (
	testProject = "triton-for-games-dev"
	testBucket  = "gs://triton-integration"
	bufferSize  = 1024 * 1024
)

func makeTestFunc(ctx context.Context,
	testFunc func(t *testing.T, ctx context.Context, cloud string), cloud string) func(*testing.T) {
	return func(t *testing.T) {
		testFunc(t, ctx, cloud)
	}
}

func getTestServer(ctx context.Context, t *testing.T, cloud string) (*grpc.Server, *bufconn.Listener) {
	impl, err := newTritonServer(ctx, cloud, testProject, testBucket)
	if err != nil {
		t.Fatalf("Failed to create a new Triton server instance: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterTritonServer(server, impl)
	listener := bufconn.Listen(bufferSize)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("Server exited with error: %v", err)
		}
	}()
	t.Cleanup(func() { server.Stop() })
	return server, listener
}

func assertEqualStore(t *testing.T, expected, actual *pb.Store) {
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.Name, actual.Name)
		assert.Equal(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.OwnerId, actual.OwnerId)
	}
}

func assertEqualRecord(t *testing.T, expected, actual *pb.Record) {
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.Blob, actual.Blob)
		assert.Equal(t, expected.BlobSize, actual.BlobSize)
		assert.Equal(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.OwnerId, actual.OwnerId)
		assert.Equal(t, len(expected.Properties), len(actual.Properties))
		for k, v := range expected.Properties {
			if assert.Contains(t, actual.Properties, k) {
				av := actual.Properties[k]
				assert.Equal(t, v.Type, av.Type)
				assert.Equal(t, v.Value, av.Value)
			}
		}
	}
}

func getTestClient(ctx context.Context, t *testing.T, listener *bufconn.Listener) (*grpc.ClientConn, pb.TritonClient) {
	conn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(
		func(_ context.Context, _ string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("Failed to create a gRPC connection: %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	client := pb.NewTritonClient(conn)
	return conn, client
}

func TestTriton(t *testing.T) {
	ctx := context.Background()
	backends := []string{"gcp"}
	for _, v := range backends {
		t.Run(v, func(t *testing.T) {
			testTritonBackend(ctx, t, v)
		})
	}
}

func testTritonBackend(ctx context.Context, t *testing.T, cloud string) {
	_, listener := getTestServer(ctx, t, cloud)
	_, client := getTestClient(ctx, t, listener)
	t.Run("CreateGetDeleteStore", func(t *testing.T) { createGetDeleteStore(ctx, t, client) })
	t.Run("CreateGetDeleteRecord", func(t *testing.T) { createGetDeleteRecord(ctx, t, client) })
	t.Run("UpdateRecordSimple", func(t *testing.T) { updateRecordSimple(ctx, t, client) })
	t.Run("ListStoresNamePerfectMatch",
		func(t *testing.T) { listStoresNamePerfectMatch(ctx, t, client) })
}

func createGetDeleteStore(ctx context.Context, t *testing.T, client pb.TritonClient) {
	storeKey := uuid.New().String()
	storeReq := &pb.CreateStoreRequest{
		Store: &pb.Store{
			Key:     storeKey,
			Name:    "test-createGetDeleteStore-store",
			Tags:    []string{"tag1"},
			OwnerId: "owner",
		},
	}
	storeRes, err := client.CreateStore(ctx, storeReq)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}
	assertEqualStore(t, storeReq.GetStore(), storeRes)

	getReq := &pb.GetStoreRequest{
		Key: storeKey,
	}
	store2, err := client.GetStore(ctx, getReq)
	if err != nil {
		t.Errorf("GetStore failed: %v", err)
	}
	assertEqualStore(t, storeReq.GetStore(), store2)

	deleteReq := &pb.DeleteStoreRequest{
		Key: storeKey,
	}
	_, err = client.DeleteStore(ctx, deleteReq)
	assert.NoError(t, err)
}

func createGetDeleteRecord(ctx context.Context, t *testing.T, client pb.TritonClient) {
	storeKey := uuid.New().String()
	storeReq := &pb.CreateStoreRequest{
		Store: &pb.Store{
			Key: storeKey,
		},
	}
	_, err := client.CreateStore(ctx, storeReq)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}
	t.Cleanup(func() {
		req := &pb.DeleteStoreRequest{Key: storeKey}
		_, err := client.DeleteStore(ctx, req)
		assert.NoError(t, err)
	})

	recordKey := uuid.New().String()
	testBlob := []byte{0x42, 0x24, 0x00}
	createReq := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:      recordKey,
			Blob:     testBlob,
			BlobSize: int64(len(testBlob)),
			Tags:     []string{"tag1", "tag2"},
			OwnerId:  "owner",
			Properties: map[string]*pb.Property{
				"prop1": {
					Type:  pb.Property_INTEGER,
					Value: &pb.Property_IntegerValue{IntegerValue: -42},
				},
			},
		},
	}
	record, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	assertEqualRecord(t, createReq.Record, record)

	getReq := &pb.GetRecordRequest{StoreKey: storeKey, Key: recordKey}
	record2, err := client.GetRecord(ctx, getReq)
	if err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}
	assertEqualRecord(t, createReq.Record, record2)

	deleteReq := &pb.DeleteRecordRequest{
		StoreKey: storeKey,
		Key:      recordKey,
	}
	_, err = client.DeleteRecord(ctx, deleteReq)
	if err != nil {
		t.Errorf("DeleteRecord failed: %v", err)
	}
}

func updateRecordSimple(ctx context.Context, t *testing.T, client pb.TritonClient) {
	storeKey := uuid.New().String()
	storeReq := &pb.CreateStoreRequest{
		Store: &pb.Store{
			Key: storeKey,
		},
	}
	_, err := client.CreateStore(ctx, storeReq)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}
	t.Cleanup(func() {
		req := &pb.DeleteStoreRequest{Key: storeKey}
		_, err := client.DeleteStore(ctx, req)
		assert.NoError(t, err)
	})

	recordKey := uuid.New().String()
	createReq := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:     recordKey,
			OwnerId: "owner",
		},
	}
	_, err = client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	testBlob := []byte{0x42, 0x24, 0x00}
	updateReq := &pb.UpdateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:      recordKey,
			Blob:     testBlob,
			BlobSize: int64(len(testBlob)),
		},
	}
	record, err := client.UpdateRecord(ctx, updateReq)
	if err != nil {
		t.Fatalf("UpdateRecord failed: %v", err)
	}
	expected := &pb.Record{
		Key:      recordKey,
		Blob:     testBlob,
		BlobSize: int64(len(testBlob)),
	}
	assertEqualRecord(t, expected, record)
}

func listStoresNamePerfectMatch(ctx context.Context, t *testing.T, client pb.TritonClient) {
	storeKey := uuid.New().String()
	storeName := "test store " + uuid.New().String()
	storeReq := &pb.CreateStoreRequest{
		Store: &pb.Store{
			Key:  storeKey,
			Name: storeName,
		},
	}
	_, err := client.CreateStore(ctx, storeReq)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}
	t.Cleanup(func() {
		req := &pb.DeleteStoreRequest{Key: storeKey}
		_, err := client.DeleteStore(ctx, req)
		assert.NoError(t, err)
	})

	listReq := &pb.ListStoresRequest{
		Name: storeName,
	}
	listRes, err := client.ListStores(ctx, listReq)
	assert.NoError(t, err)
	if assert.NotNil(t, listRes.GetStores()) && assert.Len(t, listRes.GetStores(), 1) {
		expected := &pb.Store{
			Key:  storeKey,
			Name: storeName,
		}
		assertEqualStore(t, expected, listRes.GetStores()[0])
	}
}
