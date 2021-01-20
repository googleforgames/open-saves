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
	"time"

	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/cache"
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testProject   = "triton-for-games-dev"
	testBucket    = "gs://triton-integration"
	bufferSize    = 1024 * 1024
	testCacheAddr = "localhost:6379"
	// The threshold of comparing times.
	// Since the server will actually access the backend datastore,
	// we need enough time to prevent flaky tests.
	timestampDelta = 10 * time.Second
)

func getOpenSavesServer(ctx context.Context, t *testing.T, cloud string) (*openSavesServer, *bufconn.Listener) {
	t.Helper()
	impl, err := newOpenSavesServer(ctx, cloud, testProject, testBucket, testCacheAddr)
	if err != nil {
		t.Fatalf("Failed to create a new Open Saves server instance: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterOpenSavesServer(server, impl)
	listener := bufconn.Listen(bufferSize)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("Server exited with error: %v", err)
		}
	}()
	t.Cleanup(func() { server.Stop() })
	return impl, listener
}

func assertTimestampsWithinDelta(t *testing.T, expected, actual *timestamppb.Timestamp) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.WithinDuration(t, expected.AsTime(), actual.AsTime(), timestampDelta)
	}
}

func assertEqualStore(t *testing.T, expected, actual *pb.Store) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.Name, actual.Name)
		assert.ElementsMatch(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.OwnerId, actual.OwnerId)
		assertTimestampsWithinDelta(t, expected.GetCreatedAt(), actual.GetCreatedAt())
		assertTimestampsWithinDelta(t, expected.GetUpdatedAt(), actual.GetUpdatedAt())
	}
}

func assertEqualRecord(t *testing.T, expected, actual *pb.Record) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.BlobSize, actual.BlobSize)
		assert.ElementsMatch(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.OwnerId, actual.OwnerId)
		// assert.Equal(t, expectecd.Properties, actual.Properties) doesn't work.
		// See Issue #138
		assert.Equal(t, len(expected.Properties), len(actual.Properties))
		for k, v := range expected.Properties {
			if assert.Contains(t, actual.Properties, k) {
				av := actual.Properties[k]
				assert.Equal(t, v.Type, av.Type)
				assert.Equal(t, v.Value, av.Value)
			}
		}
		assertTimestampsWithinDelta(t, expected.GetCreatedAt(), actual.GetCreatedAt())
		assertTimestampsWithinDelta(t, expected.GetUpdatedAt(), actual.GetUpdatedAt())
	}
}

func getTestClient(ctx context.Context, t *testing.T, listener *bufconn.Listener) (*grpc.ClientConn, pb.OpenSavesClient) {
	t.Helper()
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
	client := pb.NewOpenSavesClient(conn)
	return conn, client
}

func TestOpenSaves_CreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	storeReq := &pb.CreateStoreRequest{
		Store: &pb.Store{
			Key:     storeKey,
			Name:    "test-createGetDeleteStore-store",
			Tags:    []string{"tag1"},
			OwnerId: "owner",
		},
	}
	expected := storeReq.Store
	storeRes, err := client.CreateStore(ctx, storeReq)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}
	expected.CreatedAt = timestamppb.Now()
	expected.UpdatedAt = expected.CreatedAt
	assertEqualStore(t, expected, storeRes)
	assert.Equal(t, storeRes.GetCreatedAt(), storeRes.GetUpdatedAt())

	getReq := &pb.GetStoreRequest{
		Key: storeKey,
	}
	store2, err := client.GetStore(ctx, getReq)
	if err != nil {
		t.Errorf("GetStore failed: %v", err)
	}
	assertEqualStore(t, expected, store2)
	// Additional time checks as assertEqualStore doesn't check
	// exact timestamps.
	assert.Equal(t, storeRes.GetCreatedAt(), store2.GetCreatedAt())
	assert.Equal(t, storeRes.GetUpdatedAt(), store2.GetUpdatedAt())

	deleteReq := &pb.DeleteStoreRequest{
		Key: storeKey,
	}
	_, err = client.DeleteStore(ctx, deleteReq)
	assert.NoError(t, err)
}

func TestOpenSaves_CreateGetDeleteRecord(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
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
	expected := createReq.Record
	record, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	expected.CreatedAt = timestamppb.Now()
	expected.UpdatedAt = expected.CreatedAt
	assertEqualRecord(t, expected, record)
	assert.Equal(t, record.GetCreatedAt(), record.GetUpdatedAt())

	getReq := &pb.GetRecordRequest{StoreKey: storeKey, Key: recordKey}
	record2, err := client.GetRecord(ctx, getReq)
	if err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}
	assertEqualRecord(t, expected, record2)
	assert.Equal(t, record.GetCreatedAt(), record2.GetCreatedAt())
	assert.Equal(t, record.GetUpdatedAt(), record2.GetUpdatedAt())

	deleteReq := &pb.DeleteRecordRequest{
		StoreKey: storeKey,
		Key:      recordKey,
	}
	_, err = client.DeleteRecord(ctx, deleteReq)
	if err != nil {
		t.Errorf("DeleteRecord failed: %v", err)
	}
}

func TestOpenSaves_UpdateRecordSimple(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
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
	created, err := client.CreateRecord(ctx, createReq)
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
			BlobSize: int64(len(testBlob)),
		},
	}
	beforeUpdate := time.Now()
	record, err := client.UpdateRecord(ctx, updateReq)
	if err != nil {
		t.Fatalf("UpdateRecord failed: %v", err)
	}
	expected := &pb.Record{
		Key:       recordKey,
		BlobSize:  int64(len(testBlob)),
		CreatedAt: created.GetCreatedAt(),
		UpdatedAt: timestamppb.Now(),
	}
	assertEqualRecord(t, expected, record)
	assert.True(t, created.GetCreatedAt().AsTime().Equal(record.GetCreatedAt().AsTime()))
	assert.NotEqual(t, record.GetCreatedAt().AsTime(), record.GetUpdatedAt().AsTime())
	assert.True(t, beforeUpdate.Before(record.GetUpdatedAt().AsTime()))
}

func TestOpenSaves_ListStoresNamePerfectMatch(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
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
		now := timestamppb.Now()
		expected := &pb.Store{
			Key:       storeKey,
			Name:      storeName,
			CreatedAt: now,
			UpdatedAt: now,
		}
		assertEqualStore(t, expected, listRes.GetStores()[0])
	}
}

func TestOpenSaves_CacheRecordsWithHints(t *testing.T) {
	ctx := context.Background()
	server, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
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
		Hint: &pb.Hint{
			DoNotCache: true,
		},
	}
	expected := createReq.Record
	record, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	expected.CreatedAt = timestamppb.Now()
	expected.UpdatedAt = expected.CreatedAt
	assertEqualRecord(t, expected, record)
	assert.Equal(t, record.GetCreatedAt(), record.GetUpdatedAt())

	// Check do not cache hint was honored.
	key := cache.FormatKey(storeKey, recordKey)
	recFromCache, _ := server.getRecordFromCache(ctx, key)
	assert.Nil(t, recFromCache, "should not have retrieved record from cache after Create with DoNotCache hint")

	getReq := &pb.GetRecordRequest{
		StoreKey: storeKey,
		Key:      recordKey,
		Hint: &pb.Hint{
			DoNotCache: true,
		},
	}
	if _, err = client.GetRecord(ctx, getReq); err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}

	recFromCache2, _ := server.getRecordFromCache(ctx, key)
	assert.Nil(t, recFromCache2, "should not have retrieved record from cache after Get with DoNotCache hint")

	// Modify GetRecordRequest to not use the hint.
	getReq.Hint = nil
	if _, err = client.GetRecord(ctx, getReq); err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}

	recFromCache3, _ := server.getRecordFromCache(ctx, key)
	assert.NotNil(t, recFromCache3, "should have retrieved record from cache after Get without hints")
	assertEqualRecord(t, expected, recFromCache3.ToProto())

	// Insert some bad data directly into the cache store.
	// Check that the SkipCache hint successfully skips the
	// cache and retrieves the correct data directly.
	server.storeRecordInCache(ctx, key, &metadb.Record{
		Key: "bad record",
	})
	getReqSkipCache := &pb.GetRecordRequest{
		StoreKey: storeKey,
		Key:      recordKey,
		Hint: &pb.Hint{
			SkipCache: true,
		},
	}
	gotRecord, err := client.GetRecord(ctx, getReqSkipCache)
	if err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}
	assertEqualRecord(t, expected, gotRecord)

	deleteReq := &pb.DeleteRecordRequest{
		StoreKey: storeKey,
		Key:      recordKey,
	}
	_, err = client.DeleteRecord(ctx, deleteReq)
	if err != nil {
		t.Errorf("DeleteRecord failed: %v", err)
	}

	recFromCache4, _ := server.getRecordFromCache(ctx, key)
	assert.Nil(t, recFromCache4, "should not have retrieved record from cache post-delete")
}

func TestOpenSaves_Ping(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)

	pong, err := client.Ping(ctx, new(pb.PingRequest))
	if err != nil {
		t.Fatalf("Ping failed with an empty string: %v", err)
	}
	assert.Empty(t, pong.GetPong())

	const testString = "The quick brown fox jumps over the lazy dog. 色は匂へど。"
	pong, err = client.Ping(ctx, &pb.PingRequest{Ping: testString})
	if err != nil {
		t.Fatalf("Ping failed with a non-empty string: %v", err)
	}
	assert.Equal(t, testString, pong.GetPong())
}
