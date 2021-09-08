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
	"io"
	"net"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/blob"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testProject    = "triton-for-games-dev"
	testBucket     = "gs://triton-integration"
	testBufferSize = 1024 * 1024
	testCacheAddr  = "localhost:6379"
	// The threshold of comparing times.
	// Since the server will actually access the backend datastore,
	// we need enough time to prevent flaky tests.
	timestampDelta = 10 * time.Second
	blobKind       = "blob"
)

func getOpenSavesServer(ctx context.Context, t *testing.T, cloud string) (*openSavesServer, *bufconn.Listener) {
	t.Helper()
	impl, err := newOpenSavesServer(ctx, cloud, testProject, testBucket, testCacheAddr)
	if err != nil {
		t.Fatalf("Failed to create a new Open Saves server instance: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterOpenSavesServer(server, impl)
	listener := bufconn.Listen(testBufferSize)
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

func setupTestStore(ctx context.Context, t *testing.T, client pb.OpenSavesClient, store *pb.Store) {
	t.Helper()
	req := &pb.CreateStoreRequest{
		Store: store,
	}
	res, err := client.CreateStore(ctx, req)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}
	t.Cleanup(func() {
		_, err := client.DeleteStore(ctx, &pb.DeleteStoreRequest{Key: store.Key})
		assert.NoError(t, err, "DeleteStore returned err")
	})
	if assert.NotNil(t, res) {
		store.CreatedAt = res.GetCreatedAt()
		store.UpdatedAt = res.GetUpdatedAt()
		assertEqualStore(t, store, res)
		assert.True(t, res.GetCreatedAt().AsTime().Equal(res.GetUpdatedAt().AsTime()))
	}
}

func cleanupBlobs(ctx context.Context, t *testing.T, storeKey, recordkey string) {
	t.Helper()
	client, err := datastore.NewClient(ctx, testProject)
	if err != nil {
		t.Errorf("datastore.NewClient failed during cleanup: %v", err)
		return
	}
	blobClient, err := blob.NewBlobGCP(testBucket)
	if err != nil {
		t.Errorf("NewBlobGCP returned error: %v", err)
		blobClient = nil
	}
	query := datastore.NewQuery(blobKind).Filter("StoreKey =", storeKey).Filter("RecordKey =", recordkey)
	iter := client.Run(ctx, query)

	for {
		var blobRef blobref.BlobRef
		key, err := iter.Next(&blobRef)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Errorf("iterator.Next returned error: %v", err)
			break
		}
		if blobClient != nil {
			blobClient.Delete(ctx, blobRef.ObjectPath())
		}
		if err := client.Delete(ctx, key); err != nil {
			t.Errorf("Delete for key (%v) returned error: %v", key.String(), err)
		}
	}
}

func setupTestRecord(ctx context.Context, t *testing.T, client pb.OpenSavesClient, storeKey string, record *pb.Record) {
	t.Helper()
	req := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record:   record,
	}
	res, err := client.CreateRecord(ctx, req)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}

	t.Cleanup(func() {
		cleanupBlobs(ctx, t, storeKey, record.Key)
		req := &pb.DeleteRecordRequest{
			StoreKey: storeKey,
			Key:      record.Key,
		}
		_, err = client.DeleteRecord(ctx, req)
		if err != nil {
			t.Errorf("DeleteRecord failed: %v", err)
		}
	})

	if assert.NotNil(t, record) {
		record.CreatedAt = res.GetCreatedAt()
		record.UpdatedAt = res.GetUpdatedAt()
		assertEqualRecord(t, record, res)
		assert.True(t, res.GetCreatedAt().AsTime().Equal(res.GetUpdatedAt().AsTime()))
	}
}

func TestOpenSaves_CreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	store := &pb.Store{
		Key:     storeKey,
		Name:    "test-createGetDeleteStore-store",
		Tags:    []string{"tag1"},
		OwnerId: "owner",
	}
	setupTestStore(ctx, t, client, store)

	getReq := &pb.GetStoreRequest{
		Key: storeKey,
	}
	store2, err := client.GetStore(ctx, getReq)
	if err != nil {
		t.Errorf("GetStore failed: %v", err)
	}
	assertEqualStore(t, store, store2)
	// Additional time checks as assertEqualStore doesn't check
	// exact timestamps.
	assert.Equal(t, store.GetCreatedAt(), store2.GetCreatedAt())
	assert.Equal(t, store.GetUpdatedAt(), store2.GetUpdatedAt())
}

func TestOpenSaves_CreateGetDeleteRecord(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.New().String()
	const testBlobSize = int64(42)
	record := &pb.Record{
		Key:          recordKey,
		BlobSize:     testBlobSize,
		Tags:         []string{"tag1", "tag2"},
		OwnerId:      "owner",
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit,",
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: &pb.Property_IntegerValue{IntegerValue: -42},
			},
		},
	}
	setupTestRecord(ctx, t, client, storeKey, record)

	getReq := &pb.GetRecordRequest{StoreKey: storeKey, Key: recordKey}
	record2, err := client.GetRecord(ctx, getReq)
	if err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}
	assertEqualRecord(t, record, record2)
	assert.Equal(t, record.GetCreatedAt(), record2.GetCreatedAt())
	assert.Equal(t, record.GetUpdatedAt(), record2.GetUpdatedAt())
}

func TestOpenSaves_UpdateRecordSimple(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

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

	const testBlobSize = int64(123)
	updateReq := &pb.UpdateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:          recordKey,
			BlobSize:     testBlobSize,
			OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit,",
		},
	}
	beforeUpdate := time.Now()
	record, err := client.UpdateRecord(ctx, updateReq)
	if err != nil {
		t.Fatalf("UpdateRecord failed: %v", err)
	}
	expected := &pb.Record{
		Key:          recordKey,
		BlobSize:     testBlobSize,
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit,",
		CreatedAt:    created.GetCreatedAt(),
		UpdatedAt:    timestamppb.Now(),
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
	store := &pb.Store{
		Key:  storeKey,
		Name: storeName,
	}
	setupTestStore(ctx, t, client, store)

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
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.New().String()
	const testBlobSize = int64(256)
	createReq := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:      recordKey,
			BlobSize: testBlobSize,
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
	created, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	expected.CreatedAt = timestamppb.Now()
	expected.UpdatedAt = expected.CreatedAt
	assertEqualRecord(t, expected, created)
	assert.Equal(t, created.GetCreatedAt(), created.GetUpdatedAt())

	// Check do not cache hint was honored.
	cacheKey := record.CacheKey(storeKey, recordKey)
	recFromCache := new(record.Record)
	err = server.cacheStore.Get(ctx, cacheKey, recFromCache)
	assert.Error(t, err, "should not have retrieved record from cache after Create with DoNotCache hint")

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

	recFromCache2 := new(record.Record)
	err = server.cacheStore.Get(ctx, cacheKey, recFromCache2)
	assert.Error(t, err, "should not have retrieved record from cache after Get with DoNotCache hint")

	// Modify GetRecordRequest to not use the hint.
	getReq.Hint = nil
	if _, err = client.GetRecord(ctx, getReq); err != nil {
		t.Errorf("GetRecord failed: %v", err)
	}

	recFromCache3 := new(record.Record)
	err = server.cacheStore.Get(ctx, cacheKey, recFromCache3)
	if assert.NoError(t, err, "should have retrieved record from cache after Get without hints") {
		assertEqualRecord(t, expected, recFromCache3.ToProto())
	}

	// Insert some bad data directly into the cache store.
	// Check that the SkipCache hint successfully skips the
	// cache and retrieves the correct data directly.
	server.cacheRecord(ctx, &record.Record{
		Key: "bad record",
	}, nil)
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

	recFromCache4 := new(record.Record)
	err = server.cacheStore.Get(ctx, cacheKey, recFromCache4)
	assert.Error(t, err, "should not have retrieved record from cache post-delete")
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

func createBlob(ctx context.Context, t *testing.T, client pb.OpenSavesClient,
	storeKey, recordKey string, content []byte) {
	t.Helper()

	cbc, err := client.CreateBlob(ctx)
	if err != nil {
		t.Errorf("CreateBlob returned error: %v", err)
		return
	}

	err = cbc.Send(&pb.CreateBlobRequest{
		Request: &pb.CreateBlobRequest_Metadata{
			Metadata: &pb.BlobMetadata{
				StoreKey:  storeKey,
				RecordKey: recordKey,
				Size:      int64(len(content)),
			},
		},
	})
	if err != nil {
		t.Errorf("CreateBlobClient.Send failed on sending metadata: %v", err)
		return
	}

	sent := 0
	for {
		if sent >= len(content) {
			break
		}
		toSend := streamBufferSize
		if toSend > len(content)-sent {
			toSend = len(content) - sent
		}
		err = cbc.Send(&pb.CreateBlobRequest{
			Request: &pb.CreateBlobRequest_Content{
				Content: content[sent : sent+toSend],
			},
		})
		if err != nil {
			t.Errorf("CreateBlobClient.Send failed on sending content: %v", err)
		}
		sent += toSend
	}
	assert.Equal(t, len(content), sent)

	meta, err := cbc.CloseAndRecv()
	if err != nil {
		t.Errorf("CreateBlobClient.CloseAndRecv failed: %v", err)
		return
	}
	if assert.NotNil(t, meta) {
		assert.Equal(t, storeKey, meta.StoreKey)
		assert.Equal(t, recordKey, meta.RecordKey)
		assert.Equal(t, int64(len(content)), meta.Size)
	}

	t.Cleanup(func() {
		// Ignore error as this is just a cleanup
		client.DeleteBlob(ctx, &pb.DeleteBlobRequest{
			StoreKey:  storeKey,
			RecordKey: recordKey,
		})
	})
}

func verifyBlob(ctx context.Context, t *testing.T, client pb.OpenSavesClient,
	storeKey, recordKey string, expectedContent []byte) {
	t.Helper()
	gbc, err := client.GetBlob(ctx, &pb.GetBlobRequest{
		StoreKey:  storeKey,
		RecordKey: recordKey,
	})
	if err != nil {
		t.Errorf("GetBlob returned error: %v", err)
		return
	}
	res, err := gbc.Recv()
	if err != nil {
		t.Errorf("GetBlobClient.Recv returned error: %v", err)
		return
	}
	meta := res.GetMetadata()
	if assert.NotNil(t, meta, "First returned message must be metadata") {
		assert.Equal(t, storeKey, meta.StoreKey)
		assert.Equal(t, recordKey, meta.RecordKey)
		assert.Equal(t, int64(len(expectedContent)), meta.Size)
	}

	recvd := 0
	for {
		res, err = gbc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("GetBlobClient.Recv returned error: %v", err)
			return
		}
		content := res.GetContent()
		if assert.NotNil(t, content, "Second returned message must be content") {
			assert.Equal(t, expectedContent[recvd:recvd+len(content)], content)
			recvd += len(content)
		}
	}
	assert.Equal(t, int64(recvd), meta.Size, "Received bytes should match")
}

func TestOpenSaves_InlineBlobSimple(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	store := &pb.Store{Key: uuid.New().String()}
	setupTestStore(ctx, t, client, store)
	record := &pb.Record{Key: uuid.New().String()}
	setupTestRecord(ctx, t, client, store.Key, record)

	beforeCreateBlob := time.Now()
	testBlob := []byte{0x42, 0x24, 0x00, 0x20, 0x20}
	createBlob(ctx, t, client, store.Key, record.Key, testBlob)

	// Check if the size is reflected to the record as well.
	updatedRecord, err := client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.Key, Key: record.Key,
	})
	if assert.NoError(t, err) {
		if assert.NotNil(t, updatedRecord) {
			assert.Equal(t, int64(len(testBlob)), updatedRecord.BlobSize)
			assert.True(t, record.GetCreatedAt().AsTime().Equal(updatedRecord.GetCreatedAt().AsTime()))
			assert.True(t, beforeCreateBlob.Before(updatedRecord.GetUpdatedAt().AsTime()))
		}
	}

	// Check the blob
	verifyBlob(ctx, t, client, store.Key, record.Key, testBlob)

	// Deletion test
	_, err = client.DeleteBlob(ctx, &pb.DeleteBlobRequest{
		StoreKey:  store.Key,
		RecordKey: record.Key,
	})
	if err != nil {
		t.Errorf("DeleteBlob failed: %v", err)
	}
	verifyBlob(ctx, t, client, store.Key, record.Key, make([]byte, 0))
}

func TestOpenSaves_ExternalBlobSimple(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	store := &pb.Store{Key: uuid.New().String()}
	setupTestStore(ctx, t, client, store)
	record := &pb.Record{Key: uuid.New().String()}
	setupTestRecord(ctx, t, client, store.Key, record)

	const blobSize = 4*1024*1024 + 13 // 4 Mi + 13 B
	testBlob := make([]byte, blobSize)
	for i := 0; i < blobSize; i++ {
		testBlob[i] = byte(i % 256)
	}

	beforeCreateBlob := time.Now()
	createBlob(ctx, t, client, store.Key, record.Key, testBlob)

	// Check if the size is reflected to the record as well.
	updatedRecord, err := client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.Key, Key: record.Key,
	})
	if assert.NoError(t, err) {
		if assert.NotNil(t, updatedRecord) {
			assert.Equal(t, int64(len(testBlob)), updatedRecord.BlobSize)
			assert.True(t, record.GetCreatedAt().AsTime().Equal(updatedRecord.GetCreatedAt().AsTime()))
			assert.True(t, beforeCreateBlob.Before(updatedRecord.GetUpdatedAt().AsTime()))
		}
	}

	// Check the blob
	verifyBlob(ctx, t, client, store.Key, record.Key, testBlob)

	// Deletion test
	_, err = client.DeleteBlob(ctx, &pb.DeleteBlobRequest{
		StoreKey:  store.Key,
		RecordKey: record.Key,
	})
	if err != nil {
		t.Errorf("DeleteBlob failed: %v", err)
	}
	verifyBlob(ctx, t, client, store.Key, record.Key, make([]byte, 0))
}

func TestOpenSaves_QueryRecords_Filter(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.New().String()
	stringVal1 := &pb.Property_StringValue{StringValue: "foo"}
	createReq := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key: recordKey1,
			Properties: map[string]*pb.Property{
				"prop1": {
					Type:  pb.Property_STRING,
					Value: stringVal1,
				},
			},
		},
	}
	_, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey1}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	recordKey2 := uuid.New().String()
	createReq.Record.Key = recordKey2
	stringVal2 := &pb.Property_StringValue{StringValue: "bar"}
	createReq.Record.Properties = map[string]*pb.Property{
		"prop1": {
			Type:  pb.Property_STRING,
			Value: stringVal2,
		},
	}
	_, err = client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey2}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	queryReq := &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		Filters: []*pb.QueryFilter{
			{
				PropertyName: "prop1",
				Operator:     pb.FilterOperator_EQUAL,
				Value: &pb.Property{
					Type:  pb.Property_STRING,
					Value: stringVal1,
				},
			},
		},
	}
	resp, err := client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	// Only one record matches the query.
	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, 1, len(resp.StoreKeys))

	assert.Equal(t, storeKey, resp.StoreKeys[0])
	assert.Equal(t, resp.Records[0].Properties["prop1"].Value, stringVal1)
}

func TestOpenSaves_QueryRecords_Owner(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.New().String()
	createReq := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:     recordKey1,
			OwnerId: "owner1",
		},
	}
	_, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey1}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	recordKey2 := uuid.New().String()
	createReq.Record.Key = recordKey2
	createReq.Record.OwnerId = "owner2"
	_, err = client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey2}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	queryReq := &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		OwnerId:  "owner1",
	}
	resp, err := client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	// Only one record matches the query.
	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, 1, len(resp.StoreKeys))

	assert.Equal(t, resp.Records[0].OwnerId, "owner1")
}

func TestOpenSaves_QueryRecords_Tags(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.New().String()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.New().String()
	createReq := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:  recordKey1,
			Tags: []string{"foo", "bar"},
		},
	}
	_, err := client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey1}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	recordKey2 := uuid.New().String()
	createReq.Record.Key = recordKey2
	createReq.Record.Tags = []string{"hello", "world"}
	_, err = client.CreateRecord(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateRecord failed: %v", err)
	}
	t.Cleanup(func() {
		deleteReq := &pb.DeleteRecordRequest{StoreKey: storeKey, Key: recordKey2}
		_, err := client.DeleteRecord(ctx, deleteReq)
		assert.NoError(t, err)
	})

	queryReq := &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		Tags:     []string{"hello", "world"},
	}
	resp, err := client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	// Only one record matches the query.
	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, 1, len(resp.StoreKeys))

	assert.Contains(t, resp.Records[0].Tags, "hello")
}

func TestOpenSaves_CreateChunkedBlobNonExistent(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)

	// Non-existent record should fail with codes.FailedPrecondition
	res, err := client.CreateChunkedBlob(ctx, &pb.CreateChunkedBlobRequest{
		StoreKey:  uuid.NewString(),
		RecordKey: uuid.NewString(),
		ChunkSize: 0,
	})
	assert.Nil(t, res)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}
