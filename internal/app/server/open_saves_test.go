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
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/googleforgames/open-saves/internal/pkg/cmd"
	"github.com/googleforgames/open-saves/internal/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/blob"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testProject    = "triton-for-games-dev"
	testBucket     = "gs://triton-integration"
	testPort       = "8000"
	testBufferSize = 1024 * 1024
	// The threshold of comparing times.
	// Since the server will actually access the backend datastore,
	// we need enough time to prevent flaky tests.
	timestampDelta = 10 * time.Second
	blobKind       = "blob"
)

var (
	serviceConfig *config.ServiceConfig
)

func getServiceConfig() (*config.ServiceConfig, error) {
	if serviceConfig != nil {
		return serviceConfig, nil
	}
	configPath := cmd.GetEnvVarString("OPEN_SAVES_CONFIG", "../../../configs/")
	viper.Set(config.OpenSavesBucket, testBucket)
	viper.Set(config.OpenSavesProject, testProject)
	viper.Set(config.OpenSavesPort, testPort)
	sc, err := config.Load(configPath)
	serviceConfig = sc
	return sc, err
}

func TestOpenSaves_HealthCheck(t *testing.T) {
	serviceConfig, err := getServiceConfig()
	if err != nil {
		t.Fatalf("getServiceConfig err: %v", err)
	}
	ctx := context.Background()
	go func() {
		if err := Run(ctx, "tcp", serviceConfig); err != nil {
			log.Errorf("got err calling server.Run: %v", err)
		}
	}()

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial(fmt.Sprintf(":%s", testPort), options...)
	if err != nil {
		t.Fatalf("did not connect %v", err)
	}
	defer conn.Close()

	hc := healthgrpc.NewHealthClient(conn)
	got, err := hc.Check(ctx, &healthgrpc.HealthCheckRequest{
		Service: serviceName,
	})
	if err != nil {
		t.Errorf("healthClient.Check err: %v", err)
	}
	if want := healthgrpc.HealthCheckResponse_SERVING; got.Status != want {
		t.Errorf("hc.Check got: %v, want: %v", got.Status, want)
	}
}

func TestOpenSaves_RunServer(t *testing.T) {
	serviceConfig, err := getServiceConfig()
	if err != nil {
		t.Fatalf("getServiceConfig err: %v", err)
	}
	ctx := context.Background()
	t.Run("cancel_context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := Run(ctx, "tcp", serviceConfig); err != nil {
			t.Errorf("got err calling server.Run: %v", err)
		}
	})
	t.Run("sigint", func(t *testing.T) {
		go func() {
			time.Sleep(2 * time.Second)
			syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}()
		if err := Run(ctx, "tcp", serviceConfig); err != nil {
			t.Errorf("got err calling server.Run: %v", err)
		}
	})
}

func getOpenSavesServer(ctx context.Context, t *testing.T, cloud string) (*openSavesServer, *bufconn.Listener) {
	t.Helper()
	r := miniredis.RunT(t)

	cfg := &config.ServiceConfig{
		ServerConfig: config.ServerConfig{
			Address: ":6000",
			Cloud:   cloud,
			Bucket:  testBucket,
			Project: testProject,
		},
		RedisConfig: config.RedisConfig{
			Address:     r.Addr(),
			PoolSize:    10000,
			IdleTimeout: 0,
		},
	}
	impl, err := newOpenSavesServer(ctx, cfg)
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
		if u, err := uuid.FromBytes(actual.Signature); assert.NoError(t, err) {
			v, _ := uuid.FromBytes(expected.Signature)
			if v != uuid.Nil {
				assert.Equal(t, v, u)
			}
		}
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
	blobClient, err := blob.NewBlobGCP(ctx, testBucket)
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

// tryCreateRecord wraps client.CreateRecord and returns the returned values.
// If CreateRecord succeeds, it adds a Cleanup hook to delete the record.
func tryCreateRecord(ctx context.Context, t *testing.T, client pb.OpenSavesClient, req *pb.CreateRecordRequest) (*pb.Record, error) {
	t.Helper()
	res, err := client.CreateRecord(ctx, req)
	if err == nil {
		storeKey := req.GetStoreKey()
		key := res.GetKey()
		t.Cleanup(func() {
			cleanupBlobs(ctx, t, storeKey, key)
			_, err = client.DeleteRecord(ctx, &pb.DeleteRecordRequest{
				StoreKey: storeKey,
				Key:      key,
			})
			assert.NoError(t, err, "DeleteRecord failed during cleanup")
		})
	}
	return res, err
}

func setupTestRecord(ctx context.Context, t *testing.T, client pb.OpenSavesClient, storeKey string, record *pb.Record) *pb.Record {
	t.Helper()
	return setupTestRecordWithHint(ctx, t, client, storeKey, record, nil)
}

func setupTestRecordWithHint(ctx context.Context, t *testing.T, client pb.OpenSavesClient, storeKey string, record *pb.Record, hint *pb.Hint) *pb.Record {
	t.Helper()
	req := &pb.CreateRecordRequest{
		StoreKey: storeKey,
		Record:   record,
		Hint:     hint,
	}
	res, err := tryCreateRecord(ctx, t, client, req)
	require.NoError(t, err, "CreateRecord failed")

	if assert.NotNil(t, record) {
		record.CreatedAt = res.GetCreatedAt()
		record.UpdatedAt = res.GetUpdatedAt()
		assertEqualRecord(t, record, res)
		assert.True(t, res.GetCreatedAt().AsTime().Equal(res.GetUpdatedAt().AsTime()))
		assert.NotEqual(t, uuid.Nil, record.Signature)
	}
	return res
}

func funcValueToName(f any) string {
	s := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	return s[strings.LastIndex(s, ".")+1:]
}

func TestOpenSaves_CreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
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
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.NewString()
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
	assert.NotEqual(t, uuid.Nil, record2.Signature)
	assert.Equal(t, record.GetCreatedAt(), record2.GetCreatedAt())
	assert.Equal(t, record.GetUpdatedAt(), record2.GetUpdatedAt())
}

func TestOpenSaves_UpdateRecordSimple(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.NewString()
	created := setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:     recordKey,
		OwnerId: "owner",
	})

	updateReq := &pb.UpdateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:          recordKey,
			BlobSize:     123,
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
		BlobSize:     0, // should be ignored
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit,",
		CreatedAt:    created.GetCreatedAt(),
		UpdatedAt:    timestamppb.Now(),
	}
	assertEqualRecord(t, expected, record)
	assert.True(t, created.GetCreatedAt().AsTime().Equal(record.GetCreatedAt().AsTime()))
	assert.NotEqual(t, record.GetCreatedAt().AsTime(), record.GetUpdatedAt().AsTime())
	assert.True(t, beforeUpdate.Before(record.GetUpdatedAt().AsTime()))
	assert.NotEqual(t, created.Signature, record.Signature)
}

func TestOpenSaves_UpdateRecordWithSignature(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.NewString()
	created := setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:     recordKey,
		OwnerId: t.Name(),
	})

	assert.NotEmpty(t, created.Signature)
	assert.NotEqual(t, uuid.Nil, created.Signature)

	updateReq := &pb.UpdateRecordRequest{
		StoreKey: storeKey,
		Record: &pb.Record{
			Key:          recordKey,
			OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit,",
			Signature:    created.Signature,
		},
	}
	if record, err := client.UpdateRecord(ctx, updateReq); assert.NoErrorf(t, err, "UpdateRecord failed: %v", err) {
		assert.Equal(t, recordKey, record.Key)
		assert.Equal(t, record.OpaqueString, updateReq.Record.OpaqueString)
		assert.NotEqual(t, created.Signature, record.Signature)
	}

	dummyUUID := uuid.New()
	updateReq.Record.Signature = dummyUUID[:]
	if record, err := client.UpdateRecord(ctx, updateReq); assert.Error(t, err) {
		assert.Nil(t, record)
		assert.Equal(t, codes.Aborted, status.Code(err))
	}
}

func TestOpenSaves_ListStoresNamePerfectMatch(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	storeName := "test store " + uuid.NewString()
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
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.NewString()
	const testBlobSize = int64(256)
	expected := &pb.Record{
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
	}
	created := setupTestRecordWithHint(ctx, t, client, storeKey, expected, &pb.Hint{DoNotCache: true})

	expected.CreatedAt = timestamppb.Now()
	expected.UpdatedAt = expected.CreatedAt
	assertEqualRecord(t, expected, created)
	assert.Equal(t, created.GetCreatedAt(), created.GetUpdatedAt())

	// Check do not cache hint was honored.
	cacheKey := record.CacheKey(storeKey, recordKey)
	recFromCache := new(record.Record)
	err := server.cacheStore.Get(ctx, cacheKey, recFromCache)
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

// createBlobWithChecksums creates a blob with optional checksums.
// Passing nil to md5 or crc32c skips integrity checking on the server side.
// Passing an incorrect value fails the test.
func createBlobWithChecksums(ctx context.Context, t *testing.T, client pb.OpenSavesClient,
	storeKey, recordKey string, content []byte, cs checksums.Checksums) {
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
				Md5:       cs.MD5,
				Crc32C:    cs.GetCRC32C(),
				HasCrc32C: cs.HasCRC32C,
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
		// The server always sets the checksums regardless of what the client sends.
		assert.NotEmpty(t, meta.Md5)
		assert.True(t, meta.HasCrc32C)
	}

	t.Cleanup(func() {
		// Ignore error as this is just a cleanup
		client.DeleteBlob(ctx, &pb.DeleteBlobRequest{
			StoreKey:  storeKey,
			RecordKey: recordKey,
		})
	})
}

// createBlob calls createBlobWithChecksums with both MD5 and CRC32C.
func createBlob(ctx context.Context, t *testing.T, client pb.OpenSavesClient,
	storeKey, recordKey string, content []byte) {
	digest := checksums.NewDigest()
	digest.Write(content)
	createBlobWithChecksums(ctx, t, client, storeKey, recordKey, content, digest.Checksums())
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

		if len(expectedContent) > 0 {
			digest := checksums.NewDigest()
			digest.Write(expectedContent)
			checksums := digest.Checksums()

			assert.Equal(t, checksums.MD5, meta.Md5)
			assert.True(t, meta.HasCrc32C)
			assert.Equal(t, checksums.GetCRC32C(), meta.Crc32C)
		} else {
			assert.Empty(t, meta.Md5)
			assert.False(t, meta.HasCrc32C)
		}
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
	store := &pb.Store{Key: uuid.NewString()}
	setupTestStore(ctx, t, client, store)
	record := &pb.Record{Key: uuid.NewString()}
	createdRecord := setupTestRecord(ctx, t, client, store.Key, record)

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
			assert.NotEqual(t, createdRecord.Signature, updatedRecord.Signature)
		}
	}

	// Check the blob
	verifyBlob(ctx, t, client, store.Key, record.Key, testBlob)

	// UpdateRecord should retain the inline blob.
	client.UpdateRecord(ctx, &pb.UpdateRecordRequest{
		StoreKey: store.Key,
		Record: &pb.Record{
			Key:      record.Key,
			BlobSize: 0,
		},
	})

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

	if r, err := client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.Key, Key: record.Key,
	}); assert.NoError(t, err) {
		assert.NotEqual(t, updatedRecord.Signature, r.Signature)
	}
}

func TestOpenSaves_ExternalBlobSimple(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	store := &pb.Store{Key: uuid.NewString()}
	setupTestStore(ctx, t, client, store)
	record := &pb.Record{Key: uuid.NewString()}
	record = setupTestRecord(ctx, t, client, store.Key, record)

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
			assert.NotEqual(t, record.Signature, updatedRecord.Signature)
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

func TestOpenSaves_QueryRecords_EqualityFilter(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.NewString()
	stringVal1 := &pb.Property_StringValue{StringValue: "foo"}
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey1,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_STRING,
				Value: stringVal1,
			},
		},
	})

	recordKey2 := uuid.NewString()
	stringVal2 := &pb.Property_StringValue{StringValue: "bar"}
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey2,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_STRING,
				Value: stringVal2,
			},
		},
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
	require.Len(t, resp.Records, 1)
	require.Len(t, resp.StoreKeys, 1)

	assert.Equal(t, storeKey, resp.StoreKeys[0])
	assert.Equal(t, resp.Records[0].Properties["prop1"].Value, stringVal1)

	// Test KeysOnly
	queryReq.KeysOnly = true
	resp, err = client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	require.Len(t, resp.Records, 1)
	assertEqualRecord(t, &pb.Record{Key: recordKey1}, resp.Records[0])
	assert.Equal(t, []string{storeKey}, resp.GetStoreKeys())
}

// NOTE: this test requires composite indexes to be created. You can find the corresponding index
// configuration in deploy/datastore/index.yaml
func TestOpenSaves_QueryRecords_InequalityFilter(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.NewString()
	intVal1 := &pb.Property_IntegerValue{IntegerValue: 10}
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey1,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: intVal1,
			},
		},
	})

	recordKey2 := uuid.NewString()
	intVal2 := &pb.Property_IntegerValue{IntegerValue: 20}
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey2,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: intVal2,
			},
		},
	})

	intVal3 := &pb.Property_IntegerValue{IntegerValue: 0}
	queryReq := &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		Filters: []*pb.QueryFilter{
			{
				PropertyName: "prop1",
				Operator:     pb.FilterOperator_GREATER,
				Value: &pb.Property{
					Type:  pb.Property_INTEGER,
					Value: intVal3,
				},
			},
		},
	}
	resp, err := client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)

	// Both records match the query.
	require.Equal(t, 2, len(resp.Records))
	require.Equal(t, 2, len(resp.StoreKeys))

	assert.Equal(t, storeKey, resp.StoreKeys[0])
	assert.Equal(t, resp.Records[0].Properties["prop1"].Value, intVal1)
	assert.Equal(t, resp.Records[1].Properties["prop1"].Value, intVal2)

	// Run a new query that matches only one record.
	queryReq.Filters[0].Value.Value = &pb.Property_IntegerValue{IntegerValue: 15}

	resp, err = client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))

	assert.Equal(t, resp.Records[0].Properties["prop1"].Value, intVal2)
}

func TestOpenSaves_QueryRecords_Owner(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:     recordKey1,
		OwnerId: "owner1",
	})

	recordKey2 := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:     recordKey2,
		OwnerId: "owner2",
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
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:  recordKey1,
		Tags: []string{"foo", "bar"},
	})

	recordKey2 := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:  recordKey2,
		Tags: []string{"hello", "world"},
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

// NOTE: this test requires composite indexes to be created. You can find the corresponding index
// configuration in deploy/datastore/index.yaml
func TestOpenSaves_QueryRecords_Order(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.NewString()
	intVal1 := &pb.Property_IntegerValue{IntegerValue: 5}
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey1,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: intVal1,
			},
		},
	})

	recordKey2 := uuid.NewString()
	intVal2 := &pb.Property_IntegerValue{IntegerValue: 10}
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey2,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: intVal2,
			},
		},
	})

	queryReq := &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		SortOrders: []*pb.SortOrder{
			{
				Property:         pb.SortOrder_USER_PROPERTY,
				UserPropertyName: "prop1",
				Direction:        pb.SortOrder_DESC,
			},
		},
	}
	resp, err := client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Records))

	// Verify these records are returned in descending order.
	assert.Equal(t, resp.Records[0].Properties["prop1"].Value, intVal2)
	assert.Equal(t, resp.Records[1].Properties["prop1"].Value, intVal1)

	queryReq = &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		SortOrders: []*pb.SortOrder{
			{
				Property:         pb.SortOrder_USER_PROPERTY,
				UserPropertyName: "prop1",
				Direction:        pb.SortOrder_ASC,
			},
		},
	}
	resp, err = client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Records))

	// Verify these records are returned in ascending order.
	assert.Equal(t, resp.Records[0].Properties["prop1"].Value, intVal1)
	assert.Equal(t, resp.Records[1].Properties["prop1"].Value, intVal2)

	queryReq = &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		SortOrders: []*pb.SortOrder{
			{
				Property:  pb.SortOrder_UPDATED_AT,
				Direction: pb.SortOrder_ASC,
			},
		},
	}
	resp, err = client.QueryRecords(ctx, queryReq)
	// These records are created at the same time so no way of verifying the order.
	// Just make sure there's no error returned by this query.
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Records))

	// Test errors
	queryReq = &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		SortOrders: []*pb.SortOrder{
			{
				Property:  pb.SortOrder_USER_PROPERTY,
				Direction: pb.SortOrder_ASC,
			},
		},
	}
	_, err = client.QueryRecords(ctx, queryReq)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	queryReq = &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		SortOrders: []*pb.SortOrder{
			{
				Property:  pb.SortOrder_CREATED_AT,
				Direction: 3,
			},
		},
	}
	_, err = client.QueryRecords(ctx, queryReq)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestOpenSaves_QueryRecords_Limit(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey1 := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey1,
	})

	recordKey2 := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey2,
	})

	queryReq := &pb.QueryRecordsRequest{
		StoreKey: storeKey,
		Limit:    1,
	}
	resp, err := client.QueryRecords(ctx, queryReq)
	require.NoError(t, err)
	// Make sure the query only returns 1 record.
	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, 1, len(resp.StoreKeys))
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

func verifyProperty(ctx context.Context, t *testing.T, c pb.OpenSavesClient, storeKey, recordKey string,
	name string, expected *pb.Property, sig []byte, updated bool) {
	t.Helper()

	rr, err := c.GetRecord(ctx, &pb.GetRecordRequest{StoreKey: storeKey, Key: recordKey})
	if assert.NotNil(t, rr) && assert.NoError(t, err) {
		actual := rr.GetProperties()[name]
		if assert.NotNil(t, actual) {
			assert.Equal(t, expected.GetType(), actual.GetType())
			assert.Equal(t, expected.GetValue(), actual.GetValue())
		}
		if updated {
			assert.NotEqual(t, sig, rr.GetSignature())
		} else {
			assert.Equal(t, sig, rr.GetSignature())
		}
	}
}

func TestOpenSaves_CompareAndSwap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)
	const testPropertyName = "prop1"

	newIntProp := record.NewIntegerPropertyProto
	newStringProp := record.NewStringPropertyProto
	testCases := []struct {
		start       *pb.Property
		oldValue    *pb.Property
		value       *pb.Property
		wantUpdated bool
	}{
		{newIntProp(42), newIntProp(41), newIntProp(42), false},
		{newIntProp(42), newIntProp(42), newIntProp(43), true},
		{newIntProp(42), newIntProp(42), newStringProp("hello, world"), true},
		{newStringProp("hello, world"), newIntProp(42), newStringProp("hello, world"), false},
		{newStringProp("hello, world"), newStringProp("hello, world"), newIntProp(42), true},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("start %v old %v value %v",
			tc.start.GetValue(), tc.oldValue.GetValue(), tc.value.GetValue()), func(t *testing.T) {
			t.Parallel()

			recordKey := uuid.NewString()
			rr := setupTestRecord(ctx, t, client, storeKey, &pb.Record{
				Key: recordKey,
				Properties: map[string]*pb.Property{
					testPropertyName: tc.start,
				},
			})
			res, err := client.CompareAndSwap(ctx, &pb.CompareAndSwapRequest{
				StoreKey:     storeKey,
				RecordKey:    recordKey,
				PropertyName: testPropertyName,
				OldValue:     tc.oldValue,
				Value:        tc.value,
			})
			if assert.NotNil(t, res) && assert.NoError(t, err) {
				assert.Equal(t, tc.start.GetType(), res.GetValue().GetType())
				assert.Equal(t, tc.start.GetValue(), res.GetValue().GetValue())
				assert.Equal(t, tc.wantUpdated, res.GetUpdated())
			}
			if tc.wantUpdated {
				verifyProperty(ctx, t, client, storeKey, recordKey, testPropertyName,
					tc.value, rr.GetSignature(), true)
			} else {
				verifyProperty(ctx, t, client, storeKey, recordKey, testPropertyName,
					tc.start, rr.GetSignature(), false)
			}
		})
	}

	// Error case.
	rr := setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key:        uuid.NewString(),
		Properties: map[string]*pb.Property{},
	})
	res, err := client.CompareAndSwap(ctx, &pb.CompareAndSwapRequest{
		StoreKey:     storeKey,
		RecordKey:    rr.Key,
		PropertyName: "non existent",
	})
	assert.Nil(t, res)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestOpenSaves_CompareAndSwapInt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	type C = pb.OpenSavesClient
	testCases := []struct {
		f           func(C, context.Context, *pb.AtomicIntRequest, ...grpc.CallOption) (*pb.AtomicIntResponse, error)
		start       int64
		value       int64
		wantUpdated bool
	}{
		{C.CompareAndSwapGreaterInt, 43, 42, false},
		{C.CompareAndSwapGreaterInt, 43, 43, false},
		{C.CompareAndSwapGreaterInt, 43, 44, true},
		{C.CompareAndSwapGreaterInt, 0, -1, false},
		{C.CompareAndSwapGreaterInt, -2, -1, true},
		{C.CompareAndSwapLessInt, 44, 45, false},
		{C.CompareAndSwapLessInt, 44, 44, false},
		{C.CompareAndSwapLessInt, 44, 43, true},
		{C.CompareAndSwapLessInt, 43, -2, true},
		{C.CompareAndSwapLessInt, -2, -1, false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%s start %v value %v", funcValueToName(tc.f), tc.start, tc.value), func(t *testing.T) {
			t.Parallel()
			const testPropertyName = "prop1"
			rr := &pb.Record{
				Key: uuid.NewString(),
				Properties: map[string]*pb.Property{
					testPropertyName: record.NewIntegerPropertyProto(tc.start),
				},
			}
			rr = setupTestRecord(ctx, t, client, storeKey, rr)
			res, err := tc.f(client, ctx, &pb.AtomicIntRequest{
				StoreKey:     storeKey,
				RecordKey:    rr.Key,
				PropertyName: testPropertyName,
				Value:        tc.value,
			})

			if assert.NoError(t, err) && assert.NotNil(t, res) {
				assert.Equal(t, tc.start, res.GetValue())
				assert.Equal(t, tc.wantUpdated, res.GetUpdated())
			}

			if tc.wantUpdated {
				verifyProperty(ctx, t, client, storeKey, rr.Key, testPropertyName,
					record.NewIntegerPropertyProto(tc.value), rr.GetSignature(), true)
			} else {
				verifyProperty(ctx, t, client, storeKey, rr.Key, testPropertyName,
					record.NewIntegerPropertyProto(tc.start), rr.GetSignature(), false)
			}
		})
	}
}

func TestOpenSaves_CompareAndSwapIntErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey,
		Properties: map[string]*pb.Property{
			"string": record.NewStringPropertyProto("Lorem ipsum"),
		},
	})

	// Check for type mismatch
	type C = pb.OpenSavesClient
	methods := []func(C, context.Context, *pb.AtomicIntRequest, ...grpc.CallOption) (*pb.AtomicIntResponse, error){
		C.CompareAndSwapGreaterInt,
		C.CompareAndSwapLessInt,
		C.AtomicAddInt,
		C.AtomicSubInt,
	}
	testCases := []struct {
		name string
		prop string
		want codes.Code
	}{
		{"Invalid Argument", "string", codes.InvalidArgument},
		{"Nonexistent", "nonexistent", codes.NotFound},
	}

	for _, method := range methods {
		method := method
		t.Run(funcValueToName(method), func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					res, err := method(client, ctx, &pb.AtomicIntRequest{
						StoreKey:     storeKey,
						RecordKey:    recordKey,
						PropertyName: tc.prop,
					})
					assert.Nil(t, res)
					assert.Equal(t, tc.want, status.Code(err))
				})
			}
		})
	}
}

func TestOpenSaves_AtomicAddSubInt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	type C = pb.OpenSavesClient
	testCases := []struct {
		f     func(C, context.Context, *pb.AtomicIntRequest, ...grpc.CallOption) (*pb.AtomicIntResponse, error)
		start int64
		value int64
		want  int64
	}{
		{C.AtomicAddInt, -1, 1, 0},
		{C.AtomicAddInt, 0, 1, 1},
		{C.AtomicAddInt, 1, -2, -1},

		{C.AtomicSubInt, 2, 1, 1},
		{C.AtomicSubInt, -1, 1, -2},
		{C.AtomicSubInt, -1, -3, 2},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%v start %v value %v", funcValueToName(tc.f), tc.start, tc.value), func(t *testing.T) {
			t.Parallel()
			const testPropertyName = "prop1"
			rr := &pb.Record{
				Key: uuid.NewString(),
				Properties: map[string]*pb.Property{
					testPropertyName: record.NewIntegerPropertyProto(tc.start),
				},
			}
			rr = setupTestRecord(ctx, t, client, storeKey, rr)
			res, err := tc.f(client, ctx, &pb.AtomicIntRequest{
				StoreKey:     storeKey,
				RecordKey:    rr.Key,
				PropertyName: testPropertyName,
				Value:        tc.value,
			})

			if assert.NoError(t, err) && assert.NotNil(t, res) {
				assert.Equal(t, tc.start, res.GetValue())
				assert.True(t, res.GetUpdated())
			}

			verifyProperty(ctx, t, client, storeKey, rr.Key, testPropertyName,
				record.NewIntegerPropertyProto(tc.want), rr.GetSignature(), true)
		})
	}
}

func TestOpenSaves_AtomicIncDecInt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	type C = pb.OpenSavesClient
	testCases := []struct {
		f     func(C, context.Context, *pb.AtomicIncRequest, ...grpc.CallOption) (*pb.AtomicIntResponse, error)
		start int64
		lower int64
		upper int64
		want  int64
	}{
		{C.AtomicInc, -1, 0, 2, 0},
		{C.AtomicInc, 0, 0, 2, 1},
		{C.AtomicInc, 1, 0, 2, 2},
		{C.AtomicInc, 2, 0, 2, 0},
		{C.AtomicInc, 3, 0, 2, 0},

		{C.AtomicDec, 0, -3, -1, -1},
		{C.AtomicDec, -1, -3, -1, -2},
		{C.AtomicDec, -2, -3, -1, -3},
		{C.AtomicDec, -3, -3, -1, -1},
		{C.AtomicDec, -4, -3, -1, -1},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%s start %v lower %v upper %v want %v",
			funcValueToName(tc.f), tc.start, tc.lower, tc.upper, tc.want), func(t *testing.T) {
			t.Parallel()

			const testPropertyName = "prop1"
			rr := setupTestRecord(ctx, t, client, storeKey, &pb.Record{
				Key: uuid.NewString(),
				Properties: map[string]*pb.Property{
					testPropertyName: record.NewIntegerPropertyProto(tc.start),
					"string":         record.NewStringPropertyProto("Lorem ipsum"),
				},
			})

			res, err := tc.f(client, ctx, &pb.AtomicIncRequest{
				StoreKey:     storeKey,
				RecordKey:    rr.Key,
				PropertyName: testPropertyName,
				LowerBound:   tc.lower,
				UpperBound:   tc.upper,
			})

			if assert.NotNil(t, res) && assert.NoError(t, err) {
				assert.True(t, res.GetUpdated())
				assert.Equal(t, tc.start, res.GetValue())
			}

			verifyProperty(ctx, t, client, storeKey, rr.Key, testPropertyName,
				record.NewIntegerPropertyProto(tc.want), rr.GetSignature(), true)
		})
	}
}

func TestOpenSaves_AtomicIncDecErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	storeKey := uuid.NewString()
	store := &pb.Store{Key: storeKey}
	setupTestStore(ctx, t, client, store)

	recordKey := uuid.NewString()
	setupTestRecord(ctx, t, client, storeKey, &pb.Record{
		Key: recordKey,
		Properties: map[string]*pb.Property{
			"string": record.NewStringPropertyProto("Lorem ipsum"),
		},
	})

	// Check for type mismatch
	testCases := []struct {
		name string
		prop string
		want codes.Code
	}{
		{"Invalid Argument", "string", codes.InvalidArgument},
		{"Nonexistent", "nonexistent", codes.NotFound},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, err := client.AtomicInc(ctx, &pb.AtomicIncRequest{
				StoreKey:     storeKey,
				RecordKey:    recordKey,
				PropertyName: tc.prop,
			})
			assert.Nil(t, res)
			assert.Equal(t, tc.want, status.Code(err))

			res, err = client.AtomicDec(ctx, &pb.AtomicIncRequest{
				StoreKey:     storeKey,
				RecordKey:    recordKey,
				PropertyName: tc.prop,
			})
			assert.Nil(t, res)
			assert.Equal(t, tc.want, status.Code(err))
		})
	}
}

func uploadChunk(ctx context.Context, t *testing.T, client pb.OpenSavesClient,
	sessionId string, number int64, content []byte) {
	t.Helper()

	digest := checksums.NewDigest()
	digest.Write(content)
	cs := digest.Checksums()

	ucc, err := client.UploadChunk(ctx)
	if err != nil {
		t.Errorf("CreateBlob returned error: %v", err)
		return
	}

	err = ucc.Send(&pb.UploadChunkRequest{
		Request: &pb.UploadChunkRequest_Metadata{
			Metadata: &pb.ChunkMetadata{
				SessionId: sessionId,
				Number:    number,
				Md5:       cs.MD5,
				Crc32C:    cs.GetCRC32C(),
				HasCrc32C: cs.HasCRC32C,
			},
		},
	})
	if err != nil {
		t.Errorf("UploadChunkClient.Send failed on sending metadata: %v", err)
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
		err = ucc.Send(&pb.UploadChunkRequest{
			Request: &pb.UploadChunkRequest_Content{
				Content: content[sent : sent+toSend],
			},
		})
		if err != nil {
			t.Errorf("CreateBlobClient.Send failed on sending content: %v", err)
		}
		sent += toSend
	}
	assert.Equal(t, len(content), sent)

	meta, err := ucc.CloseAndRecv()
	if err != nil {
		t.Errorf("CreateBlobClient.CloseAndRecv failed: %v", err)
		return
	}
	if assert.NotNil(t, meta) {
		assert.Equal(t, sessionId, meta.SessionId)
		assert.Equal(t, number, meta.Number)
		assert.Equal(t, int64(len(content)), meta.Size)
		// The server always sets the checksums regardless of what the client sends.
		assert.NotEmpty(t, meta.Md5)
		assert.True(t, meta.HasCrc32C)
	}
}

func verifyChunk(ctx context.Context, t *testing.T, client pb.OpenSavesClient,
	storeKey, recordKey string, sessionId string, number int64, expectedContent []byte) {
	t.Helper()
	gbc, err := client.GetBlobChunk(ctx, &pb.GetBlobChunkRequest{
		StoreKey:    storeKey,
		RecordKey:   recordKey,
		ChunkNumber: number,
	})
	if err != nil {
		t.Errorf("GetBlobChunk returned error: %v", err)
		return
	}
	res, err := gbc.Recv()
	if err != nil {
		t.Errorf("GetBlobChunkClient.Recv returned error: %v", err)
		return
	}
	meta := res.GetMetadata()
	if assert.NotNil(t, meta, "First returned message must be metadata") {
		assert.Equal(t, sessionId, meta.SessionId)
		assert.Equal(t, number, meta.Number)
		assert.Equal(t, int64(len(expectedContent)), meta.Size)

		if len(expectedContent) > 0 {
			digest := checksums.NewDigest()
			digest.Write(expectedContent)
			checksums := digest.Checksums()

			assert.Equal(t, checksums.MD5, meta.Md5)
			assert.True(t, meta.HasCrc32C)
			assert.Equal(t, checksums.GetCRC32C(), meta.Crc32C)
		} else {
			assert.Empty(t, meta.Md5)
			assert.False(t, meta.HasCrc32C)
		}
	}

	recvd := 0
	for {
		res, err = gbc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("GetBlobChunkClient.Recv returned error: %v", err)
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

func TestOpenSaves_UploadChunkedBlob(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	store := &pb.Store{Key: uuid.NewString()}
	setupTestStore(ctx, t, client, store)
	record := &pb.Record{Key: uuid.NewString()}
	record = setupTestRecord(ctx, t, client, store.Key, record)

	const chunkSize = 1*1024*1024 + 13 // 1 Mi + 13 B
	const chunkCount = 4
	testChunk := make([]byte, chunkSize)
	for i := 0; i < chunkSize; i++ {
		testChunk[i] = byte(i % 256)
	}

	beforeCreateChunk := time.Now()
	var sessionId string
	if res, err := client.CreateChunkedBlob(ctx, &pb.CreateChunkedBlobRequest{
		StoreKey:  store.Key,
		RecordKey: record.Key,
		ChunkSize: chunkSize,
	}); assert.NoError(t, err) {
		if assert.NotNil(t, res) {
			_, err := uuid.Parse(res.SessionId)
			assert.NoError(t, err)
			sessionId = res.SessionId
		}
	}
	t.Cleanup(func() {
		client.DeleteBlob(ctx, &pb.DeleteBlobRequest{StoreKey: store.Key, RecordKey: record.Key})
	})

	for i := 0; i < chunkCount; i++ {
		uploadChunk(ctx, t, client, sessionId, int64(i), testChunk)
		// UploadChunk shouldn't update Signature.
		if actual, err := client.GetRecord(ctx, &pb.GetRecordRequest{StoreKey: store.Key, Key: record.Key}); assert.NoError(t, err) {
			assert.Equal(t, record.Signature, actual.Signature)
		}
	}

	if meta, err := client.CommitChunkedUpload(ctx, &pb.CommitChunkedUploadRequest{
		SessionId: sessionId,
	}); assert.NoError(t, err) {
		assert.Equal(t, int64(len(testChunk)*chunkCount), meta.Size)
		assert.True(t, meta.Chunked)
		assert.Equal(t, int64(chunkCount), meta.ChunkCount)
		assert.False(t, meta.HasCrc32C)
		assert.Empty(t, meta.Md5)
		assert.Equal(t, store.Key, meta.StoreKey)
		assert.Equal(t, record.Key, meta.RecordKey)
	}

	// Check if the metadata is reflected to the record as well.
	if updatedRecord, err := client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.Key, Key: record.Key,
	}); assert.NoError(t, err) {
		if assert.NotNil(t, updatedRecord) {
			assert.Equal(t, int64(len(testChunk)*chunkCount), updatedRecord.BlobSize)
			assert.Equal(t, int64(chunkCount), updatedRecord.ChunkCount)
			assert.True(t, updatedRecord.Chunked)
			assert.True(t, record.GetCreatedAt().AsTime().Equal(updatedRecord.GetCreatedAt().AsTime()))
			assert.True(t, beforeCreateChunk.Before(updatedRecord.GetUpdatedAt().AsTime()))
			assert.NotEqual(t, record.Signature, updatedRecord.Signature)
		}
	}

	for i := 0; i < chunkCount; i++ {
		verifyChunk(ctx, t, client, store.Key, record.Key, sessionId, int64(i), testChunk)
	}

	// Deletion test
	if _, err := client.DeleteBlob(ctx, &pb.DeleteBlobRequest{
		StoreKey:  store.Key,
		RecordKey: record.Key,
	}); err != nil {
		t.Errorf("DeleteBlob failed: %v", err)
	}
	verifyBlob(ctx, t, client, store.Key, record.Key, make([]byte, 0))
}

func TestOpenSaves_UploadChunkedBlobWithChunkCount(t *testing.T) {
	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	store := &pb.Store{Key: uuid.NewString()}
	setupTestStore(ctx, t, client, store)
	record := &pb.Record{Key: uuid.NewString()}
	record = setupTestRecord(ctx, t, client, store.Key, record)

	const chunkSize = 1025
	const chunkCount = 4
	testChunk := make([]byte, chunkSize)
	for i := 0; i < chunkSize; i++ {
		testChunk[i] = byte(i % 256)
	}

	beforeCreateChunk := time.Now()
	var sessionId string

	if res, err := client.CreateChunkedBlob(ctx, &pb.CreateChunkedBlobRequest{
		StoreKey:   store.Key,
		RecordKey:  record.Key,
		ChunkSize:  chunkSize,
		ChunkCount: chunkCount,
	}); assert.NoError(t, err) {
		if assert.NotNil(t, res) {
			_, err := uuid.Parse(res.SessionId)
			assert.NoError(t, err)
			sessionId = res.SessionId
		}
	}
	t.Cleanup(func() {
		client.DeleteBlob(ctx, &pb.DeleteBlobRequest{StoreKey: store.Key, RecordKey: record.Key})
	})

	for i := 0; i < chunkCount-1; i++ {
		uploadChunk(ctx, t, client, sessionId, int64(i), testChunk)
		// UploadChunk shouldn't update Signature.
		if actual, err := client.GetRecord(ctx, &pb.GetRecordRequest{StoreKey: store.Key, Key: record.Key}); assert.NoError(t, err) {
			assert.Equal(t, record.Signature, actual.Signature)
		}
		b, err := client.CommitChunkedUpload(ctx, &pb.CommitChunkedUploadRequest{SessionId: sessionId})
		assert.Nil(t, b)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	}
	uploadChunk(ctx, t, client, sessionId, chunkCount-1, testChunk)

	if meta, err := client.CommitChunkedUpload(ctx, &pb.CommitChunkedUploadRequest{
		SessionId: sessionId,
	}); assert.NoError(t, err) {
		assert.Equal(t, int64(len(testChunk)*chunkCount), meta.Size)
		assert.True(t, meta.Chunked)
		assert.Equal(t, int64(chunkCount), meta.ChunkCount)
		assert.False(t, meta.HasCrc32C)
		assert.Empty(t, meta.Md5)
		assert.Equal(t, store.Key, meta.StoreKey)
		assert.Equal(t, record.Key, meta.RecordKey)
	}

	// Check if the metadata is reflected to the record as well.
	if updatedRecord, err := client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.Key, Key: record.Key,
	}); assert.NoError(t, err) {
		if assert.NotNil(t, updatedRecord) {
			assert.Equal(t, int64(len(testChunk)*chunkCount), updatedRecord.BlobSize)
			assert.Equal(t, int64(chunkCount), updatedRecord.ChunkCount)
			assert.True(t, updatedRecord.Chunked)
			assert.True(t, record.GetCreatedAt().AsTime().Equal(updatedRecord.GetCreatedAt().AsTime()))
			assert.True(t, beforeCreateChunk.Before(updatedRecord.GetUpdatedAt().AsTime()))
			assert.NotEqual(t, record.Signature, updatedRecord.Signature)
		}
	}

	for i := 0; i < chunkCount; i++ {
		verifyChunk(ctx, t, client, store.Key, record.Key, sessionId, int64(i), testChunk)
	}

	// Deletion test
	if _, err := client.DeleteBlob(ctx, &pb.DeleteBlobRequest{
		StoreKey:  store.Key,
		RecordKey: record.Key,
	}); err != nil {
		t.Errorf("DeleteBlob failed: %v", err)
	}
	verifyBlob(ctx, t, client, store.Key, record.Key, make([]byte, 0))
}

func TestOpenSaves_LongOpaqueStrings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, listener := getOpenSavesServer(ctx, t, "gcp")
	_, client := getTestClient(ctx, t, listener)
	store := &pb.Store{Key: uuid.NewString()}
	setupTestStore(ctx, t, client, store)

	const (
		// https://cloud.google.com/datastore/docs/concepts/limits
		maximumEntitySize   = 1_048_572
		maximumPropertySize = 1_048_487
	)

	buf := new(strings.Builder)
	buf.Grow(maximumEntitySize + 1)
	for i := 0; i < maximumEntitySize+1; i++ {
		buf.WriteByte(' ')
	}
	s := buf.String()
	passCases := []struct {
		len  int
		code codes.Code
	}{
		{maximumPropertySize - 1000, codes.OK}, // should be small enough
		{maximumPropertySize + 1, codes.InvalidArgument},
		{maximumEntitySize + 1, codes.InvalidArgument},
	}
	for _, tc := range passCases {
		tc := tc
		t.Run(fmt.Sprintf("%v bytes %v", tc.len, tc.code.String()), func(t *testing.T) {
			t.Parallel()
			dummyRec := &pb.Record{Key: uuid.NewString()}
			dummyRec = setupTestRecord(ctx, t, client, store.Key, dummyRec)

			// Creation test.
			res, err := tryCreateRecord(ctx, t, client, &pb.CreateRecordRequest{
				StoreKey: store.GetKey(),
				Record: &pb.Record{
					Key:          uuid.NewString(),
					OpaqueString: s[:tc.len],
				},
			})
			assert.Equal(t, tc.code, status.Code(err))
			if err == nil {
				if assert.NotNil(t, res) {
					assert.Equal(t, s[:tc.len], res.GetOpaqueString())
				}
			}

			// Update test.
			res, err = client.UpdateRecord(ctx, &pb.UpdateRecordRequest{
				StoreKey: store.Key,
				Record: &pb.Record{
					Key:          dummyRec.Key,
					OpaqueString: s[:tc.len],
				},
			})
			assert.Equal(t, tc.code, status.Code(err))
			if err == nil {
				if assert.NotNil(t, res) {
					assert.Equal(t, s[:tc.len], res.GetOpaqueString())
				}
			}
		})
	}
}
