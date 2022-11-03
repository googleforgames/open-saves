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

package metadb_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums/checksumstest"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	blobKind      = "blob"
	chunkKind     = "chunk"
	testProject   = "triton-for-games-dev"
	testNamespace = "datastore-unittests"
)

func TestMetaDB_NewMetaDB(t *testing.T) {
	ctx := context.Background()
	metaDB, err := m.NewMetaDB(ctx, testProject)
	assert.NotNil(t, metaDB, "NewMetaDB() should return a non-nil instance.")
	assert.NoError(t, err, "NewMetaDB should succeed.")
}

const testTimestampThreshold = 30 * time.Second

func newDatastoreClient(ctx context.Context, t *testing.T) *datastore.Client {
	client, err := datastore.NewClient(ctx, testProject)
	if err != nil {
		t.Fatalf("datastore.NewClient() failed: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
	})
	return client
}

func newMetaDB(ctx context.Context, t *testing.T) *m.MetaDB {
	t.Helper()
	metaDB, err := m.NewMetaDB(ctx, testProject)
	if err != nil {
		t.Fatalf("Initializing MetaDB: %v", err)
	}
	metaDB.Namespace = testNamespace
	t.Cleanup(func() { metaDB.Disconnect(ctx) })
	return metaDB
}

func newStoreKey() string {
	return uuid.NewString() + "_unittest_store"
}

func newRecordKey() string {
	return uuid.NewString() + "_unittest_record"
}

func cloneRecord(r *record.Record) *record.Record {
	if r == nil {
		return nil
	}
	ret := *r
	ret.Blob = append([]byte{}, r.Blob...)
	ret.Properties = make(record.PropertyMap)
	for k, v := range r.Properties {
		ret.Properties[k] = v
	}
	ret.Tags = append([]string{}, r.Tags...)
	return &ret
}

func blobRefKey(key uuid.UUID) *datastore.Key {
	dsKey := datastore.NameKey(blobKind, key.String(), nil)
	dsKey.Namespace = testNamespace
	return dsKey
}

func chunkRefKey(blobKey, key uuid.UUID) *datastore.Key {
	dsKey := datastore.NameKey(chunkKind, key.String(), blobRefKey(blobKey))
	dsKey.Namespace = testNamespace
	return dsKey
}

// setupTestStoreRecord creates a new store and inserts a record into it, then registers
// cleanup functions to delete these test store and record.
// Passing a nil to record will skip the record insertion.
func setupTestStoreRecord(ctx context.Context, t *testing.T, metaDB *m.MetaDB, store *store.Store, r *record.Record) (*store.Store, *record.Record) {
	t.Helper()
	gotStore, err := metaDB.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	t.Cleanup(func() {
		metaDB.DeleteStore(ctx, gotStore.Key)
	})
	if diff := cmp.Diff(store, gotStore); diff != "" {
		t.Errorf("CreateStore() = (-want, +got):\n%s", diff)
	}

	var gotRecord *record.Record
	if r != nil {
		gotRecord = setupTestRecord(ctx, t, metaDB, gotStore.Key, r)
	}
	return gotStore, gotRecord
}

func setupTestRecord(ctx context.Context, t *testing.T, metaDB *m.MetaDB, storeKey string, r *record.Record) *record.Record {
	t.Helper()

	got, err := metaDB.InsertRecord(ctx, storeKey, r)
	require.NoError(t, err, "InsertRecord")
	t.Cleanup(func() {
		metaDB.DeleteRecord(ctx, storeKey, got.Key)
	})
	if diff := cmp.Diff(r, got); diff != "" {
		t.Errorf("InsertRecord() = (-want, +got):\n%s", diff)
	}
	return got
}

func setupTestBlobRef(ctx context.Context, t *testing.T, metaDB *m.MetaDB, blob *blobref.BlobRef) *blobref.BlobRef {
	t.Helper()
	got, err := metaDB.InsertBlobRef(ctx, blob)
	if err != nil {
		t.Fatalf("InsertBlobRef() failed: %v", err)
	}
	if diff := cmp.Diff(blob, got); diff != "" {
		t.Errorf("InsertBlobRef() = (-want, +got):\n%s", diff)
	}

	t.Cleanup(func() {
		// Call the Datastore method directly to avoid Status checking
		key := blobRefKey(blob.Key)
		newDatastoreClient(ctx, t).Delete(ctx, key)
	})
	return got
}

func setupTestChunkRef(ctx context.Context, t *testing.T, metaDB *m.MetaDB, blob *blobref.BlobRef, chunk *chunkref.ChunkRef) {
	t.Helper()
	if err := metaDB.InsertChunkRef(ctx, blob, chunk); err != nil {
		t.Fatalf("InsertChunkRef() failed for chunk key (%v): %v", chunk.Key, err)
	}

	t.Cleanup(func() {
		// Call the Datastore functions directly to avoid Status checks
		newDatastoreClient(ctx, t).Delete(ctx, chunkRefKey(chunk.BlobRef, chunk.Key))
	})
}

func setupTestStoreRecordBlobSet(ctx context.Context, t *testing.T, metaDB *m.MetaDB, chunked bool) (*store.Store, *record.Record, *blobref.BlobRef) {
	t.Helper()

	storeKey := newStoreKey()
	store := &store.Store{
		Key:  storeKey,
		Name: t.Name(),
	}

	recordKey := newRecordKey()
	record := &record.Record{
		Key:          recordKey,
		OpaqueString: t.Name(),
		Properties:   make(record.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, metaDB, store, record)

	blobKey := uuid.New()
	blob := &blobref.BlobRef{
		Key:       blobKey,
		Status:    blobref.StatusInitializing,
		StoreKey:  storeKey,
		RecordKey: recordKey,
		Chunked:   chunked,
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Unix(123, 0),
			UpdatedAt: time.Unix(123, 0),
		},
	}
	setupTestBlobRef(ctx, t, metaDB, blob)
	return store, record, blob
}

func TestMetaDB_Disconnect(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	if err := metaDB.Disconnect(ctx); err != nil {
		t.Fatalf("Failed to disconnect: %v", err)
	}
}

func TestMetaDB_SimpleCreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	storeKey := newStoreKey()
	storeName := "SimpleCreateGetDeleteStore" + uuid.NewString()
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	store := &store.Store{
		Key:     storeKey,
		Name:    storeName,
		OwnerID: "owner",
		Tags:    []string{"abc", "def"},
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("db94be80-e036-4ca8-a9c0-2259b8a67acc"),
		},
	}
	createdStore, err := metaDB.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	if diff := cmp.Diff(store, createdStore); diff != "" {
		t.Errorf("CreateStore() = (-want, +got):\n%s", diff)
	}

	store2, err := metaDB.GetStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Could not get store (%s): %v", storeKey, err)
	}
	if diff := cmp.Diff(store, store2); diff != "" {
		t.Errorf("GetStore() = (-want, +got):\n%s", diff)
	}

	store3, err := metaDB.FindStoreByName(ctx, storeName)
	if err != nil {
		t.Fatalf("Could not fetch store by name (%s): %v", storeName, err)
	}
	if diff := cmp.Diff(store, store3); diff != "" {
		t.Errorf("FindStoreByName() = (-want, +got):\n%s", diff)
	}

	err = metaDB.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
	store4, err := metaDB.GetStore(ctx, storeKey)
	if err == nil {
		t.Fatalf("GetStore didn't return an error after deleting a store: %v", err)
	}
	assert.Nil(t, store4, "GetStore should return a nil store.")
}

func TestMetaDB_SimpleCreateGetDeleteRecord(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	storeKey := newStoreKey()
	store := &store.Store{
		Key:     storeKey,
		Name:    "SimpleCreateGetDeleteRecord",
		OwnerID: "owner",
	}
	setupTestStoreRecord(ctx, t, metaDB, store, nil)

	recordKey := newRecordKey()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	record := &record.Record{
		Key:      recordKey,
		Blob:     blob,
		BlobSize: int64(len(blob)),
		OwnerID:  "record owner",
		Tags:     []string{"abc", "def"},
		Properties: record.PropertyMap{
			"BoolTP":   {Type: pb.Property_BOOLEAN, BooleanValue: false},
			"IntTP":    {Type: pb.Property_INTEGER, IntegerValue: 42},
			"StringTP": {Type: pb.Property_STRING, StringValue: "a string value"},
		},
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("89223949-0414-438e-8f5e-3fd9e2d11c1e"),
		},
	}
	expected := cloneRecord(record)
	expected.StoreKey = storeKey

	createdRecord, err := metaDB.InsertRecord(ctx, storeKey, record)
	expected.Timestamps = timestamps.New()
	// Copy the new signature as we cannot generate the same UUID.
	expected.Timestamps.Signature = createdRecord.Timestamps.Signature
	if err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	if diff := cmp.Diff(expected, createdRecord, cmpopts.EquateApproxTime(testTimestampThreshold)); diff != "" {
		t.Errorf("InsertRecord() = (-want, +got):\n%s", diff)
	}

	// Use the updated timestamps for the subsequent checks
	expected.Timestamps = createdRecord.Timestamps
	record2, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	if diff := cmp.Diff(expected, record2, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("GetRecord() = (-want, +got):\n%s", diff)
	}

	record3, err := metaDB.InsertRecord(ctx, storeKey, record)
	assert.NotNil(t, err, "Insert should fail if a record with the same already exists.")
	assert.Nil(t, record3, "Insert should fail and return nil.")

	err = metaDB.DeleteRecord(ctx, storeKey, recordKey)
	assert.Nilf(t, err, "Failed to delete a record (%s) in store (%s): %v", recordKey, storeKey, err)

	record4, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	assert.NotNilf(t, err, "GetRecord didn't return an error after deleting a record (%s)", recordKey)
	assert.Nil(t, record4, "GetRecord should return a nil with error")

	// The test store is deleted inside the cleanup function.
}

func TestMetaDB_InsertRecordShouldFailWithNonExistentStore(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	storeKey := newStoreKey()
	record := &record.Record{
		Key: newRecordKey(),
	}
	record1, err := metaDB.InsertRecord(ctx, storeKey, record)
	if err == nil {
		t.Error("InsertRecord should fail if the store doesn't exist.")
		metaDB.DeleteRecord(ctx, storeKey, record.Key)
	} else {
		assert.Equalf(t, codes.FailedPrecondition, status.Code(err),
			"InsertRecord should return FailedPrecondition if the store doesn't exist: %v", err)
	}
	assert.Nil(t, record1, "InsertRecord should return nil with error")
}

func TestMetaDB_DeleteStoreShouldFailWhenNotEmpty(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	store := &store.Store{Key: newStoreKey()}
	record := &record.Record{Key: newRecordKey(), Properties: make(record.PropertyMap)}

	setupTestStoreRecord(ctx, t, metaDB, store, record)

	if err := metaDB.DeleteStore(ctx, store.Key); err == nil {
		t.Error("DeleteStore should fail if the store is not empty.")
	} else {
		assert.Equalf(t, codes.FailedPrecondition, status.Code(err),
			"DeleteStore should return FailedPrecondition if the store is not empty: %v", err)
	}
}

func TestMetaDB_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	storeKey := newStoreKey()
	store := &store.Store{
		Key:     storeKey,
		Name:    "UpdateRecord",
		OwnerID: "owner",
	}

	recordKey := newRecordKey()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	work := &record.Record{
		Key:        recordKey,
		Blob:       blob,
		BlobSize:   int64(len(blob)),
		Properties: make(record.PropertyMap),
		OwnerID:    "record owner",
		Tags:       []string{"abc", "def"},
	}
	work.Timestamps = timestamps.New()
	expected := cloneRecord(work)
	expected.StoreKey = storeKey

	updated, err := metaDB.UpdateRecord(ctx, storeKey, recordKey,
		func(*record.Record) (*record.Record, error) { return nil, nil })
	assert.NotNil(t, err, "UpdateRecord should return an error if the specified record doesn't exist.")
	assert.Nil(t, updated, "UpdateRecord should return nil with error")

	setupTestStoreRecord(ctx, t, metaDB, store, work)

	work.Tags = append(work.Tags, "ghi")
	expected.Tags = append(expected.Tags, "ghi")
	work.OwnerID = "NewOwner"
	expected.OwnerID = work.OwnerID
	expected.Timestamps = timestamps.New()

	// Make sure UpdateRecord doesn't update CreatedAt
	work.Timestamps.CreatedAt = time.Unix(0, 0)
	updated, err = metaDB.UpdateRecord(ctx, storeKey, recordKey,
		func(r *record.Record) (*record.Record, error) {
			r.OwnerID = work.OwnerID
			r.Tags = work.Tags
			return r, nil
		})
	assert.Nilf(t, err, "Failed to update a record (%s) in store (%s): %v", recordKey, storeKey, err)
	// Make sure the signatures are different before passing to AssertEqualRecordWithinDuration
	assert.NotEqual(t, expected.Timestamps.Signature, updated.Timestamps.Signature)
	expected.Timestamps.Signature = updated.Timestamps.Signature
	if diff := cmp.Diff(expected, updated,
		cmpopts.EquateApproxTime(testTimestampThreshold),
		cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("UpdateRecord() = (-want, +got):\n%s", diff)
	}

	stored, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	if diff := cmp.Diff(expected, stored,
		cmpopts.EquateApproxTime(testTimestampThreshold),
		cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("GetRecord() = (-want, +got):\n%s", diff)
	}
}

func TestMetaDB_UpdateRecordErrNoUpdate(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	store := &store.Store{
		Key:  newStoreKey(),
		Name: t.Name(),
	}
	rr := &record.Record{
		Key:          newRecordKey(),
		OpaqueString: t.Name(),
		Properties:   make(record.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, metaDB, store, rr)

	_, err := metaDB.UpdateRecord(ctx, store.Key, rr.Key,
		func(toUpdate *record.Record) (*record.Record, error) {
			return toUpdate, m.ErrNoUpdate
		})
	assert.NoError(t, err)

	// Check if timestamps are not updated.
	got, err := metaDB.GetRecord(ctx, store.Key, rr.Key)
	if assert.NoError(t, err) {
		assert.True(t, rr.Timestamps.UpdatedAt.Equal(got.Timestamps.UpdatedAt))
		assert.Equal(t, rr.Timestamps.Signature, got.Timestamps.Signature)
	}
}

func TestMetaDB_DeleteShouldNotFailWithNonExistentKey(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	storeKey := newStoreKey()
	if err := metaDB.DeleteStore(ctx, storeKey); err != nil {
		t.Fatalf("DeleteStore() failed with a non-existent key: %v", err)
	}
	recordKey := newRecordKey()
	if err := metaDB.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Fatalf("DeleteRecord() failed with a non-existent key: %v", err)
	}
}

func TestMetaDB_SimpleCreateGetDeleteBlobRef(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	storeKey := newStoreKey()
	store := &store.Store{
		Key:  storeKey,
		Name: "SimpleCreateGetDeleteBlobRef",
	}
	testBlob := []byte{1, 2, 3, 4, 5}

	recordKey := newRecordKey()
	createdAt := time.Unix(12345, 0)
	record := &record.Record{
		Key:        recordKey,
		Blob:       testBlob,
		BlobSize:   int64(len(testBlob)),
		Properties: make(record.PropertyMap),
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.New(),
		},
	}

	setupTestStoreRecord(ctx, t, metaDB, store, record)
	blobKey := uuid.New()
	origSig := uuid.New()
	blob := &blobref.BlobRef{
		Key:       blobKey,
		Size:      12345,
		Status:    blobref.StatusInitializing,
		StoreKey:  storeKey,
		RecordKey: recordKey,
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: origSig,
		},
	}

	setupTestBlobRef(ctx, t, metaDB, blob)

	_, err := metaDB.GetCurrentBlobRef(ctx, storeKey, recordKey)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	blob2, err := metaDB.GetBlobRef(ctx, blobKey)
	if err != nil {
		t.Errorf("GetBlobRef() failed: %v", err)
	} else {
		if diff := cmp.Diff(blob, blob2, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("GetBlobRef() = (-want, +got):\n%s", diff)
		}
	}

	beforePromo := time.Now()
	promoRecord, promoBlob, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobAsCurrent() failed: %v", err)
	} else {
		if assert.NotNil(t, promoRecord) {
			assert.Nil(t, promoRecord.Blob)
			assert.Equal(t, blobKey, promoRecord.ExternalBlob)
			assert.Equal(t, blob.Size, promoRecord.BlobSize)
			assert.False(t, promoRecord.Chunked)
			assert.Zero(t, promoRecord.ChunkCount)
			assert.True(t, beforePromo.Before(promoRecord.Timestamps.UpdatedAt))
			assert.NotEqual(t, record.Timestamps.Signature, promoRecord.Timestamps.Signature)
		}
		if assert.NotNil(t, promoBlob) {
			assert.Equal(t, blobKey, promoBlob.Key)
			assert.Equal(t, blobref.StatusReady, promoBlob.Status)
			assert.False(t, promoBlob.Chunked)
			assert.Zero(t, promoBlob.ChunkCount)
			assert.True(t, beforePromo.Before(promoBlob.Timestamps.UpdatedAt))
			assert.NotEqual(t, origSig, promoBlob.Timestamps.Signature)
		}
	}

	currentBlob, err := metaDB.GetCurrentBlobRef(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("GetCurrentBlobRef() failed: %v", err)
	} else {
		if diff := cmp.Diff(promoBlob, currentBlob,
			cmpopts.EquateEmpty(),
			cmpopts.EquateApproxTime(timestamps.Precision)); diff != "" {
			t.Errorf("GetCurrentBlobRef() = (-want, +got):\n%s", diff)
		}
	}

	if err := metaDB.DeleteBlobRef(ctx, blobKey); err == nil {
		t.Errorf("DeleteBlobRef should fail on current blob: %v", err)
	} else {
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	}

	delPendRecord, delPendBlob, err := metaDB.RemoveBlobFromRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("RemoveBlobFromRecord() failed: %v", err)
	} else {
		if assert.NotNil(t, delPendRecord) {
			assert.Equal(t, uuid.Nil, delPendRecord.ExternalBlob)
			assert.Empty(t, delPendRecord.Blob)
			assert.Zero(t, delPendRecord.BlobSize)
			assert.False(t, delPendRecord.Chunked)
			assert.Zero(t, delPendRecord.ChunkCount)
		}
		if assert.NotNil(t, delPendBlob) {
			assert.Equal(t, blobref.StatusPendingDeletion, delPendBlob.Status)
		}
	}

	assert.NoError(t, metaDB.DeleteBlobRef(ctx, blobKey))

	deletedBlob, err := metaDB.GetBlobRef(ctx, blobKey)
	assert.Nil(t, deletedBlob)
	assert.Equal(t, codes.NotFound, status.Code(err), "GetBlobRef should return NotFound after deletion.")
}

func TestMetaDB_SwapBlobRefs(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	store := &store.Store{
		Key:  newStoreKey(),
		Name: "SwapBlobRefs",
	}

	record := &record.Record{
		Key:        newRecordKey(),
		Properties: make(record.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, metaDB, store, record)

	blob := &blobref.BlobRef{
		Key:       uuid.New(),
		Status:    blobref.StatusInitializing,
		StoreKey:  store.Key,
		RecordKey: record.Key,
	}
	setupTestBlobRef(ctx, t, metaDB, blob)

	_, blob, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobRefToCurrent() failed: %v", err)
	}

	newBlob := &blobref.BlobRef{
		Key:       uuid.New(),
		Status:    blobref.StatusInitializing,
		StoreKey:  store.Key,
		RecordKey: record.Key,
	}
	setupTestBlobRef(ctx, t, metaDB, newBlob)

	record, newCurrBlob, err := metaDB.PromoteBlobRefToCurrent(ctx, newBlob)
	if assert.NoError(t, err) {
		if assert.NotNil(t, record) {
			assert.Equal(t, newBlob.Key, record.ExternalBlob)
		}
		if assert.NotNil(t, newCurrBlob) {
			assert.Equal(t, newBlob.Key, newCurrBlob.Key)
			assert.Equal(t, blobref.StatusReady, newCurrBlob.Status)
		}
	}

	oldBlob, err := metaDB.GetBlobRef(ctx, blob.Key)
	if assert.NoError(t, err) {
		if assert.NotNil(t, oldBlob) {
			assert.Equal(t, blob.Key, oldBlob.Key)
			assert.Equal(t, blobref.StatusPendingDeletion, oldBlob.Status)
		}
	}

	_, _, err = metaDB.RemoveBlobFromRecord(ctx, store.Key, record.Key)
	assert.NoError(t, err)
}

func TestMetaDB_UpdateBlobRef(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	store := &store.Store{
		Key:  newStoreKey(),
		Name: "SimpleCreateGetDeleteBlobRef",
	}

	record := &record.Record{
		Key:        newRecordKey(),
		Properties: make(record.PropertyMap),
	}

	setupTestStoreRecord(ctx, t, metaDB, store, record)
	blob := &blobref.BlobRef{
		Key:       uuid.New(),
		Status:    blobref.StatusInitializing,
		StoreKey:  store.Key,
		RecordKey: record.Key,
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Unix(123, 0),
			UpdatedAt: time.Unix(123, 0),
		},
	}

	setupTestBlobRef(ctx, t, metaDB, blob)

	blob.Fail()
	blob.Timestamps.UpdatedAt = time.Unix(234, 0)

	updatedBlob, err := metaDB.UpdateBlobRef(ctx, blob)
	if err != nil {
		t.Errorf("UpdateBlobRef() failed: %v", err)
	} else {
		if diff := cmp.Diff(blob, updatedBlob); diff != "" {
			t.Errorf("UpdateBlobRef() = (-want, +got):\n%s", diff)
		}
	}

	receivedBlob, err := metaDB.GetBlobRef(ctx, blob.Key)
	if err != nil {
		t.Errorf("GetBlobRef() failed: %v", err)
	} else {
		if diff := cmp.Diff(blob, receivedBlob, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("GetBlobRef() = (-want, +got):\n%s", diff)
		}
	}
}

func TestMetaDB_BlobInsertShouldFailForNonexistentRecord(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	blob := &blobref.BlobRef{
		Key:       uuid.New(),
		StoreKey:  "non-existent" + uuid.NewString(),
		RecordKey: "non-existent" + uuid.NewString(),
	}

	insertedBlob, err := metaDB.InsertBlobRef(ctx, blob)
	if err == nil {
		t.Error("InsertBlobRef should fail for a non-existent record.")
		assert.NoError(t, metaDB.DeleteBlobRef(ctx, blob.Key))
	} else {
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Nil(t, insertedBlob)
	}
}

func TestMetaDB_UpdateRecordWithExternalBlobs(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	storeKey := newStoreKey()
	store := &store.Store{
		Key: storeKey,
	}

	recordKey := newRecordKey()
	origRecord := &record.Record{
		Key:        recordKey,
		Properties: make(record.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, metaDB, store, origRecord)

	blobKey := uuid.New()
	blob := &blobref.BlobRef{
		Key:       blobKey,
		Status:    blobref.StatusInitializing,
		StoreKey:  storeKey,
		RecordKey: recordKey,
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Unix(123, 0),
			UpdatedAt: time.Unix(123, 0),
		},
	}
	setupTestBlobRef(ctx, t, metaDB, blob)

	_, blob, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobRefToCurrent() failed: %v", err)
	}

	_, err = metaDB.UpdateRecord(ctx, storeKey, recordKey, func(record *record.Record) (*record.Record, error) {
		record.ExternalBlob = uuid.New()
		return record, nil
	})
	if assert.Error(t, err, "UpdateRecord should fail when ExternalBlob is modified") {
		assert.Equal(t, codes.Internal, status.Code(err))
	}

	// Make sure ExternalBlob is preserved when not modified
	newRecord, err := metaDB.UpdateRecord(ctx, storeKey, recordKey, func(record *record.Record) (*record.Record, error) {
		// noop
		return record, nil
	})

	if assert.NoError(t, err) {
		if assert.NotNil(t, newRecord) {
			assert.Equal(t, blobKey, newRecord.ExternalBlob)
		}
	}

	actual, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	if assert.NoError(t, err) {
		if assert.NotNil(t, actual) {
			assert.Equal(t, blobKey, actual.ExternalBlob)
		}
	}

	// Check if ExternalBlob is marked for deletion
	testBlob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	newRecord, err = metaDB.UpdateRecord(ctx, storeKey, recordKey, func(record *record.Record) (*record.Record, error) {
		record.Blob = testBlob
		record.BlobSize = int64(len(testBlob))
		return record, nil
	})
	if assert.NoError(t, err) {
		if assert.NotNil(t, newRecord) {
			assert.Equal(t, testBlob, newRecord.Blob)
			assert.Equal(t, int64(len(testBlob)), newRecord.BlobSize)
			assert.Equal(t, uuid.Nil, newRecord.ExternalBlob)
		}
	}

	actual, err = metaDB.GetRecord(ctx, storeKey, recordKey)
	if assert.NoError(t, err) {
		if assert.NotNil(t, actual) {
			assert.Equal(t, testBlob, actual.Blob)
			assert.Equal(t, int64(len(testBlob)), actual.BlobSize)
			assert.Equal(t, uuid.Nil, actual.ExternalBlob)
		}
	}

	blob, err = metaDB.GetBlobRef(ctx, blob.Key)
	if assert.NoError(t, err) {
		if assert.NotNil(t, blob) {
			assert.Equal(t, blobref.StatusPendingDeletion, blob.Status)
		}
	}
}

func TestMetaDB_ListBlobsByStatus(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	client := newDatastoreClient(ctx, t)

	storeKey := newStoreKey()
	store := &store.Store{
		Key: storeKey,
	}

	recordKey := newRecordKey()
	record := &record.Record{
		Key:        recordKey,
		Properties: make(record.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, metaDB, store, record)

	statuses := []blobref.Status{
		blobref.StatusError,
		blobref.StatusInitializing,
		blobref.StatusPendingDeletion,
		blobref.StatusPendingDeletion}
	blobs := []*blobref.BlobRef{}
	for _, s := range statuses {
		blob := &blobref.BlobRef{
			Key:       uuid.New(),
			Status:    s,
			StoreKey:  storeKey,
			RecordKey: recordKey,
		}
		blobs = append(blobs, blob)
		setupTestBlobRef(ctx, t, metaDB, blob)

		bKey := blobRefKey(blob.Key)
		if _, err := client.Put(ctx, bKey, blob); err != nil {
			t.Fatalf("Failed to change UpdatedAt for %v: %v", blob.Key, err)
		}
	}

	// Should return iterator.Done and nil when not found
	iter, err := metaDB.ListBlobRefsByStatus(ctx, blobref.StatusUnknown)
	assert.NoError(t, err)
	if assert.NotNil(t, iter) {
		b, err := iter.Next()
		assert.Equal(t, iterator.Done, err)
		assert.Nil(t, b)
	}

	iter, err = metaDB.ListBlobRefsByStatus(ctx, blobref.StatusPendingDeletion)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, err := iter.Next()
	for err == nil {
		if got.Status != blobref.StatusPendingDeletion {
			t.Fatalf("got status %v, want %v", got.Status, blobref.StatusPendingDeletion)
		}
		got, err = iter.Next()
	}
}

func TestMetaDB_DeleteRecordWithExternalBlob(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	storeKey := newStoreKey()
	store := &store.Store{
		Key:  storeKey,
		Name: t.Name(),
	}

	recordKey := newRecordKey()
	record := &record.Record{
		Key:        recordKey,
		Tags:       []string{t.Name()},
		Properties: make(record.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, metaDB, store, record)

	blob := blobref.NewBlobRef(0, storeKey, recordKey)
	setupTestBlobRef(ctx, t, metaDB, blob)

	_, blob, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobRefToCurrent() failed: %v", err)
	}

	if err := metaDB.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Errorf("DeleteRecord() failed: %v", err)
	}

	actual, err := metaDB.GetBlobRef(ctx, blob.Key)
	assert.NoError(t, err, "GetBlobRef should not return error")
	if assert.NotNil(t, actual) {
		assert.Equal(t, blobref.StatusPendingDeletion, actual.Status)
	}
}

// This case tests if DeleteRecord deletes a record anyway when
// the associated BlobRef does not exist and the database is inconsistent.
func TestMetaDB_DeleteRecordWithNonExistentBlobRef(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	storeKey := newStoreKey()
	store := &store.Store{
		Key:  storeKey,
		Name: t.Name(),
	}

	recordKey := newRecordKey()
	record := &record.Record{
		Key:          recordKey,
		ExternalBlob: uuid.New(),
		Tags:         []string{t.Name()},
		Properties:   make(record.PropertyMap),
	}

	setupTestStoreRecord(ctx, t, metaDB, store, record)

	actualBlob, err := metaDB.GetBlobRef(ctx, record.ExternalBlob)
	assert.Nil(t, actualBlob)
	if assert.Error(t, err, "GetBlobRef should return error when BlobRef doesn't exist.") {
		assert.Equal(t, codes.NotFound, status.Code(err),
			"GetBlobRef should return NotFound when BlobRef doesn't exist.")
	}

	assert.NotEqual(t, uuid.Nil, record.ExternalBlob)
	assert.NoError(t, metaDB.DeleteRecord(ctx, storeKey, recordKey),
		"DeleteRecord should succeed even if ExternalBlob doesn't exist.")

	actualRecord, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	assert.Nil(t, actualRecord)
	if assert.Error(t, err, "GetRecord should return error after DeleteRecord") {
		assert.Equal(t, codes.NotFound, status.Code(err),
			"GetRecord should return NotFound after DeleteRecord")
	}
}

func TestMetaDB_SimpleCreateGetDeleteChunkedBlob(t *testing.T) {
	const (
		testChunkCount = 3
		testChunkSize  = int32(42)
	)

	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	ds := newDatastoreClient(ctx, t)

	store, _, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	// Create Chunks with Initializing state
	chunks := []*chunkref.ChunkRef{}
	for i := 0; i < testChunkCount; i++ {
		chunk := chunkref.New(blob.Key, int32(i))
		chunk.Size = testChunkSize
		if err := chunk.Ready(); err != nil {
			t.Fatalf("Ready() failed for chunk (%v): %v", chunk.Key, err)
		}
		setupTestChunkRef(ctx, t, metaDB, blob, chunk)
		chunks = append(chunks, chunk)
	}

	// Mark the chunks ready
	for _, chunk := range chunks {
		dsKey := chunkRefKey(chunk.BlobRef, chunk.Key)
		got := new(chunkref.ChunkRef)
		if err := ds.Get(ctx, dsKey, got); err != nil {
			t.Errorf("Failed to get updated ChunkRef (%v): %v", dsKey, err)
		} else {
			assert.Equal(t, blobref.StatusReady, got.Status)
		}
	}
	record, blobRetrieved, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Fatalf("PromoteBlobRefToCurrent() failed: %v", err)
	}
	if assert.NotNil(t, record) {
		assert.EqualValues(t, testChunkSize*testChunkCount, record.BlobSize)
		assert.EqualValues(t, testChunkCount, record.ChunkCount)
		assert.True(t, record.Chunked)
	}
	if assert.NotNil(t, blobRetrieved) {
		assert.EqualValues(t, testChunkSize*testChunkCount, blobRetrieved.Size)
		assert.True(t, blobRetrieved.Chunked)
		assert.Equal(t, int64(testChunkCount), blobRetrieved.ChunkCount)
	}

	// Verify blob and chunk metadata
	if blobRetrieved, err := metaDB.GetCurrentBlobRef(ctx, store.Key, record.Key); err != nil {
		t.Errorf("GetCurrentBlobRef() failed: %v", err)
	} else {
		assert.EqualValues(t, testChunkSize*testChunkCount, blobRetrieved.Size)
		assert.Equal(t, blob.Key, blobRetrieved.Key)
		assert.True(t, blobRetrieved.Chunked)
		assert.Equal(t, int64(testChunkCount), blobRetrieved.ChunkCount)
	}
	for i := 0; i < testChunkCount; i++ {
		got, err := metaDB.FindChunkRefByNumber(ctx, store.Key, record.Key, int32(i))
		if err != nil {
			t.Errorf("FindChunkRefByNumber(%d) failed: %v", i, err)
		}
		if diff := cmp.Diff(chunks[i], got, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("FindChunkRefByNumber(%d) = (-want, +got):\n%s", i, diff)
		}
	}

	// Delete chunks and blob
	if record, _, err := metaDB.RemoveBlobFromRecord(ctx, store.Key, record.Key); err != nil {
		t.Fatalf("RemoveBlobFromRecord should work with Chunked blobs")
	} else {
		if assert.NotNil(t, record) {
			assert.Zero(t, record.BlobSize)
			assert.False(t, record.Chunked)
			assert.Zero(t, record.ChunkCount)
		}
	}

	// Check if child chunks are marked as well
	for _, chunk := range chunks {
		got := new(chunkref.ChunkRef)
		if err := ds.Get(ctx, chunkRefKey(chunk.BlobRef, chunk.Key), got); err != nil {
			t.Errorf("Couldn't get chunk (%v) after RemoveBlobFromRecord: %v", chunk.Key, err)
		} else {
			assert.Equal(t, chunk.Key, got.Key)
			assert.True(t, got.Timestamps.UpdatedAt.After(chunk.Timestamps.UpdatedAt))
			assert.Equal(t, blobref.StatusPendingDeletion, got.Status)
		}
	}

	if err := metaDB.DeleteBlobRef(ctx, blob.Key); err != nil {
		t.Fatalf("DeleteBlobRef() failed: %v", err)
	}

	for _, chunk := range chunks {
		dsKey := chunkRefKey(chunk.BlobRef, chunk.Key)
		got := new(chunkref.ChunkRef)
		if err := ds.Get(ctx, dsKey, got); err != datastore.ErrNoSuchEntity {
			t.Errorf("ChunkRef was not deleted for key (%v): %v", chunk.Key, err)
		}
	}
}

func TestMetaDB_MarkUncommittedBlobForDeletion(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	store, record, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	chunk := chunkref.New(blob.Key, 0)
	setupTestChunkRef(ctx, t, metaDB, blob, chunk)

	assert.NoError(t, metaDB.MarkUncommittedBlobForDeletion(ctx, blob.Key))

	// Should fail if blob is live
	blob = blobref.NewChunkedBlobRef(store.Key, record.Key, 0)
	setupTestBlobRef(ctx, t, metaDB, blob)
	_, _, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if assert.NoError(t, err) {
		err = metaDB.MarkUncommittedBlobForDeletion(ctx, blob.Key)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	}

	// Should fail if not found
	err = metaDB.MarkUncommittedBlobForDeletion(ctx, uuid.New())
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestMetaDB_UpdateChunkRef(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	_, _, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	chunk := chunkref.New(blob.Key, 0)
	setupTestChunkRef(ctx, t, metaDB, blob, chunk)

	chunk.Fail()
	assert.NoError(t, metaDB.UpdateChunkRef(ctx, chunk))

	ds := newDatastoreClient(ctx, t)
	got := new(chunkref.ChunkRef)
	if err := ds.Get(ctx, chunkRefKey(chunk.BlobRef, chunk.Key), got); err != nil {
		t.Errorf("datastore.Get() failed: %v", err)
	}
	if diff := cmp.Diff(chunk, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("datastore.Get() = (-want, +got):\n%s", diff)
	}
}

func TestMetaDB_MultipleChunksWithSameNumber(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	_, _, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	chunks := []*chunkref.ChunkRef{
		chunkref.New(blob.Key, 0),
		chunkref.New(blob.Key, 0),
	}

	for i, chunk := range chunks {
		chunk.Size = int32(i)
		assert.NoError(t, chunk.Ready())
		setupTestChunkRef(ctx, t, metaDB, blob, chunk)
	}

	chunks[0].Status = blobref.StatusPendingDeletion
	chunks[1].Status = blobref.StatusReady
	ds := newDatastoreClient(ctx, t)
	for _, chunk := range chunks {
		got := new(chunkref.ChunkRef)
		if err := ds.Get(ctx, chunkRefKey(chunk.BlobRef, chunk.Key), got); err != nil {
			t.Errorf("Couldn't get ChunkRef (%v): %v", chunk.Key, err)
		} else {
			assert.Equal(t, chunk.Key, got.Key)
			assert.Equal(t, chunk.Status, got.Status)
		}
	}

	if _, blob, err := metaDB.PromoteBlobRefToCurrent(ctx, blob); err != nil {
		t.Errorf("PromoteBlobRefToCurrent() failed for BlobRef (%v): %v", blob.Key, err)
	} else {
		assert.EqualValues(t, chunks[1].Size, blob.Size)
	}

	if got, err := metaDB.FindChunkRefByNumber(ctx, blob.StoreKey, blob.RecordKey, 0); err != nil {
		t.Errorf("FindChunkRefByNumber() failed for BlobRef (%v): %v", blob.Key, err)
	} else {
		if diff := cmp.Diff(got, chunks[1], cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("FindChunkRefByNumber() = (-want, +got):\n%s", diff)
		}
	}
}

func TestMetaDB_GetChildChunkRefs(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	_, _, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	chunks := []*chunkref.ChunkRef{
		chunkref.New(blob.Key, 0),
		chunkref.New(blob.Key, 1),
		chunkref.New(blob.Key, 2),
	}
	chunks[1].Status = blobref.StatusError
	chunks[2].Status = blobref.StatusPendingDeletion

	for _, chunk := range chunks {
		setupTestChunkRef(ctx, t, metaDB, blob, chunk)
	}

	cur := metaDB.GetChildChunkRefs(ctx, blob.Key)
	got := []*chunkref.ChunkRef{}
	for i := 0; i < len(chunks); i++ {
		chunk, err := cur.Next()
		if err != nil {
			t.Errorf("Next() failed: %v", err)
		}
		got = append(got, chunk)
	}
	if diff := cmp.Diff(got, chunks,
		cmpopts.SortSlices(func(a, b *chunkref.ChunkRef) bool { return a.Number < b.Number }),
		cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("GetChildChunkRefs() iterator = (-want, +got):\n%s", diff)
	}
	if _, err := cur.Next(); !errors.Is(err, iterator.Done) {
		t.Errorf("Next() = %v, want = iterator.Done", err)
	}
}

func TestMetaDB_ListChunkRefsByStatus(t *testing.T) {
	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)

	_, _, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	statuses := []blobref.Status{
		blobref.StatusError,
		blobref.StatusInitializing,
		blobref.StatusPendingDeletion,
		blobref.StatusPendingDeletion}
	chunks := []*chunkref.ChunkRef{}
	for i, s := range statuses {
		chunk := &chunkref.ChunkRef{
			Key:       uuid.New(),
			BlobRef:   blob.Key,
			Status:    s,
			Number:    int32(i),
			Checksums: checksumstest.RandomChecksums(t),
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Date(2000, 1, i, 0, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2000, 1, i, 0, 0, 0, 0, time.UTC),
				Signature: uuid.New(),
			},
		}
		chunks = append(chunks, chunk)
		setupTestChunkRef(ctx, t, metaDB, blob, chunk)
	}

	testCase := []struct {
		name   string
		status blobref.Status
	}{
		{
			"not found",
			blobref.StatusUnknown,
		},
		{
			"PendingDeletion",
			blobref.StatusPendingDeletion,
		},
	}
	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			iter := metaDB.ListChunkRefsByStatus(ctx, tc.status)
			if iter == nil {
				t.Fatalf("ListChunkRefsByStatus() = %v, want non-nil", iter)
			}
			c, err := iter.Next()
			for err == nil {
				if c.Status != tc.status {
					t.Fatalf("got status %v, want %v", c.Status, tc.status)
				}
				c, err = iter.Next()
			}
			if !errors.Is(err, iterator.Done) {
				t.Errorf("Next() = %v, want = iterator.Done", err)
			}
			if c != nil {
				t.Errorf("Next() = %v, want = nil", iter)
			}
		})
	}
}

func TestMetaDB_QueryRecords(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	metaDB := newMetaDB(ctx, t)

	storeKeys := []string{
		newRecordKey(),
		newRecordKey(),
	}
	sort.Strings(storeKeys)

	stores := make([]*store.Store, 2)
	for i := range stores {
		stores[i], _ = setupTestStoreRecord(ctx, t, metaDB, &store.Store{Key: storeKeys[i]}, nil)
	}

	// Create the records with keys that are ordered to help in verifying the records
	// returned with offset and limit
	keys := []string{
		newRecordKey(),
		newRecordKey(),
		newRecordKey(),
	}
	sort.Strings(keys)

	records := []*record.Record{
		{
			Key:        keys[0],
			OwnerID:    "abc",
			Tags:       []string{"tag1", "tag2"},
			Properties: make(record.PropertyMap),
		},
		{
			Key:        keys[1],
			OwnerID:    "cba",
			Tags:       []string{"gat1", "gat2"},
			Properties: make(record.PropertyMap),
		},
		{
			Key:        keys[2],
			OwnerID:    "xyz",
			Tags:       []string{"tag1", "tag2"},
			Properties: make(record.PropertyMap),
		},
	}
	records[0] = setupTestRecord(ctx, t, metaDB, stores[0].Key, records[0])
	records[1] = setupTestRecord(ctx, t, metaDB, stores[0].Key, records[1])
	records[2] = setupTestRecord(ctx, t, metaDB, stores[1].Key, records[2])

	testCases := []struct {
		name        string
		req         *pb.QueryRecordsRequest
		wantRecords []*record.Record
		wantCode    codes.Code
	}{
		{
			"OwnerId",
			&pb.QueryRecordsRequest{
				StoreKey: stores[0].Key,
				OwnerId:  "abc",
			},
			[]*record.Record{records[0]}, codes.OK,
		},
		{
			"Tags No Result",
			&pb.QueryRecordsRequest{
				StoreKey: stores[1].Key,
				Tags:     []string{"tag1", "gat2"},
			},
			nil, codes.OK,
		},
		{
			"Tags Multiple Records",
			&pb.QueryRecordsRequest{
				Tags: []string{"tag1"},
			},
			[]*record.Record{records[0], records[2]}, codes.OK,
		},
		{
			"Limit",
			&pb.QueryRecordsRequest{
				StoreKey: stores[0].Key,
				Tags:     []string{"tag1"},
				Limit:    1,
			},
			[]*record.Record{records[0]}, codes.OK,
		},
		{
			"Limit No Filtering",
			&pb.QueryRecordsRequest{
				Limit: 1,
			},
			[]*record.Record{records[0]}, codes.OK,
		},
		{
			"Offset No Filtering",
			&pb.QueryRecordsRequest{
				Offset: 1,
			},
			[]*record.Record{records[1], records[2]}, codes.OK,
		},
		{
			"Keys Only",
			&pb.QueryRecordsRequest{
				StoreKey: stores[0].Key,
				OwnerId:  "abc",
				Tags:     []string{"tag1"},
				KeysOnly: true,
			},
			[]*record.Record{{Key: records[0].Key, StoreKey: records[0].StoreKey}}, codes.OK,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The default order of the records returned by Query are sorted per store + record keys
			rr, err := metaDB.QueryRecords(ctx, tc.req)
			assert.Empty(t, cmp.Diff(rr, tc.wantRecords,
				cmpopts.SortSlices(func(x, y *record.Record) bool {
					return x.Key < y.Key
				}),
				cmpopts.EquateEmpty()))
			assert.Equal(t, tc.wantCode, status.Code(err))
			if tc.wantCode == codes.OK {
				assert.NoError(t, err)
			}
		})
	}
}
