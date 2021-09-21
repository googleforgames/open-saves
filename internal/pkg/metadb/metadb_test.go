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
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/metadbtest"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	storeKind          = "store"
	recordKind         = "record"
	blobKind           = "blob"
	chunkKind          = "chunk"
	timestampTestDelta = 5 * time.Second
	testProject        = "triton-for-games-dev"
	testNamespace      = "datastore-unittests"
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
		t.Fatalf("datastore.NewClient failed: %v", err)
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
	newStore, err := metaDB.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	t.Cleanup(func() {
		metaDB.DeleteStore(ctx, newStore.Key)
	})
	metadbtest.AssertEqualStore(t, store, newStore, "CreateStore should return the created store.")
	var newRecord *record.Record
	if r != nil {
		newRecord, err = metaDB.InsertRecord(ctx, newStore.Key, r)
		if err != nil {
			t.Fatalf("Could not create a new record: %v", err)
		}
		t.Cleanup(func() {
			metaDB.DeleteRecord(ctx, newStore.Key, newRecord.Key)
		})
		metadbtest.AssertEqualRecord(t, r, newRecord, "GetRecord should return the exact same record.")
	}
	return newStore, newRecord
}

func setupTestBlobRef(ctx context.Context, t *testing.T, metaDB *m.MetaDB, blob *blobref.BlobRef) *blobref.BlobRef {
	t.Helper()
	newBlob, err := metaDB.InsertBlobRef(ctx, blob)
	if err != nil {
		t.Fatalf("InsertBlobRef failed: %v", err)
	}
	metadbtest.AssertEqualBlobRef(t, blob, newBlob)

	t.Cleanup(func() {
		// Call the Datastore method directly to avoid Status checking
		key := blobRefKey(blob.Key)
		newDatastoreClient(ctx, t).Delete(ctx, key)
	})
	return newBlob
}

func setupTestChunkRef(ctx context.Context, t *testing.T, metaDB *m.MetaDB, chunk *chunkref.ChunkRef) {
	t.Helper()
	if err := metaDB.InsertChunkRef(ctx, chunk); err != nil {
		t.Fatalf("InsertChunkRef failed for chunk key (%v): %v", chunk.Key, err)
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
	metadbtest.AssertEqualStore(t, store, createdStore, "CreateStore should return the created store.")

	store2, err := metaDB.GetStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Could not get store (%s): %v", storeKey, err)
	}
	metadbtest.AssertEqualStore(t, store, store2, "GetStore should return the exact same store.")

	store3, err := metaDB.FindStoreByName(ctx, storeName)
	if err != nil {
		t.Fatalf("Could not fetch store by name (%s): %v", storeName, err)
	}
	metadbtest.AssertEqualStore(t, store, store3, "FindStoreByName should return the exact same store.")

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

	createdRecord, err := metaDB.InsertRecord(ctx, storeKey, record)
	expected.Timestamps = timestamps.New()
	// Copy the new signature as we cannot generate the same UUID.
	expected.Timestamps.Signature = createdRecord.Timestamps.Signature
	if err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecordWithinDuration(t, expected, createdRecord,
		testTimestampThreshold, "InsertRecord should return the created record.")

	// Use the updated timestamps for the subsequent checks
	expected.Timestamps = createdRecord.Timestamps
	record2, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecord(t, expected, record2, "GetRecord should return the exact same record.")

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
	metadbtest.AssertEqualRecordWithinDuration(t, expected,
		updated, testTimestampThreshold, "Updated record is not the same as original.")

	stored, err := metaDB.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecord(t, updated, stored, "GetRecord should fetch the updated record.")
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
			return toUpdate, metadb.ErrNoUpdate
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
		t.Fatalf("DeleteStore failed with a non-existent key: %v", err)
	}
	recordKey := newRecordKey()
	if err := metaDB.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Fatalf("DeleteRecord failed with a non-existent key: %v", err)
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
		t.Errorf("GetBlobRef failed: %v", err)
	} else {
		metadbtest.AssertEqualBlobRef(t, blob, blob2)
	}

	beforePromo := time.Now()
	promoRecord, promoBlob, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobAsCurrent failed: %v", err)
	} else {
		if assert.NotNil(t, promoRecord) {
			assert.Nil(t, promoRecord.Blob)
			assert.Equal(t, blobKey, promoRecord.ExternalBlob)
			assert.Equal(t, blob.Size, promoRecord.BlobSize)
			assert.True(t, beforePromo.Before(promoRecord.Timestamps.UpdatedAt))
			assert.NotEqual(t, record.Timestamps.Signature, promoRecord.Timestamps.Signature)
		}
		if assert.NotNil(t, promoBlob) {
			assert.Equal(t, blobKey, promoBlob.Key)
			assert.Equal(t, blobref.StatusReady, promoBlob.Status)
			assert.True(t, beforePromo.Before(promoBlob.Timestamps.UpdatedAt))
			assert.NotEqual(t, origSig, promoBlob.Timestamps.Signature)
		}
	}

	currentBlob, err := metaDB.GetCurrentBlobRef(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("GetCurrentBlobRef failed: %v", err)
	} else {
		metadbtest.AssertEqualBlobRefWithinDuration(t, promoBlob, currentBlob, timestamps.Precision)
	}

	if err := metaDB.DeleteBlobRef(ctx, blobKey); err == nil {
		t.Errorf("DeleteBlobRef should fail on current blob: %v", err)
	} else {
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	}

	delPendRecord, delPendBlob, err := metaDB.RemoveBlobFromRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("RemoveBlobFromRecord failed: %v", err)
	} else {
		if assert.NotNil(t, delPendRecord) {
			assert.Equal(t, uuid.Nil, delPendRecord.ExternalBlob)
			assert.Empty(t, delPendRecord.Blob)
			assert.Zero(t, delPendRecord.BlobSize)
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
		t.Errorf("PromoteBlobRefToCurrent failed: %v", err)
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
		t.Errorf("UpdateBlobRef failed: %v", err)
	} else {
		if assert.NotNil(t, updatedBlob) {
			metadbtest.AssertEqualBlobRef(t, blob, updatedBlob)
		}
	}

	receivedBlob, err := metaDB.GetBlobRef(ctx, blob.Key)
	if err != nil {
		t.Errorf("GetBlobRef failed: %v", err)
	} else {
		if assert.NotNil(t, receivedBlob) {
			metadbtest.AssertEqualBlobRef(t, blob, receivedBlob)
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
		t.Errorf("PromoteBlobRefToCurrent failed: %v", err)
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
	for i, s := range statuses {
		blob := &blobref.BlobRef{
			Key:       uuid.New(),
			Status:    s,
			StoreKey:  storeKey,
			RecordKey: recordKey,
		}
		blobs = append(blobs, blob)
		setupTestBlobRef(ctx, t, metaDB, blob)

		// Update the timestamps here because MetaDB automatically sets UpdatesAt
		blob.Timestamps.UpdatedAt = time.Date(2000, 1, i, 0, 0, 0, 0, time.UTC)
		bKey := blobRefKey(blob.Key)
		if _, err := client.Put(ctx, bKey, blob); err != nil {
			t.Fatalf("Failed to change UpdatedAt for %v: %v", blob.Key, err)
		}
	}

	// Should return iterator.Done and nil when not found
	iter, err := metaDB.ListBlobRefsByStatus(ctx, blobref.StatusError, time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.NoError(t, err)
	if assert.NotNil(t, iter) {
		b, err := iter.Next()
		assert.Equal(t, iterator.Done, err)
		assert.Nil(t, b)
	}

	iter, err = metaDB.ListBlobRefsByStatus(ctx, blobref.StatusPendingDeletion, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.NoError(t, err)
	if assert.NotNil(t, iter) {
		// Should return both of the PendingDeletion entries
		b, err := iter.Next()
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			metadbtest.AssertEqualBlobRef(t, blobs[2], b)
		}
		b, err = iter.Next()
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			metadbtest.AssertEqualBlobRef(t, blobs[3], b)
		}
		b, err = iter.Next()
		assert.Equal(t, iterator.Done, err)
		assert.Nil(t, b)
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
		t.Errorf("PromoteBlobRefToCurrent failed: %v", err)
	}

	if err := metaDB.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Errorf("DeleteRecord failed: %v", err)
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
		testChunKSize  = int32(42)
	)

	ctx := context.Background()
	metaDB := newMetaDB(ctx, t)
	ds := newDatastoreClient(ctx, t)

	store, record, blob := setupTestStoreRecordBlobSet(ctx, t, metaDB, true)

	// Create Chunks with Initializing state
	chunks := []*chunkref.ChunkRef{}
	for i := 0; i < testChunkCount; i++ {
		chunk := chunkref.New(blob.Key, int32(i))
		setupTestChunkRef(ctx, t, metaDB, chunk)
		chunks = append(chunks, chunk)
	}

	// Mark the chunks ready
	for _, chunk := range chunks {
		chunk.Size = testChunKSize
		now := time.Now()
		if err := metaDB.MarkChunkRefReady(ctx, chunk); err != nil {
			t.Fatalf("MarkChunkRefReady failed for chunk (%v): %v", chunk.Key, err)
		}
		dsKey := chunkRefKey(chunk.BlobRef, chunk.Key)
		got := new(chunkref.ChunkRef)
		if err := ds.Get(ctx, dsKey, got); err != nil {
			t.Errorf("Failed to get updated ChunkRef (%v): %v", dsKey, err)
		} else {
			assert.True(t, now.Before(got.Timestamps.UpdatedAt))
			assert.Equal(t, blobref.StatusReady, got.Status)
		}
	}
	_, blobRetrieved, err := metaDB.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Fatalf("PromoteBlobRefToCurrent failed: %v", err)
	}
	if assert.NotNil(t, blobRetrieved) {
		assert.EqualValues(t, testChunKSize*testChunkCount, blobRetrieved.Size)
	}

	// Verify blob and chunk metadata
	if blobRetrieved, err := metaDB.GetCurrentBlobRef(ctx, store.Key, record.Key); err != nil {
		t.Errorf("GetCurrentBlobRef failed: %v", err)
	} else {
		assert.EqualValues(t, testChunKSize*testChunkCount, blobRetrieved.Size)
		assert.Equal(t, blob.Key, blobRetrieved.Key)
		assert.True(t, blobRetrieved.Chunked)
	}
	for i := 0; i < testChunkCount; i++ {
		got, err := metaDB.FindChunkRefByNumber(ctx, store.Key, record.Key, int32(i))
		if assert.NoError(t, err, "FindChunkRefByNumber should not return error") {
			metadbtest.AssertEqualChunkRef(t, chunks[i], got)
		}
	}

	// Delete chunks and blob
	if _, _, err := metaDB.RemoveBlobFromRecord(ctx, store.Key, record.Key); err != nil {
		t.Fatalf("RemoveBlobFromRecord should work with Chunked blobs")
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
		t.Fatalf("DeleteBlobRef failed: %v", err)
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
	setupTestChunkRef(ctx, t, metaDB, chunk)

	assert.NoError(t, metaDB.MarkUncommittedBlobForDeletion(ctx, blob.Key))

	// Should fail if blob is live
	blob = blobref.NewChunkedBlobRef(store.Key, record.Key)
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
	setupTestChunkRef(ctx, t, metaDB, chunk)

	chunk.Fail()
	assert.NoError(t, metaDB.UpdateChunkRef(ctx, chunk))

	ds := newDatastoreClient(ctx, t)
	got := new(chunkref.ChunkRef)
	err := ds.Get(ctx, chunkRefKey(chunk.BlobRef, chunk.Key), got)
	if assert.NoError(t, err) {
		metadbtest.AssertEqualChunkRef(t, chunk, got)
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

	for _, chunk := range chunks {
		setupTestChunkRef(ctx, t, metaDB, chunk)
	}

	for i, chunk := range chunks {
		chunk.Size = int32(i)
		assert.NoError(t, metaDB.MarkChunkRefReady(ctx, chunk))
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
		t.Errorf("PromoteBlobRefToCurrent failed for BlobRef (%v): %v", blob.Key, err)
	} else {
		assert.EqualValues(t, chunks[1].Size, blob.Size)
	}

	if got, err := metaDB.FindChunkRefByNumber(ctx, blob.StoreKey, blob.RecordKey, 0); err != nil {
		t.Errorf("FindChunkRefByNumber failed for BlobRef (%v): %v", blob.Key, err)
	} else {
		metadbtest.AssertEqualChunkRef(t, chunks[1], got)
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
		setupTestChunkRef(ctx, t, metaDB, chunk)
	}

	cur := metaDB.GetChildChunkRefs(ctx, blob.Key)
	for i := 0; i < len(chunks); i++ {
		actual, err := cur.Next()
		if assert.NoError(t, err) {
			metadbtest.AssertEqualChunkRef(t, chunks[actual.Number], actual)
		}
	}
	actual, err := cur.Next()
	assert.ErrorIs(t, err, iterator.Done)
	assert.Nil(t, actual)
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
			Key:     uuid.New(),
			BlobRef: blob.Key,
			Status:  s,
			Number:  int32(i),
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Date(2000, 1, i, 0, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2000, 1, i, 0, 0, 0, 0, time.UTC),
				Signature: uuid.New(),
			},
		}
		chunks = append(chunks, chunk)
		setupTestChunkRef(ctx, t, metaDB, chunk)
	}

	// Should return iterator.Done and nil when not found
	iter := metaDB.ListChunkRefsByStatus(ctx, blobref.StatusError, time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC))
	if assert.NotNil(t, iter) {
		b, err := iter.Next()
		assert.Equal(t, iterator.Done, err)
		assert.Nil(t, b)
	}

	iter = metaDB.ListChunkRefsByStatus(ctx, blobref.StatusPendingDeletion, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	if assert.NotNil(t, iter) {
		// Should return both of the PendingDeletion entries
		b, err := iter.Next()
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			metadbtest.AssertEqualChunkRef(t, chunks[2], b)
		}
		b, err = iter.Next()
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			metadbtest.AssertEqualChunkRef(t, chunks[3], b)
		}
		b, err = iter.Next()
		assert.Equal(t, iterator.Done, err)
		assert.Nil(t, b)
	}
}
