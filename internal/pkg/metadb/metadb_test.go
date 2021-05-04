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
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
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
	timestampPrecision = 1 * time.Microsecond
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
	return uuid.New().String() + "_unittest_store"
}

func newRecordKey() string {
	return uuid.New().String() + "_unittest_record"
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
	// save the current namespace as it might change between now and when we delete
	namespace := metaDB.Namespace
	t.Cleanup(func() {
		// Call the Datastore method directly to avoid Status checking
		key := datastore.NameKey(blobKind, blob.Key.String(), nil)
		key.Namespace = namespace
		newDatastoreClient(ctx, t).Delete(ctx, key)
	})
	return newBlob
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
	storeName := "SimpleCreateGetDeleteStore" + uuid.New().String()
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
	expected.Timestamps.NewTimestamps(timestampPrecision)
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
	work.Timestamps.NewTimestamps(timestampPrecision)
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
	expected.Timestamps.NewTimestamps(timestampPrecision)

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
		metadbtest.AssertEqualBlobRefWithinDuration(t, promoBlob, currentBlob, timestampPrecision)
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

	assert.NoError(t, blob.Fail())
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
		StoreKey:  "non-existent" + uuid.New().String(),
		RecordKey: "non-existent" + uuid.New().String(),
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
		bKey := datastore.NameKey(blobKind, blob.Key.String(), nil)
		bKey.Namespace = metaDB.Namespace
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
