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

package datastore

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/metadbtest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testTimestampThreshold = 30 * time.Second

func newDriver(ctx context.Context, t *testing.T) *Driver {
	t.Helper()
	driver, err := NewDriver(ctx, "triton-for-games-dev")
	if err != nil {
		t.Fatalf("Initializing Datastore driver: %v", err)
	}
	driver.Namespace = "datastore-unittests"
	if err := driver.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to Datastore: %v", err)
	}
	t.Cleanup(func() { driver.Disconnect(ctx) })
	return driver
}

func newStoreKey() string {
	return uuid.New().String() + "_unittest_store"
}

func newRecordKey() string {
	return uuid.New().String() + "_unittest_record"
}

func cloneRecord(r *m.Record) *m.Record {
	if r == nil {
		return nil
	}
	ret := *r
	ret.Blob = append([]byte{}, r.Blob...)
	ret.Properties = make(m.PropertyMap)
	for k, v := range r.Properties {
		ret.Properties[k] = v
	}
	ret.Tags = append([]string{}, r.Tags...)
	return &ret
}

// setupTestStoreRecord creates a new store and inserts a record into it, then registers
// cleanup functions to delete these test store and record.
// Passing a nil to record will skip the record insertion.
func setupTestStoreRecord(ctx context.Context, t *testing.T, driver *Driver, store *m.Store, record *m.Record) (*m.Store, *m.Record) {
	t.Helper()
	newStore, err := driver.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	t.Cleanup(func() {
		driver.DeleteStore(ctx, newStore.Key)
	})
	metadbtest.AssertEqualStore(t, store, newStore, "CreateStore should return the created store.")
	var newRecord *m.Record
	if record != nil {
		newRecord, err = driver.InsertRecord(ctx, newStore.Key, record)
		if err != nil {
			t.Fatalf("Could not create a new record: %v", err)
		}
		t.Cleanup(func() {
			driver.DeleteRecord(ctx, newStore.Key, newRecord.Key)
		})
		metadbtest.AssertEqualRecord(t, record, newRecord, "GetRecord should return the exact same record.")
	}
	return newStore, newRecord
}

func setupTestBlobRef(ctx context.Context, t *testing.T, driver *Driver, blob *m.BlobRef) *m.BlobRef {
	t.Helper()
	newBlob, err := driver.InsertBlobRef(ctx, blob)
	if err != nil {
		t.Fatalf("InsertBlobRef failed: %v", err)
	}
	metadbtest.AssertEqualBlobRef(t, blob, newBlob)
	// save the current namespace as it might change between now and when we delete
	namespace := driver.Namespace
	t.Cleanup(func() {
		// Call the Datastore method directly to avoid Status checking
		key := datastore.NameKey(blobKind, blob.Key.String(), nil)
		key.Namespace = namespace
		driver.client.Delete(ctx, key)
	})
	return newBlob
}

func TestDriver_ConnectDisconnect(t *testing.T) {
	ctx := context.Background()
	// Connect() is tested inside newDriver().
	driver := newDriver(ctx, t)
	if err := driver.Disconnect(ctx); err != nil {
		t.Fatalf("Failed to disconnect: %v", err)
	}
}

func TestDriver_SimpleCreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	storeKey := newStoreKey()
	storeName := "SimpleCreateGetDeleteStore" + uuid.New().String()
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	store := &m.Store{
		Key:     storeKey,
		Name:    storeName,
		OwnerID: "owner",
		Tags:    []string{"abc", "def"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("db94be80-e036-4ca8-a9c0-2259b8a67acc"),
		},
	}
	createdStore, err := driver.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	metadbtest.AssertEqualStore(t, store, createdStore, "CreateStore should return the created store.")

	store2, err := driver.GetStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Could not get store (%s): %v", storeKey, err)
	}
	metadbtest.AssertEqualStore(t, store, store2, "GetStore should return the exact same store.")

	store3, err := driver.FindStoreByName(ctx, storeName)
	if err != nil {
		t.Fatalf("Could not fetch store by name (%s): %v", storeName, err)
	}
	metadbtest.AssertEqualStore(t, store, store3, "FindStoreByName should return the exact same store.")

	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
	store4, err := driver.GetStore(ctx, storeKey)
	if err == nil {
		t.Fatalf("GetStore didn't return an error after deleting a store: %v", err)
	}
	assert.Nil(t, store4, "GetStore should return a nil store.")
}

func TestDriver_SimpleCreateGetDeleteRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	storeKey := newStoreKey()
	store := &m.Store{
		Key:     storeKey,
		Name:    "SimpleCreateGetDeleteRecord",
		OwnerID: "owner",
	}
	setupTestStoreRecord(ctx, t, driver, store, nil)

	recordKey := newRecordKey()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	record := &m.Record{
		Key:      recordKey,
		Blob:     blob,
		BlobSize: int64(len(blob)),
		OwnerID:  "record owner",
		Tags:     []string{"abc", "def"},
		Properties: m.PropertyMap{
			"BoolTP":   {Type: pb.Property_BOOLEAN, BooleanValue: false},
			"IntTP":    {Type: pb.Property_INTEGER, IntegerValue: 42},
			"StringTP": {Type: pb.Property_STRING, StringValue: "a string value"},
		},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("89223949-0414-438e-8f5e-3fd9e2d11c1e"),
		},
	}
	expected := cloneRecord(record)

	createdRecord, err := driver.InsertRecord(ctx, storeKey, record)
	if err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecord(t, expected, createdRecord, "InsertRecord should return the created record.")

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecord(t, expected, record2, "GetRecord should return the exact same record.")

	record3, err := driver.InsertRecord(ctx, storeKey, record)
	assert.NotNil(t, err, "Insert should fail if a record with the same already exists.")
	assert.Nil(t, record3, "Insert should fail and return nil.")

	err = driver.DeleteRecord(ctx, storeKey, recordKey)
	assert.Nilf(t, err, "Failed to delete a record (%s) in store (%s): %v", recordKey, storeKey, err)

	record4, err := driver.GetRecord(ctx, storeKey, recordKey)
	assert.NotNilf(t, err, "GetRecord didn't return an error after deleting a record (%s)", recordKey)
	assert.Nil(t, record4, "GetRecord should return a nil with error")

	// The test store is deleted inside the cleanup function.
}

func TestDriver_InsertRecordShouldFailWithNonExistentStore(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	storeKey := newStoreKey()
	record := &m.Record{
		Key: newRecordKey(),
	}
	record1, err := driver.InsertRecord(ctx, storeKey, record)
	if err == nil {
		t.Error("InsertRecord should fail if the store doesn't exist.")
		driver.DeleteRecord(ctx, storeKey, record.Key)
	} else {
		assert.Equalf(t, codes.FailedPrecondition, status.Code(err),
			"InsertRecord should return FailedPrecondition if the store doesn't exist: %v", err)
	}
	assert.Nil(t, record1, "InsertRecord should return nil with error")
}

func TestDriver_DeleteStoreShouldFailWhenNotEmpty(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	store := &m.Store{Key: newStoreKey()}
	record := &m.Record{Key: newRecordKey(), Properties: make(m.PropertyMap)}

	setupTestStoreRecord(ctx, t, driver, store, record)

	if err := driver.DeleteStore(ctx, store.Key); err == nil {
		t.Error("DeleteStore should fail if the store is not empty.")
	} else {
		assert.Equalf(t, codes.FailedPrecondition, status.Code(err),
			"DeleteStore should return FailedPrecondition if the store is not empty: %v", err)
	}
}

func TestDriver_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)

	storeKey := newStoreKey()
	store := &m.Store{
		Key:     storeKey,
		Name:    "UpdateRecord",
		OwnerID: "owner",
	}

	recordKey := newRecordKey()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	record := &m.Record{
		Key:        recordKey,
		Blob:       blob,
		BlobSize:   int64(len(blob)),
		Properties: make(m.PropertyMap),
		OwnerID:    "record owner",
		Tags:       []string{"abc", "def"},
	}
	record.Timestamps.NewTimestamps(driver.TimestampPrecision())
	expected := cloneRecord(record)

	updatedRecord, err := driver.UpdateRecord(ctx, storeKey, recordKey,
		func(*m.Record) (*m.Record, error) { return nil, nil })
	assert.NotNil(t, err, "UpdateRecord should return an error if the specified record doesn't exist.")
	assert.Nil(t, updatedRecord, "UpdateRecord should return nil with error")

	setupTestStoreRecord(ctx, t, driver, store, record)

	record.Tags = append(record.Tags, "ghi")
	expected.Tags = append(expected.Tags, "ghi")
	record.OwnerID = "NewOwner"
	expected.OwnerID = record.OwnerID
	expected.Timestamps.NewTimestamps(driver.TimestampPrecision())

	// Make sure UpdateRecord doesn't update CreatedAt
	record.Timestamps.CreatedAt = time.Unix(0, 0)
	updatedRecord, err = driver.UpdateRecord(ctx, storeKey, recordKey,
		func(r *m.Record) (*m.Record, error) {
			r.OwnerID = record.OwnerID
			r.Tags = record.Tags
			return r, nil
		})
	assert.Nilf(t, err, "Failed to update a record (%s) in store (%s): %v", recordKey, storeKey, err)
	// Make sure the signatures are different before passing to AssertEqualRecordWithinDuration
	assert.NotEqual(t, expected.Timestamps.Signature, updatedRecord.Timestamps.Signature)
	expected.Timestamps.Signature = updatedRecord.Timestamps.Signature
	metadbtest.AssertEqualRecordWithinDuration(t, expected,
		updatedRecord, testTimestampThreshold, "Updated record is not the same as original.")

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecord(t, updatedRecord, record2, "GetRecord should fetch the updated record.")
}

func TestDriver_DeleteShouldNotFailWithNonExistentKey(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	storeKey := newStoreKey()
	if err := driver.DeleteStore(ctx, storeKey); err != nil {
		t.Fatalf("DeleteStore failed with a non-existent key: %v", err)
	}
	recordKey := newRecordKey()
	if err := driver.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Fatalf("DeleteRecord failed with a non-existent key: %v", err)
	}
}

func TestDriver_TimestampPrecision(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	assert.Equal(t, timestampPrecision, driver.TimestampPrecision())
}

func TestDriver_SimpleCreateGetDeleteBlobRef(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	storeKey := newStoreKey()
	store := &m.Store{
		Key:  storeKey,
		Name: "SimpleCreateGetDeleteBlobRef",
	}
	testBlob := []byte{1, 2, 3, 4, 5}

	recordKey := newRecordKey()
	createdAt := time.Unix(12345, 0)
	record := &m.Record{
		Key:        recordKey,
		Blob:       testBlob,
		BlobSize:   int64(len(testBlob)),
		Properties: make(m.PropertyMap),
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.New(),
		},
	}

	setupTestStoreRecord(ctx, t, driver, store, record)
	blobKey := uuid.New()
	origSig := uuid.New()
	blob := &m.BlobRef{
		Key:       blobKey,
		Size:      12345,
		Status:    m.BlobRefStatusInitializing,
		StoreKey:  storeKey,
		RecordKey: recordKey,
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: origSig,
		},
	}

	setupTestBlobRef(ctx, t, driver, blob)

	_, err := driver.GetCurrentBlobRef(ctx, storeKey, recordKey)
	assert.Equal(t, codes.FailedPrecondition, grpc.Code(err))

	blob2, err := driver.GetBlobRef(ctx, blobKey)
	if err != nil {
		t.Errorf("GetBlobRef failed: %v", err)
	} else {
		metadbtest.AssertEqualBlobRef(t, blob, blob2)
	}

	beforePromo := time.Now()
	promoRecord, promoBlob, err := driver.PromoteBlobRefToCurrent(ctx, blob)
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
			assert.Equal(t, m.BlobRefStatusReady, promoBlob.Status)
			assert.True(t, beforePromo.Before(promoBlob.Timestamps.UpdatedAt))
			assert.NotEqual(t, origSig, promoBlob.Timestamps.Signature)
		}
	}

	currentBlob, err := driver.GetCurrentBlobRef(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("GetCurrentBlobRef failed: %v", err)
	} else {
		metadbtest.AssertEqualBlobRefWithinDuration(t, promoBlob, currentBlob, driver.TimestampPrecision())
	}

	if err := driver.DeleteBlobRef(ctx, blobKey); err == nil {
		t.Errorf("DeleteBlobRef should fail on current blob: %v", err)
	} else {
		assert.Equal(t, codes.FailedPrecondition, grpc.Code(err))
	}

	delPendRecord, delPendBlob, err := driver.RemoveBlobFromRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("RemoveBlobFromRecord failed: %v", err)
	} else {
		if assert.NotNil(t, delPendRecord) {
			assert.Equal(t, uuid.Nil, delPendRecord.ExternalBlob)
			assert.Empty(t, delPendRecord.Blob)
			assert.Zero(t, delPendRecord.BlobSize)
		}
		if assert.NotNil(t, delPendBlob) {
			assert.Equal(t, m.BlobRefStatusPendingDeletion, delPendBlob.Status)
		}
	}

	assert.NoError(t, driver.DeleteBlobRef(ctx, blobKey))

	deletedBlob, err := driver.GetBlobRef(ctx, blobKey)
	assert.Nil(t, deletedBlob)
	assert.Equal(t, codes.NotFound, grpc.Code(err), "GetBlobRef should return NotFound after deletion.")
}

func TestDriver_SwapBlobRefs(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	store := &m.Store{
		Key:  newStoreKey(),
		Name: "SwapBlobRefs",
	}

	record := &m.Record{
		Key:        newRecordKey(),
		Properties: make(m.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, driver, store, record)

	blob := &m.BlobRef{
		Key:       uuid.New(),
		Status:    m.BlobRefStatusInitializing,
		StoreKey:  store.Key,
		RecordKey: record.Key,
	}
	setupTestBlobRef(ctx, t, driver, blob)

	_, blob, err := driver.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobRefToCurrent failed: %v", err)
	}

	newBlob := &m.BlobRef{
		Key:       uuid.New(),
		Status:    m.BlobRefStatusInitializing,
		StoreKey:  store.Key,
		RecordKey: record.Key,
	}
	setupTestBlobRef(ctx, t, driver, newBlob)

	record, newCurrBlob, err := driver.PromoteBlobRefToCurrent(ctx, newBlob)
	if assert.NoError(t, err) {
		if assert.NotNil(t, record) {
			assert.Equal(t, newBlob.Key, record.ExternalBlob)
		}
		if assert.NotNil(t, newCurrBlob) {
			assert.Equal(t, newBlob.Key, newCurrBlob.Key)
			assert.Equal(t, m.BlobRefStatusReady, newCurrBlob.Status)
		}
	}

	oldBlob, err := driver.GetBlobRef(ctx, blob.Key)
	if assert.NoError(t, err) {
		if assert.NotNil(t, oldBlob) {
			assert.Equal(t, blob.Key, oldBlob.Key)
			assert.Equal(t, m.BlobRefStatusPendingDeletion, oldBlob.Status)
		}
	}

	_, _, err = driver.RemoveBlobFromRecord(ctx, store.Key, record.Key)
	assert.NoError(t, err)
}

func TestDriver_UpdateBlobRef(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	store := &m.Store{
		Key:  newStoreKey(),
		Name: "SimpleCreateGetDeleteBlobRef",
	}

	record := &m.Record{
		Key:        newRecordKey(),
		Properties: make(m.PropertyMap),
	}

	setupTestStoreRecord(ctx, t, driver, store, record)
	blob := &m.BlobRef{
		Key:       uuid.New(),
		Status:    m.BlobRefStatusInitializing,
		StoreKey:  store.Key,
		RecordKey: record.Key,
		Timestamps: m.Timestamps{
			CreatedAt: time.Unix(123, 0),
			UpdatedAt: time.Unix(123, 0),
		},
	}

	setupTestBlobRef(ctx, t, driver, blob)

	assert.NoError(t, blob.Fail())
	blob.Timestamps.UpdatedAt = time.Unix(234, 0)

	updatedBlob, err := driver.UpdateBlobRef(ctx, blob)
	if err != nil {
		t.Errorf("UpdateBlobRef failed: %v", err)
	} else {
		if assert.NotNil(t, updatedBlob) {
			metadbtest.AssertEqualBlobRef(t, blob, updatedBlob)
		}
	}

	receivedBlob, err := driver.GetBlobRef(ctx, blob.Key)
	if err != nil {
		t.Errorf("GetBlobRef failed: %v", err)
	} else {
		if assert.NotNil(t, receivedBlob) {
			metadbtest.AssertEqualBlobRef(t, blob, receivedBlob)
		}
	}
}

func TestDriver_BlobInsertShouldFailForNonexistentRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	blob := &m.BlobRef{
		Key:       uuid.New(),
		StoreKey:  "non-existent" + uuid.New().String(),
		RecordKey: "non-existent" + uuid.New().String(),
	}

	insertedBlob, err := driver.InsertBlobRef(ctx, blob)
	if err == nil {
		t.Error("InsertBlobRef should fail for a non-existent record.")
		assert.NoError(t, driver.DeleteBlobRef(ctx, blob.Key))
	} else {
		assert.Equal(t, codes.FailedPrecondition, grpc.Code(err))
		assert.Nil(t, insertedBlob)
	}
}

func TestDriver_UpdateRecordWithExternalBlobs(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)

	storeKey := newStoreKey()
	store := &m.Store{
		Key: storeKey,
	}

	recordKey := newRecordKey()
	record := &m.Record{
		Key:        recordKey,
		Properties: make(m.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, driver, store, record)

	blobKey := uuid.New()
	blob := &m.BlobRef{
		Key:       blobKey,
		Status:    m.BlobRefStatusInitializing,
		StoreKey:  storeKey,
		RecordKey: recordKey,
		Timestamps: m.Timestamps{
			CreatedAt: time.Unix(123, 0),
			UpdatedAt: time.Unix(123, 0),
		},
	}
	setupTestBlobRef(ctx, t, driver, blob)

	record, blob, err := driver.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobRefToCurrent failed: %v", err)
	}

	_, err = driver.UpdateRecord(ctx, storeKey, recordKey, func(record *m.Record) (*m.Record, error) {
		record.ExternalBlob = uuid.New()
		return record, nil
	})
	if assert.Error(t, err, "UpdateRecord should fail when ExternalBlob is modified") {
		assert.Equal(t, codes.Internal, grpc.Code(err))
	}

	// Make sure ExternalBlob is preserved when not modified
	newRecord, err := driver.UpdateRecord(ctx, storeKey, recordKey, func(record *m.Record) (*m.Record, error) {
		// noop
		return record, nil
	})

	if assert.NoError(t, err) {
		if assert.NotNil(t, newRecord) {
			assert.Equal(t, blobKey, newRecord.ExternalBlob)
		}
	}

	actual, err := driver.GetRecord(ctx, storeKey, recordKey)
	if assert.NoError(t, err) {
		if assert.NotNil(t, actual) {
			assert.Equal(t, blobKey, actual.ExternalBlob)
		}
	}

	// Check if ExternalBlob is marked for deletion
	testBlob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	newRecord, err = driver.UpdateRecord(ctx, storeKey, recordKey, func(record *m.Record) (*m.Record, error) {
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

	actual, err = driver.GetRecord(ctx, storeKey, recordKey)
	if assert.NoError(t, err) {
		if assert.NotNil(t, actual) {
			assert.Equal(t, testBlob, actual.Blob)
			assert.Equal(t, int64(len(testBlob)), actual.BlobSize)
			assert.Equal(t, uuid.Nil, actual.ExternalBlob)
		}
	}

	blob, err = driver.GetBlobRef(ctx, blob.Key)
	if assert.NoError(t, err) {
		if assert.NotNil(t, blob) {
			assert.Equal(t, m.BlobRefStatusPendingDeletion, blob.Status)
		}
	}
}

func TestDriver_ListBlobsByStatus(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)

	storeKey := newStoreKey()
	store := &m.Store{
		Key: storeKey,
	}

	recordKey := newRecordKey()
	record := &m.Record{
		Key:        recordKey,
		Properties: make(m.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, driver, store, record)

	statuses := []m.BlobRefStatus{m.BlobRefStatusError, m.BlobRefStatusInitializing, m.BlobRefStatusPendingDeletion, m.BlobRefStatusPendingDeletion}
	blobs := []*m.BlobRef{}
	for i, s := range statuses {
		blob := &m.BlobRef{
			Key:       uuid.New(),
			Status:    s,
			StoreKey:  storeKey,
			RecordKey: recordKey,
			Timestamps: m.Timestamps{
				UpdatedAt: time.Date(2000, 1, i, 0, 0, 0, 0, time.UTC),
			},
		}
		blobs = append(blobs, blob)
		setupTestBlobRef(ctx, t, driver, blob)
	}

	// Should return iterator.Done and nil when not found
	iter, err := driver.ListBlobRefsByStatus(ctx, m.BlobRefStatusError, time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.NoError(t, err)
	if assert.NotNil(t, iter) {
		b, err := iter.Next()
		assert.Equal(t, iterator.Done, err)
		assert.Nil(t, b)
	}

	iter, err = driver.ListBlobRefsByStatus(ctx, m.BlobRefStatusPendingDeletion, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
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

func TestDriver_DeleteRecordWithExternalBlob(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)

	storeKey := newStoreKey()
	store := &m.Store{
		Key:  storeKey,
		Name: t.Name(),
	}

	recordKey := newRecordKey()
	record := &m.Record{
		Key:        recordKey,
		Tags:       []string{t.Name()},
		Properties: make(m.PropertyMap),
	}
	setupTestStoreRecord(ctx, t, driver, store, record)

	blob := m.NewBlobRef(0, storeKey, recordKey)
	setupTestBlobRef(ctx, t, driver, blob)

	record, blob, err := driver.PromoteBlobRefToCurrent(ctx, blob)
	if err != nil {
		t.Errorf("PromoteBlobRefToCurrent failed: %v", err)
	}

	if err := driver.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Errorf("DeleteRecord failed: %v", err)
	}

	actual, err := driver.GetBlobRef(ctx, blob.Key)
	assert.NoError(t, err, "GetBlobRef should not return error")
	if assert.NotNil(t, actual) {
		assert.Equal(t, m.BlobRefStatusPendingDeletion, actual.Status)
	}
}

// This case tests if DeleteRecord deletes a record anyway when
// the associated BlobRef does not exist and the database is inconsistent.
func TestDriver_DeleteRecordWithNonExistentBlobRef(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)

	storeKey := newStoreKey()
	store := &m.Store{
		Key:  storeKey,
		Name: t.Name(),
	}

	recordKey := newRecordKey()
	record := &m.Record{
		Key:          recordKey,
		ExternalBlob: uuid.New(),
		Tags:         []string{t.Name()},
		Properties:   make(m.PropertyMap),
	}

	setupTestStoreRecord(ctx, t, driver, store, record)

	actualBlob, err := driver.GetBlobRef(ctx, record.ExternalBlob)
	assert.Nil(t, actualBlob)
	if assert.Error(t, err, "GetBlobRef should return error when BlobRef doesn't exist.") {
		assert.Equal(t, codes.NotFound, grpc.Code(err),
			"GetBlobRef should return NotFound when BlobRef doesn't exist.")
	}

	assert.NotEqual(t, uuid.Nil, record.ExternalBlob)
	assert.NoError(t, driver.DeleteRecord(ctx, storeKey, recordKey),
		"DeleteRecord should succeed even if ExternalBlob doesn't exist.")

	actualRecord, err := driver.GetRecord(ctx, storeKey, recordKey)
	assert.Nil(t, actualRecord)
	if assert.Error(t, err, "GetRecord should return error after DeleteRecord") {
		assert.Equal(t, codes.NotFound, grpc.Code(err),
			"GetRecord should return NotFound after DeleteRecord")
	}
}
