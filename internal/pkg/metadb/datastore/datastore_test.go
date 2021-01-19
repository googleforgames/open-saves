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

	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/metadbtest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newDriver(ctx context.Context, t *testing.T) *Driver {
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
	return "unittest_store_" + uuid.New().String()
}

func newRecordKey() string {
	return "unittest_record_" + uuid.New().String()
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
	newStore, err := driver.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	t.Cleanup(func() {
		driver.DeleteStore(ctx, newStore.Key)
		assert.NoError(t, driver.DeleteStore(ctx, newStore.Key))
	})
	metadbtest.AssertEqualStore(t, store, newStore, "CreateStore should return the created store.")
	var newRecord *m.Record
	if record != nil {
		newRecord, err = driver.InsertRecord(ctx, newStore.Key, record)
		if err != nil {
			t.Fatalf("Could not create a new record: %v", err)
		}
		t.Cleanup(func() {
			assert.NoError(t, driver.DeleteRecord(ctx, newStore.Key, newRecord.Key))
		})
		metadbtest.AssertEqualRecord(t, record, newRecord, "GetRecord should return the exact same record.")
	}
	return newStore, newRecord
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
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	record := &m.Record{
		Key:        recordKey,
		Blob:       blob,
		BlobSize:   int64(len(blob)),
		Properties: make(m.PropertyMap),
		OwnerID:    "record owner",
		Tags:       []string{"abc", "def"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("e4a677f6-8f1c-4765-be45-11b6400cc43b"),
		},
	}
	expected := cloneRecord(record)

	updatedRecord, err := driver.UpdateRecord(ctx, storeKey, record)
	assert.NotNil(t, err, "UpdateRecord should return an error if the specified record doesn't exist.")
	assert.Nil(t, updatedRecord, "UpdateRecord should return nil with error")

	setupTestStoreRecord(ctx, t, driver, store, record)

	record.Tags = append(record.Tags, "ghi")
	expected.Tags = append(expected.Tags, "ghi")
	record.OwnerID = "NewOwner"
	expected.OwnerID = record.OwnerID
	record.Timestamps.UpdatedAt = time.Date(1988, 5, 17, 5, 6, 8, int(4321*time.Microsecond), time.UTC)
	record.Timestamps.Signature = uuid.MustParse("3c1dc762-8f22-4d85-b729-b90393f45ca6")
	expected.Timestamps = record.Timestamps

	// Make sure UpdateRecord doesn't update CreatedAt
	record.Timestamps.CreatedAt = time.Unix(0, 0)
	updatedRecord, err = driver.UpdateRecord(ctx, storeKey, record)
	assert.Nilf(t, err, "Failed to update a record (%s) in store (%s): %v", recordKey, storeKey, err)
	metadbtest.AssertEqualRecord(t, expected, updatedRecord, "Updated record is not the same as original.")

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	metadbtest.AssertEqualRecord(t, expected, record2, "GetRecord should fetch the updated record.")
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

	recordKey := newRecordKey()
	createdAt := time.Unix(12345, 0)
	record := &m.Record{
		Key:        recordKey,
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
		Key:        blobKey,
		Size:       12345,
		ObjectName: "test",
		Status:     m.BlobRefStatusInitializing,
		StoreKey:   storeKey,
		RecordKey:  recordKey,
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: origSig,
		},
	}

	insertedBlob, err := driver.InsertBlobRef(ctx, blob)
	if err != nil {
		t.Fatalf("InsertBlobRef failed: %v", err)
	}
	metadbtest.AssertEqualBlobRef(t, blob, insertedBlob)

	_, err = driver.GetCurrentBlobRef(ctx, storeKey, recordKey)
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
			assert.Equal(t, blobKey, promoRecord.ExternalBlob)
			assert.Equal(t, blob.Size, promoRecord.BlobSize)
			assert.True(t, beforePromo.Before(promoRecord.Timestamps.UpdatedAt))
			assert.NotEqual(t, record.Timestamps.Signature, promoRecord.Timestamps.Signature)
		}
		if assert.NotNil(t, promoRecord) {
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
		metadbtest.AssertEqualBlobRef(t, promoBlob, currentBlob)
	}

	if err := driver.DeleteBlobRef(ctx, blobKey); err == nil {
		t.Errorf("DeleteBlobRef should fail on current blob: %v", err)
	} else {
		assert.Equal(t, codes.FailedPrecondition, grpc.Code(err))
	}

	delPendRecord, delPendBlob, err := driver.MarkBlobRefForDeletion(ctx, storeKey, recordKey)
	if err != nil {
		t.Errorf("MarkBlobForDeletion failed: %v", err)
	} else {
		if assert.NotNil(t, delPendRecord) {
			assert.Equal(t, uuid.Nil, delPendRecord.ExternalBlob)
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

	if _, err := driver.InsertBlobRef(ctx, blob); err != nil {
		t.Fatalf("InsertBlobRef failed: %v", err)
	}
	t.Cleanup(func() {
		assert.NoError(t, driver.DeleteBlobRef(ctx, blob.Key))
	})

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
	if _, err := driver.InsertBlobRef(ctx, newBlob); err != nil {
		t.Fatalf("InsertBlobRef failed: %v", err)
	}
	t.Cleanup(func() {
		assert.NoError(t, driver.DeleteBlobRef(ctx, newBlob.Key))
	})

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

	_, _, err = driver.MarkBlobRefForDeletion(ctx, store.Key, record.Key)
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

	if _, err := driver.InsertBlobRef(ctx, blob); err != nil {
		t.Fatalf("InsertBlobRef failed: %v", err)
	}
	t.Cleanup(func() {
		assert.NoError(t, driver.DeleteBlobRef(ctx, blob.Key))
	})

	assert.NoError(t, blob.Fail())
	blob.Timestamps.UpdatedAt = time.Unix(234, 0)

	updatedBlob, err := driver.UpdateBlobRef(ctx, blob)
	if err != nil {
		t.Errorf("UpdateBlobRef failed: %v", err)
	} else {
		if assert.NotNil(t, updatedBlob) {
			assert.Equal(t, blob, updatedBlob)
		}
	}

	receivedBlob, err := driver.GetBlobRef(ctx, blob.Key)
	if err != nil {
		t.Errorf("GetBlobRef failed: %v", err)
	} else {
		if assert.NotNil(t, receivedBlob) {
			assert.Equal(t, blob, receivedBlob)
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
		t.Error("InsertBlob should fail for a non-existent record.")
		assert.NoError(t, driver.DeleteBlobRef(ctx, blob.Key))
	} else {
		assert.Equal(t, codes.FailedPrecondition, grpc.Code(err))
		assert.Nil(t, insertedBlob)
	}
}
