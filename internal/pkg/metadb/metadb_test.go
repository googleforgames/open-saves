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

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	mock_metadb "github.com/googleforgames/open-saves/internal/pkg/metadb/mock"
	"github.com/stretchr/testify/assert"
)

func TestMetaDB_NewMetaDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDriver := mock_metadb.NewMockDriver(ctrl)
	metadb := m.NewMetaDB(mockDriver)
	assert.NotNil(t, metadb, "NewMetaDB() should return a non-nil instance.")
}

// Checks if actual is equal to or after expected, within d.
func timeEqualOrAfter(expected, actual time.Time, d time.Duration) bool {
	if expected.Equal(actual) {
		return true
	}
	return expected.Before(actual) && actual.Sub(expected) < d
}

func TestMetaDB_DriverCalls(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockDriver := mock_metadb.NewMockDriver(ctrl)
	store := new(m.Store)
	record := new(m.Record)
	const (
		key           = "random key string"
		key2          = "another key string"
		name          = "random name"
		timeThreshold = 1 * time.Second
	)
	aUUID := uuid.New()

	metadb := m.NewMetaDB(mockDriver)
	mockDriver.EXPECT().TimestampPrecision().AnyTimes().Return(time.Microsecond)

	mockDriver.EXPECT().Connect(ctx)
	assert.NoError(t, metadb.Connect(ctx))

	mockDriver.EXPECT().Disconnect(ctx)
	assert.NoError(t, metadb.Disconnect(ctx))

	beforeCreate := time.Now()
	mockDriver.EXPECT().CreateStore(ctx, store).Return(store, nil)
	createdStore, err := metadb.CreateStore(ctx, store)
	assert.NoError(t, err)
	assert.NotNil(t, createdStore)
	assert.True(t, timeEqualOrAfter(beforeCreate, createdStore.Timestamps.CreatedAt, timeThreshold))
	assert.True(t, timeEqualOrAfter(beforeCreate, createdStore.Timestamps.UpdatedAt, timeThreshold))

	mockDriver.EXPECT().GetStore(ctx, key).Return(store, nil)
	actualstore, err := metadb.GetStore(ctx, key)
	assert.Same(t, actualstore, store)
	assert.NoError(t, err)

	mockDriver.EXPECT().FindStoreByName(ctx, name).Return(store, nil)
	actualstore, err = metadb.FindStoreByName(ctx, name)
	assert.Same(t, store, actualstore)
	assert.NoError(t, err)

	mockDriver.EXPECT().DeleteStore(ctx, key)
	assert.NoError(t, metadb.DeleteStore(ctx, key))

	beforeInsert := time.Now()
	mockDriver.EXPECT().InsertRecord(ctx, key, record).Return(record, nil)
	insertedRecord, err := metadb.InsertRecord(ctx, key, record)
	assert.NoError(t, err)
	assert.NotNil(t, insertedRecord)
	assert.True(t, timeEqualOrAfter(beforeInsert, insertedRecord.Timestamps.CreatedAt, timeThreshold))
	assert.True(t, timeEqualOrAfter(beforeInsert, insertedRecord.Timestamps.UpdatedAt, timeThreshold))

	createdAt := record.Timestamps.CreatedAt
	mockDriver.EXPECT().UpdateRecord(ctx, key, record).Return(record, nil)
	updatedRecord, err := metadb.UpdateRecord(ctx, key, record)
	assert.NoError(t, err)
	assert.NotNil(t, updatedRecord)
	assert.True(t, createdAt.Equal(updatedRecord.Timestamps.CreatedAt))
	assert.True(t, timeEqualOrAfter(createdAt, updatedRecord.Timestamps.UpdatedAt, timeThreshold))

	mockDriver.EXPECT().GetRecord(ctx, key, key2).Return(record, nil)
	actualrecord, err := metadb.GetRecord(ctx, key, key2)
	assert.Same(t, actualrecord, record)
	assert.NoError(t, err)

	mockDriver.EXPECT().DeleteRecord(ctx, key, key2)
	assert.NoError(t, metadb.DeleteRecord(ctx, key, key2))

	blob := new(m.BlobRef)

	mockDriver.EXPECT().InsertBlobRef(ctx, blob).Return(blob, nil)
	actualBlob, err := metadb.InsertBlobRef(ctx, blob)
	assert.Same(t, blob, actualBlob)
	assert.NoError(t, err)

	mockDriver.EXPECT().UpdateBlobRef(ctx, blob).Return(blob, nil)
	actualBlob, err = metadb.UpdateBlobRef(ctx, blob)
	assert.Same(t, blob, actualBlob)
	assert.NoError(t, err)

	mockDriver.EXPECT().GetBlobRef(ctx, aUUID).Return(blob, nil)
	actualBlob, err = metadb.GetBlobRef(ctx, aUUID)
	assert.Same(t, blob, actualBlob)
	assert.NoError(t, err)

	mockDriver.EXPECT().GetCurrentBlobRef(ctx, key, key2).Return(blob, nil)
	actualBlob, err = metadb.GetCurrentBlobRef(ctx, key, key2)
	assert.Same(t, blob, actualBlob)
	assert.NoError(t, err)

	mockDriver.EXPECT().PromoteBlobRefToCurrent(ctx, blob).Return(record, blob, nil)
	actualrecord, actualBlob, err = metadb.PromoteBlobRefToCurrent(ctx, blob)
	assert.Same(t, record, actualrecord)
	assert.Same(t, blob, actualBlob)
	assert.NoError(t, err)

	mockDriver.EXPECT().MarkBlobRefForDeletion(ctx, key, key2).Return(record, blob, nil)
	actualrecord, actualBlob, err = metadb.MarkBlobRefForDeletion(ctx, key, key2)
	assert.Same(t, record, actualrecord)
	assert.Same(t, blob, actualBlob)
	assert.NoError(t, err)

	mockDriver.EXPECT().DeleteBlobRef(ctx, aUUID)
	assert.NoError(t, metadb.DeleteBlobRef(ctx, aUUID))
}
