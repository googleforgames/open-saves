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
	// The mock driver cannot verify the function argument
	// due to a technical limitation: https://github.com/golang/mock/issues/324
	// Passing a nil as a workaround.
	mockDriver.EXPECT().UpdateRecord(ctx, key, key2, nil).Return(record, nil)
	updatedRecord, err := metadb.UpdateRecord(ctx, key, key2, nil)
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
}
