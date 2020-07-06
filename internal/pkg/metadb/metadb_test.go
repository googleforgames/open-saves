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

	"github.com/golang/mock/gomock"
	m "github.com/googleforgames/triton/internal/pkg/metadb"
	mock_metadb "github.com/googleforgames/triton/internal/pkg/metadb/mock"
	"github.com/stretchr/testify/assert"
)

func TestMetaDB_NewMetaDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDriver := mock_metadb.NewMockDriver(ctrl)
	metadb := m.NewMetaDB(mockDriver)
	assert.NotNil(t, metadb, "NewMetaDB() should return a non-nil instance.")
}

func TestMetaDB_DriverCalls(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockDriver := mock_metadb.NewMockDriver(ctrl)
	store := new(m.Store)
	record := new(m.Record)
	const (
		key  = "random key string"
		key2 = "another key string"
		name = "random name"
	)

	metadb := m.NewMetaDB(mockDriver)

	mockDriver.EXPECT().Connect(ctx)
	assert.NoError(t, metadb.Connect(ctx))

	mockDriver.EXPECT().Disconnect(ctx)
	assert.NoError(t, metadb.Disconnect(ctx))

	mockDriver.EXPECT().CreateStore(ctx, store)
	assert.NoError(t, metadb.CreateStore(ctx, store))

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

	mockDriver.EXPECT().InsertRecord(ctx, key, record)
	assert.NoError(t, metadb.InsertRecord(ctx, key, record))

	mockDriver.EXPECT().UpdateRecord(ctx, key, record)
	assert.NoError(t, metadb.UpdateRecord(ctx, key, record))

	mockDriver.EXPECT().GetRecord(ctx, key, key2).Return(record, nil)
	actualrecord, err := metadb.GetRecord(ctx, key, key2)
	assert.Same(t, actualrecord, record)
	assert.NoError(t, err)

	mockDriver.EXPECT().DeleteRecord(ctx, key, key2)
	assert.NoError(t, metadb.DeleteRecord(ctx, key, key2))
}
