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

package metadb

import (
	"context"
	"time"
)

// Driver interface defines common operations for the metadata server.
// MetaDB uses this interface to perform actual operations on the backend service.
type Driver interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error

	CreateStore(ctx context.Context, store *Store) (*Store, error)
	GetStore(ctx context.Context, key string) (*Store, error)
	FindStoreByName(ctx context.Context, name string) (*Store, error)
	DeleteStore(ctx context.Context, key string) error

	InsertRecord(ctx context.Context, storeKey string, record *Record) (*Record, error)
	UpdateRecord(ctx context.Context, storeKey string, record *Record) (*Record, error)
	GetRecord(ctx context.Context, storeKey, key string) (*Record, error)
	DeleteRecord(ctx context.Context, storeKey, key string) error

	TimestampPrecision() time.Duration
}

// MetaDB is a metadata database manager of Triton.
// It performs operations through the Driver interface.
type MetaDB struct {
	driver Driver
}

// NewMetaDB creates a new MetaDB instance with an initialized database Driver.
func NewMetaDB(driver Driver) *MetaDB {
	return &MetaDB{driver: driver}
}

// Connect initiates the database connection.
func (m *MetaDB) Connect(ctx context.Context) error {
	return m.driver.Connect(ctx)
}

// Disconnect terminates the database connection.
// Make sure to call this method to release resources (e.g. using defer).
// The MetaDB instance will not be available after Disconnect().
func (m *MetaDB) Disconnect(ctx context.Context) error {
	return m.driver.Disconnect(ctx)
}

// CreateStore creates a new store.
func (m *MetaDB) CreateStore(ctx context.Context, store *Store) (*Store, error) {
	store.Timestamps.NewTimestamps(m.driver.TimestampPrecision())
	return m.driver.CreateStore(ctx, store)
}

// GetStore fetches a store based on the key provided.
// Returns error if the key is not found.
func (m *MetaDB) GetStore(ctx context.Context, key string) (*Store, error) {
	return m.driver.GetStore(ctx, key)
}

// FindStoreByName finds and fetch a store based on the name (complete match).
func (m *MetaDB) FindStoreByName(ctx context.Context, name string) (*Store, error) {
	return m.driver.FindStoreByName(ctx, name)
}

// DeleteStore deletes the store with specified key.
// Returns error if the store has any child records.
func (m *MetaDB) DeleteStore(ctx context.Context, key string) error {
	return m.driver.DeleteStore(ctx, key)
}

// InsertRecord creates a new Record in the store specified with storeKey.
// Returns error if there is already a record with the same key.
func (m *MetaDB) InsertRecord(ctx context.Context, storeKey string, record *Record) (*Record, error) {
	record.Timestamps.NewTimestamps(m.driver.TimestampPrecision())
	return m.driver.InsertRecord(ctx, storeKey, record)
}

// UpdateRecord updates the record in the store specified with storeKey.
// Returns error if the store doesn't have a record with the key provided.
func (m *MetaDB) UpdateRecord(ctx context.Context, storeKey string, record *Record) (*Record, error) {
	record.Timestamps.UpdateTimestamps(m.driver.TimestampPrecision())
	return m.driver.UpdateRecord(ctx, storeKey, record)
}

// GetRecord fetches and returns a record with key in store storeKey.
// Returns error if not found.
func (m *MetaDB) GetRecord(ctx context.Context, storeKey, key string) (*Record, error) {
	return m.driver.GetRecord(ctx, storeKey, key)
}

// DeleteRecord deletes a record with key in store storeKey.
// It doesn't return error even if the key is not found in the database.
func (m *MetaDB) DeleteRecord(ctx context.Context, storeKey, key string) error {
	return m.driver.DeleteRecord(ctx, storeKey, key)
}
