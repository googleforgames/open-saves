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

	"github.com/google/uuid"
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

	InsertBlobRef(ctx context.Context, blob *BlobRef) (*BlobRef, error)
	UpdateBlobRef(ctx context.Context, blob *BlobRef) (*BlobRef, error)
	GetBlobRef(ctx context.Context, key uuid.UUID) (*BlobRef, error)
	GetCurrentBlobRef(ctx context.Context, storeKey, recordKey string) (*BlobRef, error)
	PromoteBlobRefToCurrent(ctx context.Context, blob *BlobRef) (*Record, *BlobRef, error)
	MarkBlobRefForDeletion(ctx context.Context, storeKey string, recordKey string) (*Record, *BlobRef, error)
	DeleteBlobRef(ctx context.Context, key uuid.UUID) error

	TimestampPrecision() time.Duration
}

// MetaDB is a metadata database manager of Open Saves.
// It performs operations through the Driver interface.
// The methods return gRPC error codes. Here are some common error codes
// returned. For additional details, please look at the method help.
// Common errors:
//	- NotFound: entity or object is not found
//	- Aborted: transaction is aborted
//	- InvalidArgument: key or value provided is not valid
//	- Internal: internal unrecoverable error
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

// InsertBlobRef inserts a new BlobRef object to the datastore.
func (m *MetaDB) InsertBlobRef(ctx context.Context, blob *BlobRef) (*BlobRef, error) {
	blob.Timestamps.NewTimestamps(m.driver.TimestampPrecision())
	return m.driver.InsertBlobRef(ctx, blob)
}

// UpdateBlobRef updates a BlobRef object with the new property values.
// Returns a NotFound error if the key is not found.
func (m *MetaDB) UpdateBlobRef(ctx context.Context, blob *BlobRef) (*BlobRef, error) {
	blob.Timestamps.UpdateTimestamps(m.driver.TimestampPrecision())
	return m.driver.UpdateBlobRef(ctx, blob)
}

// GetBlobRef returns a BlobRef object specified by the key.
// Returns errors:
//	- NotFound: the object is not found.
func (m *MetaDB) GetBlobRef(ctx context.Context, key uuid.UUID) (*BlobRef, error) {
	return m.driver.GetBlobRef(ctx, key)
}

// GetCurrentBlobRef gets a BlobRef object associated with a record.
// Returned errors:
// 	- NotFound: the record is not found.
// 	- FailedPrecondition: the record doesn't have a blob.
func (m *MetaDB) GetCurrentBlobRef(ctx context.Context, storeKey, recordKey string) (*BlobRef, error) {
	return m.driver.GetCurrentBlobRef(ctx, storeKey, recordKey)
}

// PromoteBlobRefToCurrent promotes the provided BlobRef object as a current
// external blob reference.
// Returned errors:
//	- NotFound: the specified record or the blobref was not found
//  - Internal: BlobRef status transition error
func (m *MetaDB) PromoteBlobRefToCurrent(ctx context.Context, blob *BlobRef) (*Record, *BlobRef, error) {
	return m.driver.PromoteBlobRefToCurrent(ctx, blob)
}

// MarkBlobRefForDeletion removes the ExternalBlob from the record specified by
// storeKey and recordKey. It also changes the status of the blob object to
// BlobRefStatusPendingDeletion.
// Returned errors:
//	- NotFound: the specified record or the blobref was not found
//	- FailedPrecondition: the record doesn't have an external blob
//  - Internal: BlobRef status transition error
func (m *MetaDB) MarkBlobRefForDeletion(ctx context.Context, storeKey string, recordKey string) (*Record, *BlobRef, error) {
	return m.driver.MarkBlobRefForDeletion(ctx, storeKey, recordKey)
}

// DeleteBlobRef deletes the BlobRef object from the database.
// Returned errors:
//	- NotFound: the blobref object is not found
//	- FailedPrecondition: the blobref status is Ready and can't be deleted
func (m *MetaDB) DeleteBlobRef(ctx context.Context, key uuid.UUID) error {
	return m.driver.DeleteBlobRef(ctx, key)
}
