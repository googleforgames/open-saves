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
	"errors"
	"fmt"
	"strconv"

	"github.com/googleforgames/open-saves/internal/pkg/tracing"
	"go.opentelemetry.io/otel"

	ds "cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/googleforgames/open-saves/api"
)

const (
	storeKind  = "store"
	recordKind = "record"
	blobKind   = "blob"
	chunkKind  = "chunk"

	propertiesField = "Properties"
	tagsField       = "Tags"
	ownerField      = "OwnerID"
)

var (
	ErrNoUpdate = errors.New("UpdateRecord doesn't need to commit the change")
)

// MetaDB is a metadata database manager of Open Saves.
// The methods return gRPC error codes. Here are some common error codes
// returned. For additional details, please look at the method help.
// Common errors:
//   - NotFound: entity or object is not found
//   - Aborted: transaction is aborted
//   - InvalidArgument: key or value provided is not valid
//   - Internal: internal unrecoverable error
type MetaDB struct {
	// Datastore namespace for multi-tenancy
	Namespace string

	client *ds.Client
}

// RecordUpdater is a callback function for record updates.
// Returning a non-nil error aborts the transaction.
type RecordUpdater func(record *record.Record) (*record.Record, error)

// removeInlineBlob removes inline blob related attributes from the Record.
func removeInlineBlob(r *record.Record) *record.Record {
	r.Blob = nil
	r.BlobSize = 0
	r.MD5 = nil
	r.ResetCRC32C()
	return r
}

// NewMetaDB creates a new MetaDB instance with an initialized database client.
func NewMetaDB(ctx context.Context, projectID string, opts ...option.ClientOption) (*MetaDB, error) {
	client, err := ds.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return &MetaDB{client: client}, nil
}

func (m *MetaDB) newQuery(kind string) *ds.Query {
	if m.Namespace != "" {
		return ds.NewQuery(kind).Namespace(m.Namespace)
	}
	return ds.NewQuery(kind)
}

func (m *MetaDB) createStoreKey(key string) *ds.Key {
	k := ds.NameKey(storeKind, key, nil)
	k.Namespace = m.Namespace
	return k
}

func (m *MetaDB) createRecordKey(storeKey, key string) *ds.Key {
	sk := m.createStoreKey(storeKey)
	sk.Namespace = m.Namespace
	rk := ds.NameKey(recordKind, key, sk)
	rk.Namespace = m.Namespace
	return rk
}

func (d *MetaDB) createBlobKey(key uuid.UUID) *ds.Key {
	k := ds.NameKey(blobKind, key.String(), nil)
	k.Namespace = d.Namespace
	return k
}

func (m *MetaDB) createChunkRefKey(blobRefKey, key uuid.UUID) *ds.Key {
	bk := m.createBlobKey(blobRefKey)
	bk.Namespace = m.Namespace
	ck := ds.NameKey(chunkKind, key.String(), bk)
	ck.Namespace = m.Namespace
	return ck
}

func (m *MetaDB) getBlobRef(ctx context.Context, tx *ds.Transaction, key uuid.UUID) (*blobref.BlobRef, error) {
	if key == uuid.Nil {
		return nil, status.Error(codes.FailedPrecondition, "there is no external blob associated")
	}
	blob := new(blobref.BlobRef)
	if tx == nil {
		if err := m.client.Get(ctx, m.createBlobKey(key), blob); err != nil {
			return nil, datastoreErrToGRPCStatus(err)
		}
	} else {
		if err := tx.Get(m.createBlobKey(key), blob); err != nil {
			return nil, datastoreErrToGRPCStatus(err)
		}
	}
	return blob, nil
}

// Returns a modified Record and the caller must commit the change.
func (m *MetaDB) markBlobRefForDeletion(tx *ds.Transaction,
	record *record.Record, blob *blobref.BlobRef, newBlobKey uuid.UUID) (*record.Record, error) {
	if record.ExternalBlob == uuid.Nil {
		return nil, status.Error(codes.FailedPrecondition, "the record doesn't have an external blob associated")
	}
	record.ExternalBlob = newBlobKey
	record.Timestamps.Update()
	if blob.MarkForDeletion() != nil {
		blob.Fail()
		return nil, status.Errorf(codes.Internal, "failed to transition the blob state for deletion: current = %v", blob.Status)
	}
	// Mark the blob as expired so the TTL takes care of deletion.
	blob.MarkAsExpired()
	_, err := tx.Mutate(ds.NewUpdate(m.createBlobKey(blob.Key), blob))
	return record, err
}

func (m *MetaDB) mutateSingleInTransaction(tx *ds.Transaction, mut *ds.Mutation) error {
	_, err := tx.Mutate(mut)
	if err != nil {
		if merr, ok := err.(ds.MultiError); ok {
			err = merr[0]
		}
		return datastoreErrToGRPCStatus(err)
	}
	return nil
}

func (m *MetaDB) mutateSingle(ctx context.Context, mut *ds.Mutation) error {
	_, err := m.client.Mutate(ctx, mut)
	if err != nil {
		if merr, ok := err.(ds.MultiError); ok {
			err = merr[0]
		}
		return datastoreErrToGRPCStatus(err)
	}
	return nil
}

func (m *MetaDB) recordExists(ctx context.Context, tx *ds.Transaction, key *ds.Key) (bool, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.recordExists")
	defer span.End()

	query := ds.NewQuery(recordKind).Namespace(m.Namespace).
		KeysOnly().FilterField("__key__", "=", key).Limit(1)
	if tx != nil {
		query = query.Transaction(tx)
	}
	iter := m.client.Run(ctx, query)
	if _, err := iter.Next(nil); err == iterator.Done {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// Disconnect terminates the database connection.
// Make sure to call this method to release resources (e.g. using defer).
// The MetaDB instance will not be available after Disconnect().
func (m *MetaDB) Disconnect(ctx context.Context) error {
	return datastoreErrToGRPCStatus(m.client.Close())
}

// CreateStore creates a new store.
func (m *MetaDB) CreateStore(ctx context.Context, store *store.Store) (*store.Store, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.CreateStore")
	defer span.End()

	store.Timestamps = timestamps.New()
	key := m.createStoreKey(store.Key)
	mut := ds.NewInsert(key, store)
	if err := m.mutateSingle(ctx, mut); err != nil {
		return nil, err
	}
	return store, nil
}

// GetStore fetches a store based on the key provided.
// Returns error if the key is not found.
func (m *MetaDB) GetStore(ctx context.Context, key string) (*store.Store, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.GetStore")
	defer span.End()

	dskey := m.createStoreKey(key)
	store := new(store.Store)
	err := m.client.Get(ctx, dskey, store)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return store, nil
}

// FindStoreByName finds and fetch a store based on the name (complete match).
func (m *MetaDB) FindStoreByName(ctx context.Context, name string) (*store.Store, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.FindStoreByName")
	defer span.End()

	query := m.newQuery(storeKind).Filter("Name =", name)
	iter := m.client.Run(ctx, query)
	store := new(store.Store)
	_, err := iter.Next(store)
	if err != nil {
		return nil, status.Errorf(codes.NotFound,
			"Store (name=%s) was not found.", name)
	}
	return store, nil
}

// DeleteStore deletes the store with specified key.
// Returns error if the store has any child records.
func (m *MetaDB) DeleteStore(ctx context.Context, key string) error {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.DeleteStore")
	defer span.End()

	dskey := m.createStoreKey(key)

	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		query := ds.NewQuery(recordKind).Transaction(tx).KeysOnly().
			Ancestor(dskey).Limit(1).Namespace(m.Namespace)
		iter := m.client.Run(ctx, query)
		_, err := iter.Next(nil)
		if err != iterator.Done {
			return status.Errorf(codes.FailedPrecondition,
				"DeleteStore was called for a non-empty store (%s)", key)
		}
		return tx.Delete(dskey)
	})
	if err != nil {
		return datastoreErrToGRPCStatus(err)
	}
	return nil
}

// InsertRecord creates a new Record in the store specified with storeKey.
// Returns error if there is already a record with the same key.
func (m *MetaDB) InsertRecord(ctx context.Context, storeKey string, record *record.Record) (*record.Record, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.InsertRecord")
	defer span.End()

	record.Timestamps = timestamps.New()
	record.StoreKey = storeKey
	rkey := m.createRecordKey(storeKey, record.Key)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		dskey := m.createStoreKey(storeKey)
		query := ds.NewQuery(storeKind).Transaction(tx).Namespace(m.Namespace).
			KeysOnly().FilterField("__key__", "=", dskey).Limit(1)
		iter := m.client.Run(ctx, query)
		_, err := iter.Next(nil)
		if err == iterator.Done {
			return status.Errorf(codes.FailedPrecondition,
				"InsertRecord was called with a non-existent store (%s)", storeKey)
		}
		mut := ds.NewInsert(rkey, record)
		return m.mutateSingleInTransaction(tx, mut)
	})
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return record, nil
}

// UpdateRecord updates the record in the store specified with storeKey.
// Pass a callback function to updater and change values there. The callback
// will be protected by a transaction.
// Returns error if the store doesn't have a record with the key provided.
func (m *MetaDB) UpdateRecord(ctx context.Context, storeKey string, key string, updater RecordUpdater) (*record.Record, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.UpdateRecord")
	defer span.End()

	if updater == nil {
		return nil, status.Errorf(codes.Internal, "updater cannot be nil")
	}
	var toUpdate *record.Record
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		rkey := m.createRecordKey(storeKey, key)

		// TODO(yuryu): Consider supporting transactions in MetaDB and move
		// this operation out of the Datastore specific code.
		toUpdate = new(record.Record)
		if err := tx.Get(rkey, toUpdate); err != nil {
			return err
		}

		oldExternalBlob := toUpdate.ExternalBlob

		// Update the record entry by calling the updater callback.
		var err error
		toUpdate, err = updater(toUpdate)
		if err != nil {
			return err
		}

		if oldExternalBlob != toUpdate.ExternalBlob {
			return status.Error(codes.Internal, "UpdateRecord: ExternalBlob must not be modified in UpdateRecord")
		}
		if oldExternalBlob != uuid.Nil {
			oldBlob, err := m.getBlobRef(ctx, tx, toUpdate.ExternalBlob)
			if err != nil {
				return err
			}
			// Deassociate the old blob if an external blob is associated, and a new inline blob is being added.
			if len(toUpdate.Blob) > 0 {
				toUpdate, err = m.markBlobRefForDeletion(tx, toUpdate, oldBlob, uuid.Nil)
				if err != nil {
					return err
				}
			// Update the external blob with the new ExpiresAt coming from the record.
			// Will update only if the incoming update changes the expiresAt value and it differs from the oldBlob one.
			} else if !toUpdate.ExpiresAt.IsZero() && oldBlob.ExpiresAt != toUpdate.ExpiresAt {
				oldBlob.ExpiresAt = toUpdate.ExpiresAt
				err = m.mutateSingleInTransaction(tx, ds.NewUpdate(m.createBlobKey(toUpdate.ExternalBlob), oldBlob))
				if err != nil {
					return err
				}
			}
		}

		toUpdate.Timestamps.Update()
		return m.mutateSingleInTransaction(tx, ds.NewUpdate(rkey, toUpdate))
	})
	// ErrNoUpdate is expected and not treated as an error.
	if err != nil && err != ErrNoUpdate {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return toUpdate, nil
}

// GetRecord fetches and returns a record with key in store storeKey.
// Returns error if not found.
func (m *MetaDB) GetRecord(ctx context.Context, storeKey, key string) (*record.Record, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.GetRecord")
	defer span.End()

	rkey := m.createRecordKey(storeKey, key)
	record := new(record.Record)
	if err := m.client.Get(ctx, rkey, record); err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return record, nil
}

// DeleteRecord deletes a record with key in store storeKey.
// It doesn't return error even if the key is not found in the database.
func (m *MetaDB) DeleteRecord(ctx context.Context, storeKey, key string) error {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.DeleteRecord")
	defer span.End()

	rkey := m.createRecordKey(storeKey, key)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		record := new(record.Record)
		if err := tx.Get(rkey, record); err != nil {
			if err == ds.ErrNoSuchEntity {
				// Exit the transaction as DeleteRecord should ignore a not found error.
				return nil
			}
			return err
		}
		if record.ExternalBlob != uuid.Nil {
			blob, err := m.getBlobRef(ctx, tx, record.ExternalBlob)
			if err == nil {
				_, err = m.markBlobRefForDeletion(tx, record, blob, uuid.Nil)
				if err != nil {
					return err
				}
			} else if status.Code(err) != codes.NotFound {
				return err
			}
		}
		return m.mutateSingleInTransaction(tx, ds.NewDelete(rkey))
	})
	return datastoreErrToGRPCStatus(err)
}

// InsertBlobRef inserts a new BlobRef object to the datastore.
func (m *MetaDB) InsertBlobRef(ctx context.Context, blob *blobref.BlobRef) (*blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.InsertBlobRef")
	defer span.End()

	blob.Timestamps = timestamps.New()
	rkey := m.createRecordKey(blob.StoreKey, blob.RecordKey)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		if exists, err := m.recordExists(ctx, tx, rkey); err != nil {
			return err
		} else if !exists {
			return status.Error(codes.FailedPrecondition, "InsertBlob was called for a non-exitent record")
		}
		_, err := tx.Mutate(ds.NewInsert(m.createBlobKey(blob.Key), blob))
		return err
	})
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return blob, nil
}

// UpdateBlobRef updates a BlobRef object with the new property values.
// Returns a NotFound error if the key is not found.
func (m *MetaDB) UpdateBlobRef(ctx context.Context, blob *blobref.BlobRef) (*blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.UpdateBlobRef")
	defer span.End()

	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		oldBlob, err := m.getBlobRef(ctx, tx, blob.Key)
		if err != nil {
			return err
		}
		blob.Timestamps.CreatedAt = oldBlob.Timestamps.CreatedAt
		mut := ds.NewUpdate(m.createBlobKey(blob.Key), blob)
		_, err = tx.Mutate(mut)
		return err
	})

	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return blob, nil
}

// GetBlobRef returns a BlobRef object specified by the key.
// Returns errors:
//   - NotFound: the object is not found.
func (m *MetaDB) GetBlobRef(ctx context.Context, key uuid.UUID) (*blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.GetBlobRef")
	defer span.End()

	return m.getBlobRef(ctx, nil, key)
}

func (m *MetaDB) getCurrentBlobRef(ctx context.Context, tx *ds.Transaction, storeKey, recordKey string) (*blobref.BlobRef, error) {
	record := new(record.Record)
	err := tx.Get(m.createRecordKey(storeKey, recordKey), record)
	if err != nil {
		return nil, err
	}
	blob, err := m.getBlobRef(ctx, tx, record.ExternalBlob)
	return blob, err
}

// GetCurrentBlobRef gets a BlobRef object associated with a record.
// Returned errors:
//   - NotFound: the record is not found.
//   - FailedPrecondition: the record doesn't have a blob.
func (m *MetaDB) GetCurrentBlobRef(ctx context.Context, storeKey, recordKey string) (*blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.GetCurrentBlobRef")
	defer span.End()

	var blob *blobref.BlobRef
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		var err error
		blob, err = m.getCurrentBlobRef(ctx, tx, storeKey, recordKey)
		return err
	}, ds.ReadOnly)
	return blob, datastoreErrToGRPCStatus(err)
}

func (m *MetaDB) getChildChunks(ctx context.Context, tx *ds.Transaction, blob *blobref.BlobRef) ([]*chunkref.ChunkRef, error) {
	query := m.newQuery(chunkKind).Transaction(tx).Ancestor(m.createBlobKey(blob.Key))
	iter := m.client.Run(ctx, query)
	chunks := []*chunkref.ChunkRef{}
	for {
		chunk := new(chunkref.ChunkRef)
		_, err := iter.Next(chunk)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// return size, chunk count, error
func (m *MetaDB) chunkObjectsSizeSum(ctx context.Context, tx *ds.Transaction, blob *blobref.BlobRef) (int64, int64, error) {
	chunks, err := m.getChildChunks(ctx, tx, blob)
	if err != nil {
		return 0, 0, err
	}
	size := int64(0)
	for _, chunk := range chunks {
		size += int64(chunk.Size)
	}
	return size, int64(len(chunks)), nil
}

// PromoteBlobRefToCurrent promotes the provided BlobRef object as a current
// external blob reference.
// Returned errors:
//   - NotFound: the specified record or the blobref was not found
//   - Internal: BlobRef status transition error
func (m *MetaDB) PromoteBlobRefToCurrent(ctx context.Context, blob *blobref.BlobRef) (*record.Record, *blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.PromoteBlobRefToCurrent")
	defer span.End()

	record := new(record.Record)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		rkey := m.createRecordKey(blob.StoreKey, blob.RecordKey)
		if err := tx.Get(rkey, record); err != nil {
			return err
		}
		if record.ExternalBlob == uuid.Nil {
			// Simply add the new blob if previously didn't have a blob
			record = removeInlineBlob(record)
		} else {
			// Mark previous blob for deletion
			oldBlob, err := m.getBlobRef(ctx, tx, record.ExternalBlob)
			if err != nil {
				return err
			}
			record, err = m.markBlobRefForDeletion(tx, record, oldBlob, blob.Key)
			if err != nil {
				return err
			}
		}

		// Update the blob size for chunked uploads
		if blob.Chunked {
			// TODO(yuryu): should check if chunks are continuous?
			size, count, err := m.chunkObjectsSizeSum(ctx, tx, blob)
			if err != nil {
				return err
			}
			if blob.ChunkCount != 0 && blob.ChunkCount != count {
				return status.Errorf(codes.FailedPrecondition, "expected chunk count doesn't match: expected (%v), actual (%v)", blob.ChunkCount, count)
			}
			blob.Size = size
			blob.ChunkCount = count
		}
		if blob.Status != blobref.StatusReady {
			if blob.Ready() != nil {
				return status.Error(codes.Internal, "blob is not ready to become current")
			}
		}
		blob.Timestamps.Update()
		// Set the expiration time of the new BlobRef to be the same as the Record
		blob.ExpiresAt = record.ExpiresAt
		if _, err := tx.Mutate(ds.NewUpdate(m.createBlobKey(blob.Key), blob)); err != nil {
			return err
		}

		record.BlobSize = blob.Size
		record.ExternalBlob = blob.Key
		record.Chunked = blob.Chunked
		record.ChunkCount = blob.ChunkCount
		record.Timestamps.Update()
		if err := m.mutateSingleInTransaction(tx, ds.NewUpdate(rkey, record)); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, nil, datastoreErrToGRPCStatus(err)
	}
	return record, blob, nil
}

// PromoteBlobRefWithRecordUpdater promotes the provided BlobRef object as a current
// external blob reference and updates a record in one transaction.
// Returned errors:
//   - NotFound: the specified record or the blobref was not found
//   - Internal: BlobRef status transition error
func (m *MetaDB) PromoteBlobRefWithRecordUpdater(ctx context.Context, blob *blobref.BlobRef, updateTo *record.Record, updater RecordUpdater) (*record.Record, *blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.PromoteBlobRefWithRecordUpdater")
	defer span.End()

	record := new(record.Record)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		rkey := m.createRecordKey(blob.StoreKey, blob.RecordKey)
		if err := tx.Get(rkey, record); err != nil {
			return err
		}
		if updateTo.Timestamps.Signature != uuid.Nil && record.Timestamps.Signature != updateTo.Timestamps.Signature {
			return status.Errorf(codes.Aborted, "Signature mismatch: expected (%v), actual (%v)",
				updateTo.Timestamps.Signature.String(), record.Timestamps.Signature.String())
		}
		if record.ExternalBlob == uuid.Nil {
			// Simply add the new blob if previously didn't have a blob
			record = removeInlineBlob(record)
		} else {
			// Mark previous blob for deletion
			oldBlob, err := m.getBlobRef(ctx, tx, record.ExternalBlob)
			if err != nil {
				return err
			}
			record, err = m.markBlobRefForDeletion(tx, record, oldBlob, blob.Key)
			if err != nil {
				return err
			}
		}

		// Call the custom defined updater method as well to modify the record.
		record, err := updater(record)
		if err != nil {
			return err
		}

		// Update the blob size for chunked uploads
		if blob.Chunked {
			size, count, err := m.chunkObjectsSizeSum(ctx, tx, blob)
			if err != nil {
				return err
			}
			if blob.ChunkCount != 0 && blob.ChunkCount != count {
				return status.Errorf(codes.FailedPrecondition, "expected chunk count doesn't match: expected (%v), actual (%v)", blob.ChunkCount, count)
			}
			blob.ChunkCount = count
			blob.Size = size
		}
		if blob.Status != blobref.StatusReady {
			if blob.Ready() != nil {
				return status.Error(codes.Internal, "blob is not ready to become current")
			}
		}
		blob.Timestamps.Update()

		// Set the expiration time of the new BlobRef to be the same as the Record
		blob.ExpiresAt = record.ExpiresAt
		if _, err := tx.Mutate(ds.NewUpdate(m.createBlobKey(blob.Key), blob)); err != nil {
			return err
		}

		// Update the record with the updated blob info.
		record.ChunkCount = blob.ChunkCount
		record.BlobSize = blob.Size
		if err := m.mutateSingleInTransaction(tx, ds.NewUpdate(rkey, record)); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, nil, datastoreErrToGRPCStatus(err)
	}
	return record, blob, nil
}

// RemoveBlobFromRecord removes the ExternalBlob from the record specified by
// storeKey and recordKey. It also changes the status of the blob object to
// BlobRefStatusPendingDeletion.
// Returned errors:
//   - NotFound: the specified record or the blobref was not found
//   - FailedPrecondition: the record doesn't have an external blob
//   - Internal: BlobRef status transition error
func (m *MetaDB) RemoveBlobFromRecord(ctx context.Context, storeKey string, recordKey string) (*record.Record, *blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.RemoveBlobFromRecord")
	defer span.End()

	rkey := m.createRecordKey(storeKey, recordKey)
	blob := new(blobref.BlobRef)
	record := new(record.Record)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		err := tx.Get(rkey, record)
		if err != nil {
			return err
		}

		record.BlobSize = 0
		record.Chunked = false
		record.ChunkCount = 0
		if record.ExternalBlob == uuid.Nil && len(record.Blob) > 0 {
			record = removeInlineBlob(record)
			record.Timestamps.Update()
			return m.mutateSingleInTransaction(tx, ds.NewUpdate(rkey, record))
		}

		// Otherwise, clear the association
		blob, err = m.getBlobRef(ctx, tx, record.ExternalBlob)
		if err != nil {
			return err
		}

		record, err = m.markBlobRefForDeletion(tx, record, blob, uuid.Nil)
		if err != nil {
			return err
		}
		return m.mutateSingleInTransaction(tx, ds.NewUpdate(rkey, record))
	})
	if err != nil {
		return nil, nil, datastoreErrToGRPCStatus(err)
	}
	return record, blob, nil
}

func (m *MetaDB) deleteChildChunkRefs(ctx context.Context, tx *ds.Transaction, blob *blobref.BlobRef) error {
	query := m.newQuery(chunkKind).Ancestor(m.createBlobKey(blob.Key)).Transaction(tx).KeysOnly()
	iter := m.client.Run(ctx, query)
	for {
		key, err := iter.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := tx.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// DeleteBlobRef deletes the BlobRef object from the database.
// Returned errors:
//   - NotFound: the blobref object is not found
//   - FailedPrecondition: the blobref status is Ready and can't be deleted
func (m *MetaDB) DeleteBlobRef(ctx context.Context, key uuid.UUID) error {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.DeleteBlobRef")
	defer span.End()

	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := m.getBlobRef(ctx, tx, key)
		if err != nil {
			return err
		}
		if blob.Status == blobref.StatusReady {
			return status.Error(codes.FailedPrecondition, "blob is currently marked as ready. mark it for deletion first")
		}
		if blob.Chunked {
			if err := m.deleteChildChunkRefs(ctx, tx, blob); err != nil {
				return err
			}
		}
		return tx.Delete(m.createBlobKey(key))
	})
	return datastoreErrToGRPCStatus(err)
}

// DeleteChunkRef deletes the ChunkRef object from the database.
// Returned errors:
//   - NotFound: the chunkref object is not found.
func (m *MetaDB) DeleteChunkRef(ctx context.Context, blobKey, key uuid.UUID) error {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.DeleteChunkRef")
	defer span.End()

	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		var chunk chunkref.ChunkRef
		if err := tx.Get(m.createChunkRefKey(blobKey, key), &chunk); err != nil {
			return err
		}
		return tx.Delete(m.createChunkRefKey(blobKey, key))
	})
	return datastoreErrToGRPCStatus(err)
}

// ListBlobRefsByStatus returns a cursor that iterates over BlobRefs
// where Status = status.
func (m *MetaDB) ListBlobRefsByStatus(ctx context.Context, status blobref.Status) (*blobref.BlobRefCursor, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.ListBlobRefsByStatus")
	defer span.End()

	query := m.newQuery(blobKind).FilterField("Status", "=", int(status))
	iter := blobref.NewCursor(m.client.Run(ctx, query))
	return iter, nil
}

// GetChildChunkRefs returns a ChunkRef cursor that iterats over child ChunkRef
// entries of the BlobRef specified by blobkey.
func (m *MetaDB) GetChildChunkRefs(ctx context.Context, blobKey uuid.UUID) *chunkref.ChunkRefCursor {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.GetChildChunkRefs")
	defer span.End()

	query := m.newQuery(chunkKind).Ancestor(m.createBlobKey(blobKey))
	return chunkref.NewCursor(m.client.Run(ctx, query))
}

// addPropertyFilter augments a query with the QueryFilter operations.
func addPropertyFilter(q *ds.Query, f *pb.QueryFilter) (*ds.Query, error) {
	fieldName := propertiesField + "." + f.PropertyName
	var operator string
	switch f.Operator {
	case pb.FilterOperator_EQUAL:
		operator = "="
	case pb.FilterOperator_GREATER:
		operator = ">"
	case pb.FilterOperator_LESS:
		operator = "<"
	case pb.FilterOperator_GREATER_OR_EQUAL:
		operator = ">="
	case pb.FilterOperator_LESS_OR_EQUAL:
		operator = "<="
	default:
		return nil, status.Errorf(codes.Unimplemented, "unknown filter operator detected: %+v", f.Operator)
	}
	return q.FilterField(fieldName, operator, record.ExtractValue(f.Value)), nil
}

// QueryRecords returns a list of records that match the given filters.
func (m *MetaDB) QueryRecords(ctx context.Context, req *pb.QueryRecordsRequest) ([]*record.Record, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.QueryRecords")
	defer span.End()

	query := m.newQuery(recordKind)
	if req.GetStoreKey() != "" {
		dsKey := m.createStoreKey(req.GetStoreKey())
		query = query.Ancestor(dsKey)
	}
	if owner := req.GetOwnerId(); owner != "" {
		query = query.FilterField(ownerField, "=", owner)
	}
	for _, f := range req.GetFilters() {
		q, err := addPropertyFilter(query, f)
		if err != nil {
			return nil, err
		}
		query = q
	}
	for _, t := range req.GetTags() {
		query = query.FilterField(tagsField, "=", t)
	}
	for _, s := range req.GetSortOrders() {
		var property string
		switch s.Property {
		case pb.SortOrder_CREATED_AT:
			property = "Timestamps.CreatedAt"
		case pb.SortOrder_UPDATED_AT:
			property = "Timestamps.UpdatedAt"
		case pb.SortOrder_USER_PROPERTY:
			if s.UserPropertyName == "" {
				return nil, status.Error(codes.InvalidArgument, "got empty user sort property")
			}
			property = fmt.Sprintf("%s.%s", propertiesField, s.UserPropertyName)
		default:
			return nil, status.Errorf(codes.InvalidArgument, "got invalid SortOrder property value: %v", s.Property)
		}

		switch s.Direction {
		case pb.SortOrder_ASC:
			query = query.Order(strconv.Quote(property))
		case pb.SortOrder_DESC:
			query = query.Order("-" + strconv.Quote(property))
		default:
			return nil, status.Errorf(codes.InvalidArgument, "got invalid SortOrder direction value: %v", s.Direction)
		}
	}
	// Determine if we query keys only based on offset and request params

	if limit := req.GetLimit(); limit > 0 {
		query = query.Limit(int(limit))
	}
	queryKeysOnly := false
	useOffset := false
	if offset := req.GetOffset(); offset > 0 {
		query = query.Offset(int(offset))
		query = query.KeysOnly()
		queryKeysOnly = true
		useOffset = true
	}
	if req.GetKeysOnly() {
		query = query.KeysOnly()
		queryKeysOnly = true
	}
	iter := m.client.Run(ctx, query)

	var match []*record.Record
	var keys []*ds.Key
	for {
		var r record.Record
		key, err := iter.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "metadb QueryRecords: %v", err)
		}
		if queryKeysOnly {
			if err := r.LoadKey(key); err != nil {
				return nil, status.Errorf(codes.Internal, "metadb QueryRecords LoadKey: %v", err)
			}
		}

		if useOffset && !req.GetKeysOnly() {
			keys = append(keys, key)
		} else {
			match = append(match, &r)
		}
	}

	// If an offset was passed and the clients want full records, fetch records by keys
	var err error
	if useOffset && !req.GetKeysOnly() {
		match = make([]*record.Record, len(keys))
		if err = m.client.GetMulti(ctx, keys, match); err != nil {
			if _, ok := err.(ds.MultiError); !ok {
				// Datastore internal error
				return nil, datastoreErrToGRPCStatus(err)
			}
		}
	}

	return match, m.toGRPCStatus(err)
}

// GetRecords returns records by using the get multi request interface from datastore.
func (m *MetaDB) GetRecords(ctx context.Context, storeKeys, recordKeys []string) ([]*record.Record, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.GetRecords")
	defer span.End()

	// Build the key array with parameters
	keys, err := m.createDatastoreKeys(storeKeys, recordKeys)
	if err != nil {
		return nil, err
	}

	// Query the datastore for the records by keys
	records := make([]*record.Record, len(keys))
	if err = m.client.GetMulti(ctx, keys, records); err != nil {
		if _, ok := err.(ds.MultiError); !ok {
			// Datastore internal error
			return nil, datastoreErrToGRPCStatus(err)
		}
	}
	return records, m.toGRPCStatus(err)
}

func (m *MetaDB) toGRPCStatus(err error) error {
	if err != nil {
		if dsErr, ok := err.(ds.MultiError); ok {
			for i := range dsErr {
				dsErr[i] = datastoreErrToGRPCStatus(dsErr[i])
			}
			return dsErr
		}
	}
	return err
}

func (m *MetaDB) createDatastoreKeys(storeKeys, recordKeys []string) ([]*ds.Key, error) {
	if len(storeKeys) != len(recordKeys) {
		return nil, status.Error(codes.InvalidArgument, "metadb createDatastoreKeys: invalid store/record key array(s) length")
	}
	var keys []*ds.Key
	if len(storeKeys) == 0 {
		return keys, nil
	}

	for i := 0; i < len(storeKeys); i++ {
		key := m.createRecordKey(storeKeys[i], recordKeys[i])
		keys = append(keys, key)
	}

	return keys, nil
}

func (m *MetaDB) findChunkRefsByNumber(ctx context.Context, tx *ds.Transaction, blobKey uuid.UUID, number int32) ([]*chunkref.ChunkRef, error) {
	query := m.newQuery(chunkKind).Transaction(tx).Ancestor(m.createBlobKey(blobKey)).FilterField("Number", "=", number)
	iter := m.client.Run(ctx, query)

	chunks := []*chunkref.ChunkRef{}
	for {
		chunk := new(chunkref.ChunkRef)
		_, err := iter.Next(chunk)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// FindBlobChunkRefsByNumber returns the list of ChunkRef objects associated to a BlobRef sharing the same number.
func (m *MetaDB) FindBlobChunkRefsByNumber(ctx context.Context, blobKey uuid.UUID, number int32) ([]*chunkref.ChunkRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.FindBlobChunkRefsByNumber")
	defer span.End()

	var chunks []*chunkref.ChunkRef
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		foundChunks, err := m.findChunkRefsByNumber(ctx, tx, blobKey, number)
		chunks = foundChunks
		return err
	}, ds.ReadOnly)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}

	return chunks, nil
}

// FindChunkRefByNumber returns a ChunkRef object for the specified store, record, and number.
// The Chunk upload session must be committed.
func (m *MetaDB) FindChunkRefByNumber(ctx context.Context, storeKey, recordKey string, number int32) (*chunkref.ChunkRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.FindChunkRefByNumber")
	defer span.End()

	chunks := []*chunkref.ChunkRef{}
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := m.getCurrentBlobRef(ctx, tx, storeKey, recordKey)
		if err != nil {
			return err
		}
		chunks, err = m.findChunkRefsByNumber(ctx, tx, blob.Key, number)
		return err
	}, ds.ReadOnly)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}

	if len(chunks) == 0 {
		return nil, status.Errorf(codes.NotFound, "chunk number (%v) was not found for record (%v)", number, recordKey)
	} else if len(chunks) > 1 { // This should never happen, nonetheless, we verify it.
		return nil, status.Errorf(codes.FailedPrecondition, "found multiple chunks with number (%v) found for record (%v)", number, recordKey)
	}

	return chunks[0], nil
}

// ValidateChunkRefPreconditions check if the parent blobref is chunked before attempting to upload any data
func (m *MetaDB) ValidateChunkRefPreconditions(ctx context.Context, chunk *chunkref.ChunkRef) (*blobref.BlobRef, error) {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.ValidateChunkRefPreconditions")
	defer span.End()

	blob, err := m.getBlobRef(ctx, nil, chunk.BlobRef)
	if err != nil {
		return nil, err
	} else {
		if !blob.Chunked {
			return nil, status.Errorf(codes.FailedPrecondition, "BlobRef (%v) is not chunked", chunk.BlobRef)
		}
	}
	return blob, nil
}

// InsertChunkRef inserts a new ChunkRef object to the datastore. If the current session has another chunk
// with the same Number, it will be marked for deletion.
func (m *MetaDB) InsertChunkRef(ctx context.Context, blob *blobref.BlobRef, chunk *chunkref.ChunkRef) error {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.InsertChunkRef")
	defer span.End()

	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {

		mut := ds.NewInsert(m.createChunkRefKey(chunk.BlobRef, chunk.Key), chunk)
		if err := m.mutateSingleInTransaction(tx, mut); err != nil {
			return err
		}

		return nil
	})
	return err
}

// MarkUncommittedBlobForDeletion marks the BlobRef specified by key for deletion
// if the current status is StatusInitializing.
// Returns FailedPrecondition is Status is not StatusInitializing.
func (m *MetaDB) MarkUncommittedBlobForDeletion(ctx context.Context, key uuid.UUID) error {
	_, span := otel.Tracer(tracing.ServiceName).Start(ctx, "MetaDB.MarkUncommittedBlobForDeletion")
	defer span.End()

	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := m.getBlobRef(ctx, tx, key)
		if err != nil {
			return err
		}
		if blob.Status != blobref.StatusInitializing {
			return status.Errorf(codes.FailedPrecondition, "blob object is not in initialization state (state = %v)", blob.Status)
		}
		if err := blob.MarkForDeletion(); err != nil {
			return err
		}
		// Mark the blob as expired so the TTL can handle deletion.
		blob.MarkAsExpired()
		blob.Timestamps.Update()
		return m.mutateSingleInTransaction(tx, ds.NewUpdate(m.createBlobKey(key), blob))
	})
	return err
}
