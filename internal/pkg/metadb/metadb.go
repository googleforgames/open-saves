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
)

const (
	storeKind  = "store"
	recordKind = "record"
	blobKind   = "blob"
	chunkKind  = "chunk"
)

// MetaDB is a metadata database manager of Open Saves.
// The methods return gRPC error codes. Here are some common error codes
// returned. For additional details, please look at the method help.
// Common errors:
//	- NotFound: entity or object is not found
//	- Aborted: transaction is aborted
//	- InvalidArgument: key or value provided is not valid
//	- Internal: internal unrecoverable error
type MetaDB struct {
	// Datastore namespace for multi-tenancy
	Namespace string

	client *ds.Client
}

// RecordUpdater is a callback function for record updates.
// Returning a non-nil error aborts the transaction.
type RecordUpdater func(record *record.Record) (*record.Record, error)

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
	ck := ds.NameKey(chunkKind, key.String(), bk)
	ck.Namespace = m.Namespace
	return ck
}

func (m *MetaDB) getBlobRef(ctx context.Context, tx *ds.Transaction, key uuid.UUID) (*blobref.BlobRef, error) {
	if key == uuid.Nil {
		return nil, status.Error(codes.FailedPrecondition, "there is no an external blob associated")
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
	query := ds.NewQuery(recordKind).Namespace(m.Namespace).
		KeysOnly().Filter("__key__ = ", key).Limit(1)
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

func (m *MetaDB) blobRefExists(ctx context.Context, tx *ds.Transaction, key uuid.UUID) (bool, error) {
	bk := m.createBlobKey(key)
	query := m.newQuery(blobKind).KeysOnly().Filter("__key__ = ", bk).Limit(1)
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
	record.Timestamps = timestamps.New()
	rkey := m.createRecordKey(storeKey, record.Key)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		dskey := m.createStoreKey(storeKey)
		query := ds.NewQuery(storeKind).Transaction(tx).Namespace(m.Namespace).
			KeysOnly().Filter("__key__ = ", dskey).Limit(1)
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
		// Deassociate the old blob if an external blob is associated, and a new inline blob is being added.
		if oldExternalBlob != uuid.Nil && len(toUpdate.Blob) > 0 {
			oldBlob, err := m.getBlobRef(ctx, tx, toUpdate.ExternalBlob)
			if err != nil {
				return err
			}
			toUpdate, err = m.markBlobRefForDeletion(tx, toUpdate, oldBlob, uuid.Nil)
			if err != nil {
				return err
			}
		}

		toUpdate.Timestamps.Update()
		return m.mutateSingleInTransaction(tx, ds.NewUpdate(rkey, toUpdate))
	})
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return toUpdate, nil
}

// GetRecord fetches and returns a record with key in store storeKey.
// Returns error if not found.
func (m *MetaDB) GetRecord(ctx context.Context, storeKey, key string) (*record.Record, error) {
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
//	- NotFound: the object is not found.
func (m *MetaDB) GetBlobRef(ctx context.Context, key uuid.UUID) (*blobref.BlobRef, error) {
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
// 	- NotFound: the record is not found.
// 	- FailedPrecondition: the record doesn't have a blob.
func (m *MetaDB) GetCurrentBlobRef(ctx context.Context, storeKey, recordKey string) (*blobref.BlobRef, error) {
	var blob *blobref.BlobRef
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		var err error
		blob, err = m.getCurrentBlobRef(ctx, tx, storeKey, recordKey)
		return err
	}, ds.ReadOnly)
	return blob, datastoreErrToGRPCStatus(err)
}

func (m *MetaDB) getReadyChunks(ctx context.Context, tx *ds.Transaction, blob *blobref.BlobRef) ([]*chunkref.ChunkRef, error) {
	query := m.newQuery(chunkKind).Transaction(tx).Ancestor(m.createBlobKey(blob.Key)).Filter("Status =", blobref.StatusReady)
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

func (m *MetaDB) chunkObjectsSizeSum(ctx context.Context, tx *ds.Transaction, blob *blobref.BlobRef) (int64, error) {
	chunks, err := m.getReadyChunks(ctx, tx, blob)
	if err != nil {
		return 0, err
	}
	size := int64(0)
	for _, chunk := range chunks {
		size += int64(chunk.Size)
	}
	return size, nil
}

// PromoteBlobRefToCurrent promotes the provided BlobRef object as a current
// external blob reference.
// Returned errors:
//	- NotFound: the specified record or the blobref was not found
//  - Internal: BlobRef status transition error
func (m *MetaDB) PromoteBlobRefToCurrent(ctx context.Context, blob *blobref.BlobRef) (*record.Record, *blobref.BlobRef, error) {
	record := new(record.Record)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		rkey := m.createRecordKey(blob.StoreKey, blob.RecordKey)
		if err := tx.Get(rkey, record); err != nil {
			return err
		}
		if record.ExternalBlob == uuid.Nil {
			// Simply add the new blob if previously didn't have a blob
			record.Blob = nil
			record.Timestamps.Update()
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
			size, err := m.chunkObjectsSizeSum(ctx, tx, blob)
			if err != nil {
				return err
			}
			blob.Size = size
		}
		if blob.Status != blobref.StatusReady {
			if blob.Ready() != nil {
				return status.Error(codes.Internal, "blob is not ready to become current")
			}
		}
		blob.Timestamps.Update()
		if _, err := tx.Mutate(ds.NewUpdate(m.createBlobKey(blob.Key), blob)); err != nil {
			return err
		}

		record.BlobSize = blob.Size
		record.ExternalBlob = blob.Key
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
//	- NotFound: the specified record or the blobref was not found
//	- FailedPrecondition: the record doesn't have an external blob
//  - Internal: BlobRef status transition error
func (m *MetaDB) RemoveBlobFromRecord(ctx context.Context, storeKey string, recordKey string) (*record.Record, *blobref.BlobRef, error) {
	rkey := m.createRecordKey(storeKey, recordKey)
	blob := new(blobref.BlobRef)
	record := new(record.Record)
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		err := tx.Get(rkey, record)
		if err != nil {
			return err
		}

		record.BlobSize = 0
		if record.ExternalBlob == uuid.Nil && len(record.Blob) > 0 {
			// Delete the inline blob
			record.Blob = nil
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
//	- NotFound: the blobref object is not found
//	- FailedPrecondition: the blobref status is Ready and can't be deleted
func (m *MetaDB) DeleteBlobRef(ctx context.Context, key uuid.UUID) error {
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

// ListBlobRefsByStatus returns a cursor that iterates over BlobRefs
// where Status = status and UpdatedAt < olderThan.
func (m *MetaDB) ListBlobRefsByStatus(ctx context.Context, status blobref.Status, olderThan time.Time) (*blobref.BlobRefCursor, error) {
	query := m.newQuery(blobKind).Filter("Status = ", int(status)).
		Filter("Timestamps.UpdatedAt <", olderThan)
	iter := blobref.NewCursor(m.client.Run(ctx, query))
	return iter, nil
}

func (m *MetaDB) getChunkRef(ctx context.Context, tx *ds.Transaction, blobKey, key uuid.UUID) (*chunkref.ChunkRef, error) {
	k := m.createChunkRefKey(blobKey, key)
	chunk := new(chunkref.ChunkRef)
	var err error
	if tx != nil {
		err = tx.Get(k, chunk)
	} else {
		err = m.client.Get(ctx, k, chunk)
	}
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return chunk, nil
}

func (m *MetaDB) GetChunkRef(ctx context.Context, blobKey, key uuid.UUID) (*chunkref.ChunkRef, error) {
	return m.getChunkRef(ctx, nil, blobKey, key)
}

func (m *MetaDB) findChunkRefsByNumber(ctx context.Context, tx *ds.Transaction, storeKey, recordKey string, number int32) ([]*chunkref.ChunkRef, error) {
	blob, err := m.getCurrentBlobRef(ctx, tx, storeKey, recordKey)
	if err != nil {
		return nil, err
	}
	query := m.newQuery(chunkKind).Transaction(tx).Ancestor(m.createBlobKey(blob.Key)).Filter("Number = ", number)
	iter := m.client.Run(ctx, query)

	chunks := []*chunkref.ChunkRef{}
	for {
		chunk := new(chunkref.ChunkRef)
		_, err = iter.Next(chunk)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func (m *MetaDB) FindChunkRefByNumber(ctx context.Context, storeKey, recordKey string, number int32) (*chunkref.ChunkRef, error) {
	chunks := []*chunkref.ChunkRef{}
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		var err error
		chunks, err = m.findChunkRefsByNumber(ctx, tx, storeKey, recordKey, number)
		return err

	}, ds.ReadOnly)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	for _, c := range chunks {
		if c.Status == blobref.StatusReady {
			return c, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "chunk number (%v) was not found for record (%v)", number, recordKey)
}

func (m *MetaDB) MarkChunkRefReady(ctx context.Context, chunk *chunkref.ChunkRef) error {
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := m.getBlobRef(ctx, tx, chunk.BlobRef)
		if err != nil {
			return err
		}

		// Mark the chunk ready (Datastore's transaction isolation guarantees this is not visible until
		// we commit the transaction).
		if err := chunk.Ready(); err != nil {
			return err
		}
		chunk.Timestamps.Update()
		if err := m.mutateSingleInTransaction(tx, ds.NewUpdate(m.createChunkRefKey(blob.Key, chunk.Key), chunk)); err != nil {
			return err
		}

		// Mark any other (should be at most one though) Ready chunks for deletion.
		otherChunks, err := m.findChunkRefsByNumber(ctx, tx, blob.StoreKey, blob.RecordKey, chunk.Number)
		if err != nil {
			return err
		}
		for _, o := range otherChunks {
			if o.Status == blobref.StatusReady {
				if err := o.MarkForDeletion(); err != nil {
					return err
				}
				o.Timestamps.Update()
				if err := m.mutateSingleInTransaction(tx, ds.NewUpdate(m.createChunkRefKey(blob.Key, o.Key), o)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

func (m *MetaDB) InsertChunkRef(ctx context.Context, chunk *chunkref.ChunkRef) error {
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		if found, err := m.blobRefExists(ctx, tx, chunk.BlobRef); err != nil {
			return err
		} else {
			if !found {
				return status.Error(codes.FailedPrecondition, "BlobRef does not exist")
			}
		}
		mut := ds.NewInsert(m.createChunkRefKey(chunk.BlobRef, chunk.Key), chunk)
		return m.mutateSingleInTransaction(tx, mut)
	})
	return err
}

func (m *MetaDB) UpdateChunkRef(ctx context.Context, chunk *chunkref.ChunkRef) error {
	mut := ds.NewUpdate(m.createChunkRefKey(chunk.BlobRef, chunk.Key), chunk)
	return m.mutateSingle(ctx, mut)
}

func (m *MetaDB) DeleteChunkRef(ctx context.Context, blobKey, key uuid.UUID) error {
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := m.getBlobRef(ctx, tx, blobKey)
		if err != nil {
			// TODO(yuryu): Provide a way to fix inconsistent data
			return err
		}
		if blob.Status == blobref.StatusReady {
			return status.Error(codes.FailedPrecondition, "blob is currently marked as ready. mark it for deletion first")
		}
		k := m.createChunkRefKey(blobKey, key)
		return m.mutateSingleInTransaction(tx, ds.NewDelete(k))
	})
	return err
}

func (m *MetaDB) MarkUncommittedChunkedBlobForDeletion(ctx context.Context, key uuid.UUID) error {
	_, err := m.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := m.getBlobRef(ctx, tx, key)
		if err != nil {
			return err
		}
		if !blob.Chunked {
			return status.Error(codes.InvalidArgument, "provided session ID is not valid")
		}
		if blob.Status != blobref.StatusInitializing {
			return status.Errorf(codes.FailedPrecondition, "blob object is not in initialization state (state = %v)", blob.Status)
		}
		if err := blob.MarkForDeletion(); err != nil {
			return err
		}
		blob.Timestamps.Update()
		return m.mutateSingleInTransaction(tx, ds.NewUpdate(m.createBlobKey(key), blob))
	})
	return err
}
