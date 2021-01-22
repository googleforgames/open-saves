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
	"time"

	ds "cloud.google.com/go/datastore"
	"github.com/google/uuid"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	storeKind          = "store"
	recordKind         = "record"
	blobKind           = "blob"
	timestampPrecision = 1 * time.Microsecond
)

// Driver is an implementation of the metadb.Driver interface for Google Cloud Datastore.
// Call NewDriver to create a new driver instance.
type Driver struct {
	client *ds.Client
	// Datastore namespace for multi-tenancy
	Namespace string
}

func (d *Driver) mutateSingleInTransaction(tx *ds.Transaction, mut *ds.Mutation) error {
	_, err := tx.Mutate(mut)
	if err != nil {
		if merr, ok := err.(ds.MultiError); ok {
			err = merr[0]
		}
		return datastoreErrToGRPCStatus(err)
	}
	return nil
}

func (d *Driver) mutateSingle(ctx context.Context, mut *ds.Mutation) error {
	_, err := d.client.Mutate(ctx, mut)
	if err != nil {
		if merr, ok := err.(ds.MultiError); ok {
			err = merr[0]
		}
		return datastoreErrToGRPCStatus(err)
	}
	return nil
}

func (d *Driver) createStoreKey(key string) *ds.Key {
	k := ds.NameKey(storeKind, key, nil)
	k.Namespace = d.Namespace
	return k
}

func (d *Driver) createRecordKey(storeKey, key string) *ds.Key {
	sk := d.createStoreKey(storeKey)
	sk.Namespace = d.Namespace
	rk := ds.NameKey(recordKind, key, sk)
	rk.Namespace = d.Namespace
	return rk
}

func (d *Driver) createBlobKey(key uuid.UUID) *ds.Key {
	k := ds.NameKey(blobKind, key.String(), nil)
	k.Namespace = d.Namespace
	return k
}

func (d *Driver) newQuery(kind string) *ds.Query {
	if d.Namespace != "" {
		return ds.NewQuery(kind).Namespace(d.Namespace)
	}
	return ds.NewQuery(kind)
}

// NewDriver creates a new instance of Driver that can be used by metadb.MetaDB.
// projectID: Google Cloud Platform project ID to use
func NewDriver(ctx context.Context, projectID string, opts ...option.ClientOption) (*Driver, error) {
	client, err := ds.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return &Driver{client: client}, nil
}

// Connect initiates the database connection.
func (d *Driver) Connect(ctx context.Context) error {
	// noop for Cloud Datastore
	return nil
}

// Disconnect terminates the database connection.
// Make sure to call this method to release resources (e.g. using defer).
// The MetaDB instance will not be available after Disconnect().
func (d *Driver) Disconnect(ctx context.Context) error {
	return datastoreErrToGRPCStatus(d.client.Close())
}

// CreateStore creates a new store.
func (d *Driver) CreateStore(ctx context.Context, store *m.Store) (*m.Store, error) {
	key := d.createStoreKey(store.Key)
	mut := ds.NewInsert(key, store)
	if err := d.mutateSingle(ctx, mut); err != nil {
		return nil, err
	}
	return store, nil
}

// GetStore fetches a store based on the key provided.
// Returns error if the key is not found.
func (d *Driver) GetStore(ctx context.Context, key string) (*m.Store, error) {
	dskey := d.createStoreKey(key)
	store := new(m.Store)
	err := d.client.Get(ctx, dskey, store)
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return store, nil
}

// FindStoreByName finds and fetches a store based on the name (complete match).
func (d *Driver) FindStoreByName(ctx context.Context, name string) (*m.Store, error) {
	query := d.newQuery(storeKind).Filter("Name =", name)
	iter := d.client.Run(ctx, query)
	store := new(m.Store)
	_, err := iter.Next(store)
	if err != nil {
		return nil, status.Errorf(codes.NotFound,
			"Store (name=%s) was not found.", name)
	}
	return store, nil
}

// DeleteStore deletes the store with specified key.
// Returns error if the store has any child records.
func (d *Driver) DeleteStore(ctx context.Context, key string) error {
	dskey := d.createStoreKey(key)

	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		query := ds.NewQuery(recordKind).Transaction(tx).KeysOnly().
			Ancestor(dskey).Limit(1).Namespace(d.Namespace)
		iter := d.client.Run(ctx, query)
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
func (d *Driver) InsertRecord(ctx context.Context, storeKey string, record *m.Record) (*m.Record, error) {
	rkey := d.createRecordKey(storeKey, record.Key)
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		dskey := d.createStoreKey(storeKey)
		query := ds.NewQuery(storeKind).Transaction(tx).Namespace(d.Namespace).
			KeysOnly().Filter("__key__ = ", dskey).Limit(1)
		iter := d.client.Run(ctx, query)
		_, err := iter.Next(nil)
		if err == iterator.Done {
			return status.Errorf(codes.FailedPrecondition,
				"InsertRecord was called with a non-existent store (%s)", storeKey)
		}
		mut := ds.NewInsert(rkey, record)
		return d.mutateSingleInTransaction(tx, mut)
	})
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return record, nil
}

// UpdateRecord updates the record in the store specified with storeKey.
// Returns error if the store doesn't have a record with the key provided.
func (d *Driver) UpdateRecord(ctx context.Context, storeKey string, record *m.Record) (*m.Record, error) {
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		rkey := d.createRecordKey(storeKey, record.Key)

		// TODO(yuryu): Consider supporting transactions in MetaDB and move
		// this operation out of the Datastore specific code.
		oldRecord := new(m.Record)
		if err := tx.Get(rkey, oldRecord); err != nil {
			return err
		}
		// Deassociate the old blob if an external blob is associated is a new inline blob is being added
		if oldRecord.ExternalBlob != uuid.Nil {
			oldBlob, err := d.getBlobRef(ctx, tx, record.ExternalBlob)
			if err != nil {
				return err
			}
			if err := d.markBlobRefForDeletion(ctx, tx, storeKey, record, oldBlob, uuid.Nil); err != nil {
				return err
			}
		}
		record.Timestamps.CreatedAt = oldRecord.Timestamps.CreatedAt
		mut := ds.NewUpdate(rkey, record)
		return d.mutateSingleInTransaction(tx, mut)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

// GetRecord fetches and returns a record with key in store storeKey.
// Returns error if not found.
func (d *Driver) GetRecord(ctx context.Context, storeKey, key string) (*m.Record, error) {
	rkey := d.createRecordKey(storeKey, key)
	record := new(m.Record)
	if err := d.client.Get(ctx, rkey, record); err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return record, nil
}

// DeleteRecord deletes a record with key in store storeKey.
// It doesn't return error even if the key is not found in the database.
func (d *Driver) DeleteRecord(ctx context.Context, storeKey, key string) error {
	rkey := d.createRecordKey(storeKey, key)
	// TODO: mark all blobs for deletion?
	return datastoreErrToGRPCStatus(d.client.Delete(ctx, rkey))
}

// TimestampPrecision returns the precision of timestamps stored in
// Cloud Datastore. Currently it's 1 microsecond.
// https://cloud.google.com/datastore/docs/concepts/entities#date_and_time
func (d *Driver) TimestampPrecision() time.Duration {
	return timestampPrecision
}

func (d *Driver) recordExists(ctx context.Context, tx *ds.Transaction, key *ds.Key) (bool, error) {
	query := ds.NewQuery(recordKind).Namespace(d.Namespace).
		KeysOnly().Filter("__key__ = ", key).Limit(1)
	if tx != nil {
		query = query.Transaction(tx)
	}
	iter := d.client.Run(ctx, query)
	if _, err := iter.Next(nil); err == iterator.Done {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// InsertBlobRef implements Driver.InsertBlobRef.
func (d *Driver) InsertBlobRef(ctx context.Context, blob *m.BlobRef) (*m.BlobRef, error) {
	rkey := d.createRecordKey(blob.StoreKey, blob.RecordKey)
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		if exists, err := d.recordExists(ctx, tx, rkey); err != nil {
			return err
		} else if !exists {
			return status.Error(codes.FailedPrecondition, "InsertBlob was called for a non-exitent record")
		}
		_, err := tx.Mutate(ds.NewInsert(d.createBlobKey(blob.Key), blob))
		return err
	})
	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return blob, nil
}

// UpdateBlobRef implements Driver.UpdateBlobRef.
func (d *Driver) UpdateBlobRef(ctx context.Context, blob *m.BlobRef) (*m.BlobRef, error) {
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		oldBlob, err := d.getBlobRef(ctx, tx, blob.Key)
		if err != nil {
			return err
		}
		blob.Timestamps.CreatedAt = oldBlob.Timestamps.CreatedAt
		mut := ds.NewUpdate(d.createBlobKey(blob.Key), blob)
		_, err = tx.Mutate(mut)
		return err
	})

	if err != nil {
		return nil, datastoreErrToGRPCStatus(err)
	}
	return blob, nil
}

func (d *Driver) getBlobRef(ctx context.Context, tx *ds.Transaction, key uuid.UUID) (*m.BlobRef, error) {
	if key == uuid.Nil {
		return nil, status.Error(codes.FailedPrecondition, "there is no an external blob associated")
	}
	blob := new(m.BlobRef)
	if tx == nil {
		if err := d.client.Get(ctx, d.createBlobKey(key), blob); err != nil {
			return nil, datastoreErrToGRPCStatus(err)
		}
	} else {
		if err := tx.Get(d.createBlobKey(key), blob); err != nil {
			return nil, datastoreErrToGRPCStatus(err)
		}
	}
	return blob, nil
}

// GetBlobRef implements Driver.GetCurrentBlobRef.
func (d *Driver) GetBlobRef(ctx context.Context, key uuid.UUID) (*m.BlobRef, error) {
	return d.getBlobRef(ctx, nil, key)
}

// GetCurrentBlobRef implements Driver.GetCurrentBlobRef.
func (d *Driver) GetCurrentBlobRef(ctx context.Context, storeKey, recordKey string) (*m.BlobRef, error) {
	var blob *m.BlobRef
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		record := new(m.Record)
		err := tx.Get(d.createRecordKey(storeKey, recordKey), record)
		if err != nil {
			return err
		}
		blob, err = d.getBlobRef(ctx, tx, record.ExternalBlob)
		return err
	})
	return blob, datastoreErrToGRPCStatus(err)
}

// PromoteBlobRefToCurrent implements Driver.PromoteBlobRefToCurrent.
func (d *Driver) PromoteBlobRefToCurrent(ctx context.Context, blob *m.BlobRef) (*m.Record, *m.BlobRef, error) {
	record := new(m.Record)
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		rkey := d.createRecordKey(blob.StoreKey, blob.RecordKey)
		if err := tx.Get(rkey, record); err != nil {
			return err
		}
		if record.ExternalBlob == uuid.Nil {
			// Simply add the new blob if previously didn't have a blob
			// TODO(yuryu): This should be moved out of the driver.
			// The MetaDB Driver interface currently doesn't support transactions
			// but we need to put this part inside the transaction.
			record.Blob = nil
			record.BlobSize = blob.Size
			record.ExternalBlob = blob.Key
			record.Timestamps.UpdateTimestamps(timestampPrecision)
			if _, err := tx.Mutate(ds.NewUpdate(rkey, record)); err != nil {
				return err
			}
		} else {
			// Mark previous blob for deletion
			oldBlob, err := d.getBlobRef(ctx, tx, record.ExternalBlob)
			if err != nil {
				return err
			}
			if err := d.markBlobRefForDeletion(ctx, tx, blob.StoreKey, record, oldBlob, blob.Key); err != nil {
				return err
			}
		}

		if blob.Status != m.BlobRefStatusReady {
			if blob.Ready() != nil {
				return status.Error(codes.Internal, "blob is not ready to become current")
			}
			if _, err := tx.Mutate(ds.NewUpdate(d.createBlobKey(blob.Key), blob)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, datastoreErrToGRPCStatus(err)
	}
	return record, blob, nil
}

// MarkBlobRefForDeletion implements Driver.MarkBlobRefForDeletion.
func (d *Driver) MarkBlobRefForDeletion(ctx context.Context, storeKey string, recordKey string) (*m.Record, *m.BlobRef, error) {
	rkey := d.createRecordKey(storeKey, recordKey)
	blob := new(m.BlobRef)
	record := new(m.Record)
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		err := tx.Get(rkey, record)
		if err != nil {
			return err
		}

		// Clear the association
		blob, err = d.getBlobRef(ctx, tx, record.ExternalBlob)
		if err != nil {
			return err
		}
		return d.markBlobRefForDeletion(ctx, tx, storeKey, record, blob, uuid.Nil)
	})
	if err != nil {
		return nil, nil, datastoreErrToGRPCStatus(err)
	}
	return record, blob, nil
}

func (d *Driver) markBlobRefForDeletion(_ context.Context, tx *ds.Transaction,
	storeKey string, record *m.Record, blob *m.BlobRef, newBlobKey uuid.UUID) error {
	if record.ExternalBlob == uuid.Nil {
		return status.Error(codes.FailedPrecondition, "the record doesn't have an external blob associated")
	}
	rkey := d.createRecordKey(storeKey, record.Key)
	record.ExternalBlob = newBlobKey
	record.Timestamps.UpdateTimestamps(timestampPrecision)
	if _, err := tx.Mutate(ds.NewUpdate(rkey, record)); err != nil {
		return err
	}
	if blob.MarkForDeletion() != nil && blob.Fail() != nil {
		return status.Errorf(codes.Internal, "failed to transition the blob state for deletion: current = %v", blob.Status)
	}
	_, err := tx.Mutate(ds.NewUpdate(d.createBlobKey(blob.Key), blob))
	return err
}

// DeleteBlobRef implements Driver.DeleteBlobRef.
func (d *Driver) DeleteBlobRef(ctx context.Context, key uuid.UUID) error {
	_, err := d.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		blob, err := d.getBlobRef(ctx, tx, key)
		if err != nil {
			return err
		}
		if blob.Status == m.BlobRefStatusReady {
			return status.Error(codes.FailedPrecondition, "blob is currently marked as ready. mark it for deletion first")
		}
		return tx.Delete(d.createBlobKey(key))
	})
	return datastoreErrToGRPCStatus(err)
}
