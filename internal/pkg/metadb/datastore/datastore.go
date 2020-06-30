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

	ds "cloud.google.com/go/datastore"
	m "github.com/googleforgames/triton/internal/pkg/metadb"
	"google.golang.org/api/option"
)

const storeKind = "store"
const recordKind = "record"

// Driver is an implementation of the metadb.Driver interface for Google Cloud Datastore.
// Call NewDriver to create a new driver instance.
type Driver struct {
	client *ds.Client
	// Datastore namespace for multi-tenancy
	Namespace string
}

func (d *Driver) mutateSingle(ctx context.Context, mut *ds.Mutation) error {
	_, err := d.client.Mutate(ctx, mut)
	if err != nil {
		if merr, ok := err.(ds.MultiError); ok {
			err = merr[0]
		}
		return err
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
		return nil, err
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
	return d.client.Close()
}

// CreateStore creates a new store.
func (d *Driver) CreateStore(ctx context.Context, store *m.Store) error {
	key := d.createStoreKey(store.Key)
	mut := ds.NewInsert(key, store)
	return d.mutateSingle(ctx, mut)
}

// GetStore fetches a store based on the key provided.
// Returns error if the key is not found.
func (d *Driver) GetStore(ctx context.Context, key string) (*m.Store, error) {
	dskey := d.createStoreKey(key)
	store := new(m.Store)
	err := d.client.Get(ctx, dskey, store)
	if err != nil {
		return nil, err
	}
	store.Key = key
	return store, nil
}

// FindStoreByName finds and fetch a store based on the name (complete match).
func (d *Driver) FindStoreByName(ctx context.Context, name string) (*m.Store, error) {
	query := d.newQuery(storeKind).Filter("Name =", name)
	iter := d.client.Run(ctx, query)
	store := new(m.Store)
	key, err := iter.Next(store)
	if err != nil {
		return nil, err
	}
	store.Key = key.Name
	return store, nil
}

// DeleteStore deletes the store with specified key.
// Returns error if the store has any child records.
func (d *Driver) DeleteStore(ctx context.Context, key string) error {
	dskey := d.createStoreKey(key)
	return d.client.Delete(ctx, dskey)
}

// InsertRecord creates a new Record in the store specified with storeKey.
// Returns error if there is already a record with the same key.
func (d *Driver) InsertRecord(ctx context.Context, storeKey string, record *m.Record) error {
	rkey := d.createRecordKey(storeKey, record.Key)
	mut := ds.NewInsert(rkey, record)
	return d.mutateSingle(ctx, mut)
}

// UpdateRecord updates the record in the store specified with storeKey.
// Returns error if the store doesn't have a record with the key provided.
func (d *Driver) UpdateRecord(ctx context.Context, storeKey string, record *m.Record) error {
	rkey := d.createRecordKey(storeKey, record.Key)
	mut := ds.NewUpdate(rkey, record)
	return d.mutateSingle(ctx, mut)
}

// GetRecord fetches and returns a record with key in store storeKey.
// Returns error if not found.
func (d *Driver) GetRecord(ctx context.Context, storeKey, key string) (*m.Record, error) {
	rkey := d.createRecordKey(storeKey, key)
	record := new(m.Record)
	if err := d.client.Get(ctx, rkey, record); err != nil {
		return nil, err
	}
	record.Key = key
	return record, nil
}

// DeleteRecord deletes a record with key in store storeKey.
// It doesn't return error even if the key is not found in the database.
func (d *Driver) DeleteRecord(ctx context.Context, storeKey, key string) error {
	rkey := d.createRecordKey(storeKey, key)
	return d.client.Delete(ctx, rkey)
}
