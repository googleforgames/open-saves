// Copyright 2021 Google LLC
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

package collector

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/gcerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	// TODO(yuryu): Make these configurable
	testProject       = "triton-for-games-dev"
	testBucket        = "gs://triton-integration"
	testBufferSize    = 1024 * 1024
	testCacheAddr     = "localhost:6379"
	storeKind         = "store"
	recordKind        = "record"
	blobKind          = "blob"
	testTimeThreshold = -1 * time.Hour
)

func newTestCollector(ctx context.Context, t *testing.T) *Collector {
	t.Helper()
	cfg := &Config{
		Cloud:   "gcp",
		Bucket:  testBucket,
		Project: testProject,
		Cache:   testCacheAddr,
		Before:  time.Now().Add(testTimeThreshold),
	}
	c, err := newCollector(ctx, cfg)
	if err != nil {
		t.Fatalf("newCollector returned error: %v", err)
	}
	return c
}

func setupTestStore(ctx context.Context, t *testing.T, collector *Collector) *store.Store {
	t.Helper()
	store := &store.Store{
		Key:  uuid.New().String(),
		Name: t.Name(),
	}
	store, err := collector.metaDB.CreateStore(ctx, store)
	if err != nil {
		t.Fatalf("CreateStore returned error: %v", err)
	}
	t.Cleanup(func() {
		collector.metaDB.DeleteStore(ctx, store.Key)
	})
	return store
}

func newDatastoreClient(ctx context.Context, t *testing.T) *datastore.Client {
	t.Helper()
	ds, err := datastore.NewClient(ctx, testProject)
	if err != nil {
		t.Fatalf("Failed to create a new Datastore client: %v", err)
	}
	return ds
}

func setupTestRecord(ctx context.Context, t *testing.T, collector *Collector, storeKey string) *record.Record {
	t.Helper()
	record := &record.Record{
		Key:        uuid.New().String(),
		Tags:       []string{t.Name()},
		Properties: make(record.PropertyMap),
	}
	record, err := collector.metaDB.InsertRecord(ctx, storeKey, record)
	if err != nil {
		t.Fatalf("InsertRecord returned error: %v", err)
	}
	t.Cleanup(func() {
		collector.metaDB.DeleteRecord(ctx, storeKey, record.Key)
	})
	return record
}

func setupTestBlobRef(ctx context.Context, t *testing.T, ds *datastore.Client, blobRef *blobref.BlobRef) {
	t.Helper()
	// Directly update Datastore to specify Timestamps.
	key := datastore.NameKey(blobKind, blobRef.Key.String(), nil)
	_, err := ds.Put(ctx, key, blobRef)
	if err != nil {
		t.Fatalf("InsertBlobRef returned error: %v", err)
	}
	t.Cleanup(func() {
		ds.Delete(ctx, key)
	})
}

func setupExternalBlob(ctx context.Context, t *testing.T, collector *Collector, path string) {
	t.Helper()
	err := collector.blob.Put(ctx, path, []byte{})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	t.Cleanup(func() {
		collector.blob.Delete(ctx, path)
	})
}

func TestCollector_DeletesBlobs(t *testing.T) {
	ctx := context.Background()
	collector := newTestCollector(ctx, t)
	store := setupTestStore(ctx, t, collector)
	record := setupTestRecord(ctx, t, collector, store.Key)
	const numBlobRefs = 5
	blobRefs := make([]*blobref.BlobRef, 0, numBlobRefs)

	// 0 and 2 are old, to be deleted
	// 1 and 3 have the applicable statuses but new
	// 4 is still initializing
	for i := 0; i < numBlobRefs; i++ {
		blobRef := blobref.NewBlobRef(0, store.Key, record.Key)
		blobRef.Timestamps.CreatedAt = collector.cfg.Before
		blobRef.Timestamps.UpdatedAt = collector.cfg.Before
		blobRefs = append(blobRefs, blobRef)
	}
	blobRefs[0].MarkForDeletion()
	blobRefs[0].Timestamps.UpdatedAt = collector.cfg.Before.Add(-1 * time.Microsecond)
	blobRefs[1].MarkForDeletion()
	blobRefs[2].Fail()
	blobRefs[2].Timestamps.UpdatedAt = collector.cfg.Before.Add(-1 * time.Microsecond)
	blobRefs[3].Fail()

	ds := newDatastoreClient(ctx, t)
	for _, b := range blobRefs {
		setupTestBlobRef(ctx, t, ds, b)
		setupExternalBlob(ctx, t, collector, b.ObjectPath())
	}
	collector.run(ctx)

	exists := []bool{false, true, false, true, true}
	for i, e := range exists {
		actual, err := collector.metaDB.GetBlobRef(ctx, blobRefs[i].Key)
		if e {
			assert.NotNil(t, actual)
			assert.NoError(t, err)
		} else {
			assert.Nil(t, actual)
			assert.Equal(t, codes.NotFound, grpc.Code(err))
		}
		_, err = collector.blob.Get(ctx, blobRefs[i].ObjectPath())
		if e {
			assert.NoError(t, err)
		} else {
			assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
		}
	}
}
