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
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/gcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	chunkKind         = "chunk"
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
		Key:  uuid.NewString(),
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
		Key:        uuid.NewString(),
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

func chunkRefKey(chunk *chunkref.ChunkRef) *datastore.Key {
	return datastore.NameKey(chunkKind, chunk.Key.String(),
		datastore.NameKey(blobKind, chunk.BlobRef.String(), nil))
}

func setupTestChunkRef(ctx context.Context, t *testing.T, collector *Collector, ds *datastore.Client, chunk *chunkref.ChunkRef) {
	t.Helper()

	if err := collector.metaDB.InsertChunkRef(ctx, chunk); err != nil {
		t.Fatalf("InsertChunkRef failed: %v", err)
	}
	t.Cleanup(func() {
		ds.Delete(ctx, chunkRefKey(chunk))
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
			assert.Equal(t, codes.NotFound, status.Code(err))
		}
		_, err = collector.blob.Get(ctx, blobRefs[i].ObjectPath())
		if e {
			assert.NoError(t, err)
		} else {
			assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
		}
	}
}

func TestCollector_DeletesUnlinkedBlobRefs(t *testing.T) {
	ctx := context.Background()
	collector := newTestCollector(ctx, t)
	store := setupTestStore(ctx, t, collector)
	record := setupTestRecord(ctx, t, collector, store.Key)
	const numBlobRefs = 3
	blobRefs := make([]*blobref.BlobRef, 0, numBlobRefs)
	ds := newDatastoreClient(ctx, t)
	for i := 0; i < numBlobRefs; i++ {
		blobRef := blobref.NewBlobRef(0, store.Key, record.Key)
		blobRef.Fail() // Fail() updates Timestamps so needs to come here.
		blobRef.Timestamps.CreatedAt = collector.cfg.Before.Add(-1 * time.Second)
		blobRef.Timestamps.UpdatedAt = collector.cfg.Before.Add(-1 * time.Second)
		setupTestBlobRef(ctx, t, ds, blobRef)
		blobRefs = append(blobRefs, blobRef)
	}

	// Create an external blob for just one item and test if it's deleted too
	setupExternalBlob(ctx, t, collector, blobRefs[1].ObjectPath())

	collector.run(ctx)

	for _, b := range blobRefs {
		ref, err := collector.metaDB.GetBlobRef(ctx, b.Key)
		assert.Nil(t, ref)
		assert.Equal(t, codes.NotFound, status.Code(err))

		blob, err := collector.blob.Get(ctx, b.ObjectPath())
		assert.Nil(t, blob)
		assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
	}
}

func TestCollector_DeletesChunkedBlobs(t *testing.T) {
	ctx := context.Background()
	collector := newTestCollector(ctx, t)
	store := setupTestStore(ctx, t, collector)
	record := setupTestRecord(ctx, t, collector, store.Key)
	blob := blobref.NewChunkedBlobRef(store.Key, record.Key)
	ds := newDatastoreClient(ctx, t)
	setupTestBlobRef(ctx, t, ds, blob)

	const numChunkRefs = 5
	chunks := make([]*chunkref.ChunkRef, 0, numChunkRefs)

	// 0 and 2 are old, to be deleted
	// 1 and 3 have the applicable statuses but new
	// 4 is still initializing
	for i := 0; i < numChunkRefs; i++ {
		chunk := chunkref.New(blob.Key, int32(i))
		chunk.Timestamps.CreatedAt = collector.cfg.Before
		chunk.Timestamps.UpdatedAt = collector.cfg.Before
		chunks = append(chunks, chunk)
	}
	chunks[0].MarkForDeletion()
	chunks[0].Timestamps.UpdatedAt = collector.cfg.Before.Add(-1 * time.Microsecond)
	chunks[1].MarkForDeletion()
	chunks[2].Fail()
	chunks[2].Timestamps.UpdatedAt = collector.cfg.Before.Add(-1 * time.Microsecond)
	chunks[3].Fail()

	for _, c := range chunks {
		setupTestChunkRef(ctx, t, collector, ds, c)
		setupExternalBlob(ctx, t, collector, c.ObjectPath())
	}
	collector.run(ctx)

	exists := []bool{false, true, false, true, true}
	for i, e := range exists {
		chunk := new(chunkref.ChunkRef)
		err := ds.Get(ctx, chunkRefKey(chunks[i]), chunk)
		if e {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, datastore.ErrNoSuchEntity, err)
		}
		_, err = collector.blob.Get(ctx, chunks[i].ObjectPath())
		if e {
			assert.NoError(t, err)
		} else {
			assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
		}
	}
}
