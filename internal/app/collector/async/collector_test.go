package asynccollector

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/datastore"
	protobuf "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/googleapis/google-cloudevents-go/cloud/datastoredata"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/gcerrors"
)

const (
	defaultTestProject = "triton-for-games-dev"
	defaultTestBucket  = "gs://triton-integration"
	testCacheAddr      = "localhost:6379"
	blobKind           = "blob"
	chunkKind          = "chunk"
)

func getTestProject() string {
	if testProject, ok := os.LookupEnv("TEST_PROJECT_ID"); ok {
		return testProject
	}

	return defaultTestProject
}

func getTestBucket() string {
	if testBucket, ok := os.LookupEnv("TEST_BUCKET"); ok {
		return testBucket
	}

	return defaultTestBucket
}

func newTestCollector(ctx context.Context, t *testing.T) *collector {
	t.Helper()
	cfg := &Config{
		Cloud:     "gcp",
		Bucket:    getTestBucket(),
		Project:   getTestProject(),
		LogLevel:  "debug",
	}
	c, err := newCollector(ctx, cfg)
	if err != nil {
		t.Fatalf("newCollector returned error: %v", err)
	}
	return c
}

func setupTestStore(ctx context.Context, t *testing.T, collector *collector) *store.Store {
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
	ds, err := datastore.NewClient(ctx, getTestProject())
	if err != nil {
		t.Fatalf("Failed to create a new Datastore client: %v", err)
	}
	return ds
}

func newBlobDeletedEvent(blob *blobref.BlobRef) event.Event {
	entity := datastoredata.Entity {
		Key: &datastoredata.Key{
			Path: []*datastoredata.Key_PathElement{
				{
					Kind: "blob",
					IdType: &datastoredata.Key_PathElement_Name{
						Name: blob.Key.String(),
					},
				},
			},
		},
		Properties: map[string]*datastoredata.Value{
			"Chunked": {
				ValueType: &datastoredata.Value_BooleanValue{BooleanValue: blob.Chunked},
			},
		},
	}

	entityEventData := datastoredata.EntityEventData {
		OldValue: &datastoredata.EntityResult{
			Entity: &entity,
		},
	}

	event := event.New()
	event.SetData(protobuf.ContentTypeProtobuf, &entityEventData)

	return event
}

func setupTestRecord(ctx context.Context, t *testing.T, collector *collector, storeKey string) *record.Record {
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

func chunkRefKey(chunk *chunkref.ChunkRef) *datastore.Key {
	return datastore.NameKey(chunkKind, chunk.Key.String(),
		datastore.NameKey(blobKind, chunk.BlobRef.String(), nil))
}

func setupTestChunkRef(ctx context.Context, t *testing.T, collector *collector, ds *datastore.Client, blob *blobref.BlobRef, chunk *chunkref.ChunkRef) {
	t.Helper()

	if err := collector.metaDB.InsertChunkRef(ctx, blob, chunk); err != nil {
		t.Fatalf("InsertChunkRef failed: %v", err)
	}
	t.Cleanup(func() {
		ds.Delete(ctx, chunkRefKey(chunk))
	})
}

func setupExternalBlob(ctx context.Context, t *testing.T, collector *collector, path string) {
	t.Helper()
	err := collector.blob.Put(ctx, path, []byte{})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	t.Cleanup(func() {
		collector.blob.Delete(ctx, path)
	})
}

func TestCollector_DeleteBlob(t *testing.T) {
	ctx := context.Background()
	collector := newTestCollector(ctx, t)
	store := setupTestStore(ctx, t, collector)
	record := setupTestRecord(ctx, t, collector, store.Key)
	blobRef := blobref.NewBlobRef(0, store.Key, record.Key)

	setupExternalBlob(ctx, t, collector, blobRef.ObjectPath())

	event := newBlobDeletedEvent(blobRef)
	err := collector.deleteBlobDependencies(ctx, event)
	require.NoError(t, err)

	blob, err := collector.blob.Get(ctx, blobRef.ObjectPath())
	assert.Nil(t, blob)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}

func TestCollector_DeleteChunkedBlob(t *testing.T) {
	ctx := context.Background()
	collector := newTestCollector(ctx, t)
	ds := newDatastoreClient(ctx, t)
	store := setupTestStore(ctx, t, collector)
	record := setupTestRecord(ctx, t, collector, store.Key)
	blobRef := blobref.NewBlobRef(0, store.Key, record.Key)
	blobRef.Chunked = true

	chunkRef := chunkref.New(blobRef.Key, int32(0))
	setupTestChunkRef(ctx, t, collector, ds, blobRef, chunkRef)
	setupExternalBlob(ctx, t, collector, chunkRef.ObjectPath())

	event := newBlobDeletedEvent(blobRef)
	err := collector.deleteBlobDependencies(ctx, event)
	require.NoError(t, err)

	found := new(chunkref.ChunkRef)
	err = ds.Get(ctx, chunkRefKey(chunkRef), found)
	assert.ErrorIs(t, err, datastore.ErrNoSuchEntity)

	blob, err := collector.blob.Get(ctx, chunkRef.ObjectPath())
	assert.Nil(t, blob)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}
