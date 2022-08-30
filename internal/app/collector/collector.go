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
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/blob"
	"github.com/googleforgames/open-saves/internal/pkg/cache"
	"github.com/googleforgames/open-saves/internal/pkg/cache/redis"
	"github.com/googleforgames/open-saves/internal/pkg/config"
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	log "github.com/sirupsen/logrus"
	"gocloud.dev/gcerrors"
	"google.golang.org/api/iterator"
)

// Config defines common fields needed to start the garbage collector.
type Config struct {
	Cloud   string
	Bucket  string
	Cache   string
	Project string
	Before  time.Time
}

// Collector is a garbage collector of unused resources in Datastore.
type Collector struct {
	cache  *cache.Cache
	metaDB *metadb.MetaDB
	blob   blob.BlobStore
	cfg    *Config
}

func newCollector(ctx context.Context, cfg *Config) (*Collector, error) {
	log.Infof("Creating a new Open Saves garbage collector: cloud = %v, project = %v, bucket = %v, cache address = %v",
		cfg.Cloud, cfg.Project, cfg.Bucket, cfg.Cache)

	switch cfg.Cloud {
	case "gcp":
		log.Infoln("Starting Open Saves garbage collector on GCP")
		gcs, err := blob.NewBlobGCP(ctx, cfg.Bucket)
		if err != nil {
			return nil, err
		}
		metadb, err := metadb.NewMetaDB(ctx, cfg.Project)
		if err != nil {
			log.Fatalf("Failed to create a MetaDB instance: %v", err)
			return nil, err
		}
		cache := cache.New(redis.NewRedis(cfg.Cache), &config.CacheConfig{})
		c := &Collector{
			blob:   gcs,
			metaDB: metadb,
			cache:  cache,
			cfg:    cfg,
		}
		return c, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cfg.Cloud)
	}
}

// Run the collector according to cfg.
func Run(ctx context.Context, cfg *Config) {
	c, err := newCollector(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create a new Collector instance: %v", err)
		return
	}
	c.run(ctx)
}

func (c *Collector) run(ctx context.Context) {
	var statuses = []blobref.Status{
		blobref.StatusPendingDeletion,
		blobref.StatusError,
		blobref.StatusInitializing,
	}
	for _, s := range statuses {
		c.deleteMatchingBlobRefs(ctx, s, c.cfg.Before)
		c.deleteMatchingChunkRefs(ctx, s, c.cfg.Before)
	}
}

func (c *Collector) deleteChunk(ctx context.Context, chunk *chunkref.ChunkRef) error {
	if err := c.blob.Delete(ctx, chunk.ObjectPath()); err != nil {
		if gcerrors.Code(err) != gcerrors.NotFound {
			log.Errorf("Blob.Delete failed for chunkref key(%v): %v", chunk.Key, err)
			if chunk.Status != blobref.StatusError {
				chunk.Fail()
				if err := c.metaDB.UpdateChunkRef(ctx, chunk); err != nil {
					log.Errorf("MetaDB.UpdateChunkRef failed for key(%v): %v", chunk.Key, err)
				}
			}
			return err
		} else {
			log.Warnf("Blob (%v) was not found.", chunk.ObjectPath())
		}
	}
	return nil
}

func (c *Collector) deleteChildChunks(ctx context.Context, blobKey uuid.UUID) error {
	cur := c.metaDB.GetChildChunkRefs(ctx, blobKey)
	for {
		chunk, err := cur.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Errorf("cursor.Next() returned error: %v", err)
			return err
		}
		if err := c.deleteChunk(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) markBlobFailed(ctx context.Context, blob *blobref.BlobRef) {
	if blob.Status != blobref.StatusError {
		blob.Fail()
		_, err := c.metaDB.UpdateBlobRef(ctx, blob)
		if err != nil {
			log.Errorf("MetaDB.UpdateBlobRef failed for key(%v): %v", blob.Key, err)
		}
	}
}

func (c *Collector) deleteBlob(ctx context.Context, blob *blobref.BlobRef) {
	if blob.Chunked {
		if err := c.deleteChildChunks(ctx, blob.Key); err != nil {
			c.markBlobFailed(ctx, blob)
			return
		}
	} else {
		if err := c.blob.Delete(ctx, blob.ObjectPath()); err != nil {
			if gcerrors.Code(err) != gcerrors.NotFound {
				log.Errorf("Blob.Delete failed for key(%v): %v", blob.Key, err)
				c.markBlobFailed(ctx, blob)
				return
			} else {
				log.Warnf("Blob (%v) was not found. Deleting BlobRef (%v) anyway.", blob.ObjectPath(), blob.Key)
			}
		}
	}
	if err := c.metaDB.DeleteBlobRef(ctx, blob.Key); err != nil {
		log.Errorf("DeleteBlobRef failed for key(%v): %v", blob.Key, err)
	} else {
		log.Infof("Deleted BlobRef (%v), status = %v", blob.Key, blob.Status)
	}
}

func (c *Collector) deleteMatchingBlobRefs(ctx context.Context, status blobref.Status, olderThan time.Time) error {
	log.Infof("Garbage collecting BlobRef objects with status = %v, and older than %v", status, olderThan)
	cursor, err := c.metaDB.ListBlobRefsByStatus(ctx, status, olderThan)
	if err != nil {
		log.Fatalf("ListBlobRefsByStatus returned error: %v", err)
		return err
	}
	for {
		blob, err := cursor.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Errorf("cursor.Next() returned error: %v", err)
			break
		}
		c.deleteBlob(ctx, blob)
	}
	return nil
}

func (c *Collector) deleteMatchingChunkRefs(ctx context.Context, status blobref.Status, olderThan time.Time) error {
	log.Infof("Garbage collecting ChunkRef objects with status = %v, older than %v", status, olderThan)
	cursor := c.metaDB.ListChunkRefsByStatus(ctx, status, olderThan)
	for {
		chunk, err := cursor.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Errorf("cursor.Next() return error: %v", err)
			return err
		}
		if err := c.deleteChunk(ctx, chunk); err != nil {
			log.Errorf("deleteChunk failed for chunk (%v): %v", chunk.Key, err)
			continue
		}
		if err := c.metaDB.DeleteChunkRef(ctx, chunk.BlobRef, chunk.Key); err != nil {
			log.Errorf("DeleteChunkRef failed for chunk (%v): %v", chunk.Key, err)
		}
	}
	return nil
}
