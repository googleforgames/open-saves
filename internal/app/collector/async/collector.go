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

package asynccollector

import (
	"context"
	"fmt"

	"github.com/cloudevents/sdk-go/v2/event"
	_ "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	"github.com/google/uuid"
	"github.com/googleapis/google-cloudevents-go/cloud/datastoredata"
	"github.com/googleforgames/open-saves/internal/pkg/blob"
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	log "github.com/sirupsen/logrus"
	"gocloud.dev/gcerrors"
	"google.golang.org/api/iterator"
)

type Config struct {
	Port      string
	Cloud     string
	Bucket    string
	Project   string
}

// AsyncCollector represents the instance
type collector struct {
	metaDB *metadb.MetaDB
	blob   blob.BlobStore
	config *Config
}

func newCollector(ctx context.Context, cfg *Config) (*collector, error) {
	log.Infof("Creating a new Open Saves garbage async collector: cloud = %v, project = %v, bucket = %v",
		cfg.Cloud, cfg.Bucket, cfg.Bucket)

	switch cfg.Cloud {
	case "gcp":
		log.Infoln("Starting Open Saves async garbage collector on GCP")
		gcs, err := blob.NewBlobGCP(ctx, cfg.Bucket)
		if err != nil {
			return nil, err
		}
		metadb, err := metadb.NewMetaDB(ctx, cfg.Project)
		if err != nil {
			log.Fatalf("Failed to create a MetaDB instance: %v", err)
			return nil, err
		}
		c := &collector{
			metaDB: metadb,
			blob: gcs,
			config: cfg,
		}
		return c, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cfg.Cloud)
	}
}

func (s *collector) deleteBlobDependencies(ctx context.Context, e event.Event) error {
	log.Debugf("New event received event=%v", e)
	data := datastoredata.EntityEventData{}
	if err := e.DataAs(&data); err != nil {
		return fmt.Errorf("could not deserialize the event=%v: %w", e, err)
	}

	blobRef, err := fromEventToBlobRef(&data)
	if err != nil {
		return fmt.Errorf("could not parse the event data=%v into a blobref: %w",&data,  err)
	}

	if blobRef.Chunked {
		// Delete all children chunks
		chunksCursor := s.metaDB.GetChildChunkRefs(ctx, blobRef.Key)
		for {
			chunk, err := chunksCursor.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Errorf("cursor.Next() returned error for blob=%s: %v", blobRef.Key, err)
				return err
			}
			if err := s.deleteChunk(ctx, chunk); err != nil {
				return err
			}
		}

		return nil
	}

	err = s.blob.Delete(ctx, blobRef.ObjectPath())
	if gcerrors.Code(err) != gcerrors.NotFound {
	    log.Warnf("couldn't find blob's object with objectPath=%s: %v", blobRef.ObjectPath(), err)
	} else if err != nil {
		return fmt.Errorf("couldn't delete the blob's object with objectPath=%s: %w", blobRef.ObjectPath(), err)
	}

	return nil
}

func (s *collector) deleteChunk(ctx context.Context, chunk *chunkref.ChunkRef) error {
	err := s.blob.Delete(ctx, chunk.ObjectPath())

	if gcerrors.Code(err) != gcerrors.NotFound {
	    log.Warnf("couldn't find chunk's object with objectPath=%s: %v", chunk.ObjectPath(), err)
	} else if err != nil {
		return fmt.Errorf("couldn't delete chunk's object with objectPath=%s: %w", chunk.ObjectPath(), err)
	}

	err = s.metaDB.DeleteChunkRef(ctx, chunk.BlobRef, chunk.Key)
	if err != nil {
		return fmt.Errorf("couldn't delete chunk from metadata DB with key=%s: %w", chunk.Key, err)
	}

	return nil
}

func fromEventToBlobRef(data *datastoredata.EntityEventData) (blobref.BlobRef, error) {
	deletedEntity := data.OldValue.Entity
	var name string
	for _, pathElement := range deletedEntity.Key.Path {
		if pathElement.GetKind() == "blob" {
			name = pathElement.GetName()
		}
	}

	if name == "" {
		return blobref.BlobRef{}, fmt.Errorf("cannot find the blob name, data=%v", data.OldValue)
	}

	key, err := uuid.Parse(name)
	if err != nil {
		return blobref.BlobRef{}, fmt.Errorf("blob name %s cannot be parsed as UUID, data=%v %w", name, data.OldValue, err)
	}

	chunkedProperty := data.OldValue.Entity.Properties["Chunked"]
	var chunked bool
	if chunkedProperty != nil {
		chunked = chunkedProperty.GetBooleanValue()
	}

	return blobref.BlobRef{
		Key: key,
		Chunked: chunked,
	}, nil
}
