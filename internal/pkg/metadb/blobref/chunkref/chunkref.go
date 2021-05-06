package chunkref

import (
	"bytes"
	"encoding/gob"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
)

type ChunkRef struct {
	Key     uuid.UUID `datastore:"-"`
	BlobRef uuid.UUID `datastore:"-"`

	Number     int32
	Size       int32
	Timestamps timestamps.Timestamps
}

// Assert Chunk implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(ChunkRef)
var _ datastore.KeyLoader = new(ChunkRef)

func (c *ChunkRef) LoadKey(k *datastore.Key) error {
	if uuidKey, err := uuid.Parse(k.Name); err == nil {
		c.Key = uuidKey
	} else {
		return err
	}
	if k.Parent != nil {
		if uuidParent, err := uuid.Parse(k.Parent.Name); err == nil {
			c.BlobRef = uuidParent
		} else {
			return err
		}
	}
	return nil
}

// Save and Load replicates the default behaviors, however, they are required
// for the KeyLoader interface.

func (c *ChunkRef) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(c, ps)
}

func (c *ChunkRef) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(c)
}

func (c *ChunkRef) ObjectPath() string {
	return c.Key.String()
}

// New creates a new ChunkRef instance with the input parameters.
func New(blobRef uuid.UUID, number, size int32) *ChunkRef {
	return &ChunkRef{
		Key:     uuid.New(),
		BlobRef: blobRef,
		Number:  number,
		Size:    size,
	}
}

func (c *ChunkRef) EncodeBytes() ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	if err := e.Encode(c); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *ChunkRef) DecodeBytes(by []byte) error {
	b := bytes.NewBuffer(by)
	d := gob.NewDecoder(b)
	return d.Decode(c)
}
