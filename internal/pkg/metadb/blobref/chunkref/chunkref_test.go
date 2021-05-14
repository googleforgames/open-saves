package chunkref

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestChunkRef_New(t *testing.T) {
	blobuuid := uuid.New()
	c := New(blobuuid, 42, 123450)
	assert.NotEqual(t, uuid.Nil, c.Key)
	assert.Equal(t, blobuuid, c.BlobRef)
	assert.Equal(t, 42, c.Number)
	assert.Equal(t, 123450, c.Size)
}

func TestChunkRef_ObjectPath(t *testing.T) {
	c := New(uuid.Nil, 0, 0)
	assert.Equal(t, c.Key.String(), c.ObjectPath())
}
