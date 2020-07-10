package metadb_test

import (
	"testing"

	pb "github.com/googleforgames/triton/api"
	m "github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
)

func TestStore_NewStoreFromProtoNil(t *testing.T) {
	actual := m.NewStoreFromProto(nil)
	assert.NotNil(t, actual)
	assert.Equal(t, new(m.Store), actual)
}

func TestStore_ToProtoSimple(t *testing.T) {
	store := &m.Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerID: "owner",
	}
	expected := &pb.Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerId: "owner",
	}
	assert.Equal(t, expected, store.ToProto())
}

func TestStore_NewStoreFromProtoSimple(t *testing.T) {
	proto := &pb.Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerId: "owner",
	}
	expected := &m.Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerID: "owner",
	}
	assert.Equal(t, expected, m.NewStoreFromProto(proto))
}
