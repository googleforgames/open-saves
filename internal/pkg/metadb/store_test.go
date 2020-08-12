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

package metadb_test

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
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
	createdAt := time.Date(2020, 7, 14, 13, 16, 5, 0, time.UTC)
	updatedAt := time.Date(2020, 12, 28, 12, 15, 3, 0, time.UTC)
	store := &m.Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerID: "owner",
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: uuid.MustParse("A634B035-E496-4B4E-8B18-05FD2112BEF2"),
		},
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
	actual := m.NewStoreFromProto(proto)
	assert.Equal(t, expected, actual)
}

func TestStore_LoadKey(t *testing.T) {
	store := new(m.Store)
	key := datastore.NameKey("kind", "testkey", nil)
	assert.NoError(t, store.LoadKey(key))
	assert.Equal(t, "testkey", store.Key)
}
