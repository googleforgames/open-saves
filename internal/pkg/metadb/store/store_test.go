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

package store

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStore_NewStoreFromProtoNil(t *testing.T) {
	actual := FromProto(nil)
	assert.NotNil(t, actual)
	assert.Equal(t, new(Store), actual)
}

func TestStore_ToProtoSimple(t *testing.T) {
	createdAt := time.Date(2020, 7, 14, 13, 16, 5, 0, time.UTC)
	updatedAt := time.Date(2020, 12, 28, 12, 15, 3, 0, time.UTC)
	store := &Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerID: "owner",
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: uuid.MustParse("A634B035-E496-4B4E-8B18-05FD2112BEF2"),
		},
	}
	expected := &pb.Store{
		Key:       "test",
		Name:      "a test store",
		Tags:      []string{"tag1"},
		OwnerId:   "owner",
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}
	assert.Equal(t, expected, store.ToProto())
}

func TestStore_NewStoreFromProtoSimple(t *testing.T) {
	createdAt := time.Date(2020, 7, 14, 13, 16, 5, 0, time.UTC)
	updatedAt := time.Date(2020, 12, 28, 12, 15, 3, 0, time.UTC)
	proto := &pb.Store{
		Key:       "test",
		Name:      "a test store",
		Tags:      []string{"tag1"},
		OwnerId:   "owner",
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}
	expected := &Store{
		Key:     "test",
		Name:    "a test store",
		Tags:    []string{"tag1"},
		OwnerID: "owner",
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
	}
	actual := FromProto(proto)
	assert.Equal(t, expected, actual)
}

func TestStore_LoadKey(t *testing.T) {
	store := new(Store)
	key := datastore.NameKey("kind", "testkey", nil)
	assert.NoError(t, store.LoadKey(key))
	assert.Equal(t, "testkey", store.Key)
}
