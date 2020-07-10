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
