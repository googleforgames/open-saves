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

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
)

func TestBlob_LoadKey(t *testing.T) {
	blob := new(metadb.Blob)
	key := datastore.NameKey("kind", "testkey", nil)
	assert.NoError(t, blob.LoadKey(key))
	assert.Equal(t, "testkey", blob.Key)
}

func TestBlob_Save(t *testing.T) {
	blob := metadb.Blob{
		Key:        uuid.New().String(),
		Size:       123,
		ObjectName: "object name",
		Status:     metadb.BlobStatusInitializing,
	}
	expected := []datastore.Property{
		{
			Name:  "Size",
			Value: int64(123),
		},
		{
			Name:  "ObjectName",
			Value: "object name",
		},
		{
			Name:  "Status",
			Value: int64(metadb.BlobStatusInitializing),
		},
	}
	actual, err := blob.Save()
	assert.NoError(t, err)
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected, actual[:len(expected)])
		assert.Equal(t, 4, len(actual))
		assert.Equal(t, "Timestamps", actual[3].Name)
	}
}

func TestBlob_Load(t *testing.T) {
	properties := []datastore.Property{
		{
			Name:  "Size",
			Value: int64(123),
		},
		{
			Name:  "ObjectName",
			Value: "object name",
		},
		{
			Name:  "Status",
			Value: int64(metadb.BlobStatusReady),
		},
	}
	expected := &metadb.Blob{
		Size:       123,
		ObjectName: "object name",
		Status:     metadb.BlobStatusReady,
	}
	actual := new(metadb.Blob)
	err := actual.Load(properties)
	if assert.NoError(t, err) {
		assert.Equal(t, expected, actual)
	}
}
