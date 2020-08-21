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
	key := uuid.MustParse("d13c289c-8845-485f-b582-c87342d5dade")
	assert.NoError(t, blob.LoadKey(datastore.NameKey("kind", key.String(), nil)))
	assert.Equal(t, key, blob.Key)
}

func TestBlob_Save(t *testing.T) {
	blob := metadb.Blob{
		Key:        uuid.New(),
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

func newInitBlob(t *testing.T) *metadb.Blob {
	blob := new(metadb.Blob)
	const (
		size = int64(4)
		name = "abc"
	)

	// Initialize
	assert.NoError(t, blob.Initialize(size, name))
	assert.NotEqual(t, uuid.Nil, blob.Key)
	assert.Equal(t, size, blob.Size)
	assert.Equal(t, name, blob.ObjectName)
	assert.Equal(t, metadb.BlobStatusInitializing, blob.Status)
	assert.NotEmpty(t, blob.Timestamps.CreatedAt)
	assert.NotEmpty(t, blob.Timestamps.UpdatedAt)
	assert.NotEmpty(t, blob.Timestamps.Signature)
	return blob
}

func TestBlob_LifeCycle(t *testing.T) {
	blob := newInitBlob(t)

	// Invalid transitions
	assert.Error(t, blob.Initialize(0, ""))
	assert.Error(t, blob.Retire())

	// Ready
	assert.NoError(t, blob.Ready())
	assert.Equal(t, metadb.BlobStatusReady, blob.Status)

	// Invalid transitions
	assert.Error(t, blob.Initialize(0, ""))
	assert.Error(t, blob.Ready())

	// Retire
	assert.NoError(t, blob.Retire())
	assert.Equal(t, metadb.BlobStatusPendingDeletion, blob.Status)

	// Invalid transitions
	assert.Error(t, blob.Retire())
	assert.Error(t, blob.Ready())
	assert.Error(t, blob.Initialize(0, ""))
}

func TestBlob_Fail(t *testing.T) {
	blob := new(metadb.Blob)

	// Fail should fail for BlobStatusUnknown
	assert.Error(t, blob.Fail())

	blob = newInitBlob(t)
	assert.NoError(t, blob.Fail())
	blob.Status = metadb.BlobStatusPendingDeletion
	assert.NoError(t, blob.Fail())

	blob.Status = metadb.BlobStatusReady
	assert.Error(t, blob.Fail())

	blob.Status = metadb.BlobStatusError
	assert.Error(t, blob.Fail())
}
