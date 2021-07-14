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

package blobref

import (
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBlobRef_LoadKey(t *testing.T) {
	key := uuid.MustParse("d13c289c-8845-485f-b582-c87342d5dade")
	blob := new(BlobRef)
	assert.NoError(t, blob.LoadKey(datastore.NameKey("blob", key.String(), nil)))
	assert.Equal(t, key, blob.Key)
}

func TestBlobRef_Save(t *testing.T) {
	const (
		size       = int64(123)
		objectName = "object name"
		store      = "store"
		record     = "record"
	)

	blob := BlobRef{
		Size:      size,
		Status:    StatusInitializing,
		StoreKey:  store,
		RecordKey: record,
	}
	expected := []datastore.Property{
		{
			Name:  "Size",
			Value: size,
		},
		{
			Name:  "Status",
			Value: int64(StatusInitializing),
		},
		{
			Name:  "StoreKey",
			Value: store,
		},
		{
			Name:  "RecordKey",
			Value: record,
		},
		{
			Name:  "Chunked",
			Value: false,
		},
	}
	actual, err := blob.Save()
	assert.NoError(t, err)
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected, actual[:len(expected)])
		if assert.Equal(t, 6, len(actual)) {
			assert.Equal(t, "Timestamps", actual[5].Name)
		}
	}
}

func TestBlobRef_Load(t *testing.T) {
	const (
		size       = int64(123)
		objectName = "object name"
		store      = "store"
		record     = "record"
	)
	properties := []datastore.Property{
		{
			Name:  "Size",
			Value: size,
		},
		{
			Name:  "Status",
			Value: int64(StatusReady),
		},
		{
			Name:  "StoreKey",
			Value: store,
		},
		{
			Name:  "RecordKey",
			Value: record,
		},
	}
	expected := &BlobRef{
		Size:      123,
		Status:    StatusReady,
		StoreKey:  store,
		RecordKey: record,
	}
	actual := new(BlobRef)
	err := actual.Load(properties)
	if assert.NoError(t, err) {
		assert.Equal(t, expected, actual)
	}
}

func TestBlobRef_GetObjectPath(t *testing.T) {
	blob := NewBlobRef(0, "", "")

	assert.Equal(t, blob.Key.String(), blob.ObjectPath())
}

func TestBlobRef_ToProto(t *testing.T) {
	const (
		size   = int64(123)
		store  = "store"
		record = "record"
	)
	b := NewBlobRef(size, store, record)

	proto := b.ToProto()
	if assert.NotNil(t, proto) {
		assert.Equal(t, b.StoreKey, proto.GetStoreKey())
		assert.Equal(t, b.RecordKey, proto.GetRecordKey())
		assert.Equal(t, b.Size, proto.GetSize())
	}
}
