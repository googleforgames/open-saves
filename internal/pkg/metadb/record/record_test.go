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

package record

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums/checksumstest"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecord_Save(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	signature := uuid.MustParse("34E1A605-C0FD-4A3D-A9ED-9BA42CAFAF6E")
	record := &Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Chunked:      false,
		ChunkCount:   0,
		Properties: PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID:      "owner",
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		Tags:         []string{"a", "b"},
		Checksums:    checksumstest.RandomChecksums(t),
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: signature,
		},
	}
	properties, err := record.Save()
	if err != nil {
		t.Fatalf("Save should not return err: %v", err)
	}
	if assert.Len(t, properties, 13, "Save didn't return the expected number of elements.") {
		idx := 4
		assert.Equal(t, []datastore.Property{
			{
				Name:    "Blob",
				Value:   testBlob,
				NoIndex: true,
			},
			{
				Name:  "BlobSize",
				Value: int64(len(testBlob)),
			},
			{
				Name:    "ChunkCount",
				Value:   int64(0),
				NoIndex: false,
			},
			{
				Name:  "Chunked",
				Value: false,
			},
		}, properties[:idx])
		assert.Equal(t, properties[idx].Name, "Properties")
		assert.False(t, properties[idx].NoIndex)
		if assert.IsType(t, properties[idx].Value, &datastore.Entity{}) {
			e := properties[idx].Value.(*datastore.Entity)

			assert.Nil(t, e.Key)
			assert.ElementsMatch(t, []datastore.Property{
				{
					Name:  "prop1",
					Value: int64(42),
				},
				{
					Name:  "prop2",
					Value: "value",
				},
			}, e.Properties)
		}
		idx++
		assert.Equal(t, []datastore.Property{
			{
				Name:  "OwnerID",
				Value: "owner",
			},
			{
				Name:  "Tags",
				Value: []interface{}{"a", "b"},
			},
			{
				Name:    "OpaqueString",
				Value:   "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
				NoIndex: true,
			},
		}, properties[idx:idx+3])
		idx += 3
		checksumstest.AssertPropertyListMatch(t, record.Checksums, properties[idx:idx+3])
		idx += 3
		assert.Equal(t, []datastore.Property{
			{
				Name: "Timestamps",
				Value: &datastore.Entity{
					Properties: []datastore.Property{
						{
							Name:  "CreatedAt",
							Value: createdAt,
						},
						{
							Name:  "UpdatedAt",
							Value: updatedAt,
						},
						{
							Name:    "Signature",
							Value:   signature.String(),
							NoIndex: true,
						},
					},
				},
			},
			{
				Name:  "ExternalBlob",
				Value: "",
			},
		}, properties[idx:])
	}
}

func TestRecord_Load(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	signature := uuid.MustParse("397F94F5-F851-4969-8BD8-7828ABC473A6")
	checksums := checksumstest.RandomChecksums(t)
	properties := []datastore.Property{
		{
			Name:  "Blob",
			Value: testBlob,
		},
		{
			Name:  "BlobSize",
			Value: int64(len(testBlob)),
		},
		{
			Name:  "ExternalBlob",
			Value: "",
		},
		{
			Name:  "OwnerID",
			Value: "owner",
		},
		{
			Name:  "OpaqueString",
			Value: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		},
		{
			Name:  "Tags",
			Value: []interface{}{"a", "b"},
		},
		{
			Name: "Properties",
			Value: &datastore.Entity{
				Properties: []datastore.Property{
					{
						Name:  "prop1",
						Value: int64(42),
					},
					{
						Name:  "prop2",
						Value: "value",
					},
				},
			},
		},
		{
			Name: "Timestamps",
			Value: &datastore.Entity{
				Properties: []datastore.Property{
					{
						Name:  "CreatedAt",
						Value: createdAt,
					},
					{
						Name:  "UpdatedAt",
						Value: updatedAt,
					},
					{
						Name:  "Signature",
						Value: signature.String(),
					},
				},
			},
		},
	}
	properties = append(properties, checksumstest.ChecksumsToProperties(t, checksums)...)
	var record Record
	if err := record.Load(properties); err != nil {
		t.Fatalf("Load should not return an error: %v", err)
	}
	expected := Record{
		Key:          "",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Properties: PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID:      "owner",
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		Tags:         []string{"a", "b"},
		Checksums:    checksums,
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: signature,
		},
	}
	assert.Equal(t, expected, record)
}

func TestRecord_ToProtoSimple(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	signature := uuid.MustParse("70E894AE-1020-42E8-9710-3E2D408BC356")
	record := &Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Chunked:      true,
		ChunkCount:   1,
		Properties: PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID:      "owner",
		Tags:         []string{"a", "b"},
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: signature,
		},
	}
	expected := &pb.Record{
		Key:        "key",
		BlobSize:   int64(len(testBlob)),
		Chunked:    true,
		ChunkCount: 1,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: &pb.Property_IntegerValue{IntegerValue: 42},
			},
			"prop2": {
				Type:  pb.Property_STRING,
				Value: &pb.Property_StringValue{StringValue: "value"},
			},
		},
		OwnerId:      "owner",
		Tags:         []string{"a", "b"},
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(updatedAt),
		Signature:    signature[:],
	}
	assert.Equal(t, expected, record.ToProto())
}

func TestRecord_NewRecordFromProto(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	signature := uuid.MustParse("076D7253-9AA0-48DE-B4AF-965E87B0A1C6")
	proto := &pb.Record{
		Key:        "key",
		BlobSize:   int64(len(testBlob)),
		Chunked:    true,
		ChunkCount: 100,
		Properties: map[string]*pb.Property{
			"prop1": {
				Type:  pb.Property_INTEGER,
				Value: &pb.Property_IntegerValue{IntegerValue: 42},
			},
			"prop2": {
				Type:  pb.Property_STRING,
				Value: &pb.Property_StringValue{StringValue: "value"},
			},
		},
		OwnerId:      "owner",
		Tags:         []string{"a", "b"},
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(updatedAt),
		Signature:    signature[:],
	}
	expected := &Record{
		Key:          "key",
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Properties: PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID:      "owner",
		Tags:         []string{"a", "b"},
		OpaqueString: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		Timestamps: timestamps.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: signature,
		},
		StoreKey: "test store key",
	}
	actual, err := FromProto("test store key", proto)
	assert.Equal(t, expected, actual)
	assert.NoError(t, err)
}

func TestRecord_NewRecordFromProtoNil(t *testing.T) {
	expected := new(Record)
	actual, err := FromProto("", nil)
	assert.NotNil(t, actual)
	assert.Equal(t, expected, actual)
	assert.NoError(t, err)
}

func TestRecord_LoadKey(t *testing.T) {
	record := new(Record)
	key := datastore.NameKey("kind", "testkey", nil)
	assert.NoError(t, record.LoadKey(key))
	assert.Equal(t, "testkey", record.Key)
	assert.Empty(t, record.StoreKey)

	key = datastore.NameKey("kind", "testkey2", datastore.NameKey("parent", "parentkey", nil))
	assert.NoError(t, record.LoadKey(key))
	assert.Equal(t, "testkey2", record.Key)
	assert.Equal(t, "parentkey", record.StoreKey)
}

func TestRecord_TestBlobUUID(t *testing.T) {
	testUUID := uuid.MustParse("F7B0E446-EBBE-48A2-90BA-108C36B44F7C")
	record := &Record{
		ExternalBlob: testUUID,
		Properties:   make(PropertyMap),
	}
	properties, err := record.Save()
	assert.NoError(t, err, "Save should not return error")
	idx := len(properties) - 1
	assert.Equal(t, "ExternalBlob", properties[idx].Name)
	assert.Equal(t, testUUID.String(), properties[idx].Value)
	assert.Equal(t, false, properties[idx].NoIndex)

	actual := new(Record)
	err = actual.Load(properties)
	assert.NoError(t, err, "Load should not return error")
	assert.Equal(t, record, actual)
}

func TestRecord_CacheKey(t *testing.T) {
	r := &Record{
		Key:      "abc",
		StoreKey: "def",
	}
	key := r.CacheKey()
	assert.Equal(t, "def/abc", key)
}

func TestRecord_SerializeRecord(t *testing.T) {
	testBlob := []byte("some-bytes")
	rr := []*Record{
		{
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Unix(100, 0),
				UpdatedAt: time.Unix(110, 0),
			},
		},
		{
			Key: "some-key",
			Properties: PropertyMap{
				"prop1": {
					Type:         pb.Property_BOOLEAN,
					BooleanValue: false,
				},
				"prop2": {
					Type:         pb.Property_INTEGER,
					IntegerValue: 200,
				},
				"prop3": {
					Type:        pb.Property_STRING,
					StringValue: "string value",
				},
			},
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Unix(100, 0),
				UpdatedAt: time.Unix(110, 0),
			},
		},
		{
			Key:       "some-key",
			Blob:      testBlob,
			BlobSize:  int64(len(testBlob)),
			OwnerID:   "new-owner",
			Tags:      []string{"tag1", "tag2"},
			Checksums: checksumstest.RandomChecksums(t),
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Unix(100, 0),
				UpdatedAt: time.Unix(110, 0),
			},
		},
	}

	for _, r := range rr {
		e, err := r.EncodeBytes()
		assert.NoError(t, err)
		decoded := new(Record)
		assert.NoError(t, decoded.DecodeBytes(e))
		assert.Equal(t, r, decoded)
	}
}

func TestRecord_EmptyInvalidSignature(t *testing.T) {
	const testKey = "key"

	proto := &pb.Record{
		Key:       testKey,
		CreatedAt: timestamppb.New(time.Unix(100, 0)),
		UpdatedAt: timestamppb.New(time.Unix(110, 0)),
		Signature: nil,
	}
	if rr, err := FromProto("", proto); assert.NoError(t, err) {
		if assert.NotNil(t, rr) {
			assert.Equal(t, testKey, rr.Key)
			assert.Equal(t, uuid.Nil, rr.Timestamps.Signature)
		}
	}
	proto.Signature = []byte{}
	if rr, err := FromProto("", proto); assert.NoError(t, err) {
		if assert.NotNil(t, rr) {
			assert.Equal(t, testKey, rr.Key)
			assert.Equal(t, uuid.Nil, rr.Timestamps.Signature)
		}
	}
	proto.Signature = []byte{0xff}
	if rr, err := FromProto("", proto); assert.Error(t, err) {
		assert.Nil(t, rr)
	}
}
