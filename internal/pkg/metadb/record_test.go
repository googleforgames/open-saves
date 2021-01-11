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
	pb "github.com/googleforgames/open-saves/api"
	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func TestPropertyValue_ToProto(t *testing.T) {
	booleanProperty := &m.PropertyValue{Type: pb.Property_BOOLEAN, BooleanValue: true}
	booleanExpected := &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: true},
	}
	assert.Equal(t, booleanExpected, booleanProperty.ToProto())

	integerProperty := &m.PropertyValue{Type: pb.Property_INTEGER, IntegerValue: 42}
	integerExpected := &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: 42},
	}
	assert.Equal(t, integerExpected, integerProperty.ToProto())

	stringProperty := &m.PropertyValue{Type: pb.Property_STRING, StringValue: "string value"}
	stringExpected := &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: "string value"},
	}
	assert.Equal(t, stringExpected, stringProperty.ToProto())
}

func TestPropertyValue_NewPropertyFromProtoNil(t *testing.T) {
	expected := new(m.PropertyValue)
	actual := m.NewPropertyValueFromProto(nil)
	assert.NotNil(t, expected, actual)
	assert.Equal(t, expected, actual)
}

func TestPropertyValue_NewPropertyFromProto(t *testing.T) {
	booleanProto := &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: true},
	}
	booleanExpected := &m.PropertyValue{Type: pb.Property_BOOLEAN, BooleanValue: true}
	assert.Equal(t, booleanExpected, m.NewPropertyValueFromProto(booleanProto))

	integerProto := &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: 42},
	}
	integerExpected := &m.PropertyValue{Type: pb.Property_INTEGER, IntegerValue: 42}
	assert.Equal(t, integerExpected, m.NewPropertyValueFromProto(integerProto))

	stringProto := &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: "string value"},
	}
	stringExpected := &m.PropertyValue{Type: pb.Property_STRING, StringValue: "string value"}
	assert.Equal(t, stringExpected, m.NewPropertyValueFromProto(stringProto))

}

func TestPropertyMap_NewPropertyMapFromProto(t *testing.T) {
	// Handles nil
	assert.Equal(t, make(m.PropertyMap), m.NewPropertyMapFromProto(nil))

	proto := map[string]*pb.Property{
		"boolean": {Type: pb.Property_BOOLEAN,
			Value: &pb.Property_BooleanValue{BooleanValue: true}},
		"int": {Type: pb.Property_INTEGER,
			Value: &pb.Property_IntegerValue{IntegerValue: -50}},
		"string": {Type: pb.Property_STRING,
			Value: &pb.Property_StringValue{StringValue: "abc"},
		},
	}
	expected := m.PropertyMap{
		"boolean": {Type: pb.Property_BOOLEAN, BooleanValue: true},
		"int":     {Type: pb.Property_INTEGER, IntegerValue: -50},
		"string":  {Type: pb.Property_STRING, StringValue: "abc"},
	}
	actual := m.NewPropertyMapFromProto(proto)
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected, actual)
	}
}

func TestPropertyMap_ToProto(t *testing.T) {
	// Handles nil
	var nilMap m.PropertyMap
	assert.Nil(t, nilMap.ToProto())

	propertyMap := m.PropertyMap{
		"boolean": {Type: pb.Property_BOOLEAN, BooleanValue: true},
		"int":     {Type: pb.Property_INTEGER, IntegerValue: -50},
		"string":  {Type: pb.Property_STRING, StringValue: "abc"},
	}
	expected := map[string]*pb.Property{
		"boolean": {Type: pb.Property_BOOLEAN,
			Value: &pb.Property_BooleanValue{BooleanValue: true}},
		"int": {Type: pb.Property_INTEGER,
			Value: &pb.Property_IntegerValue{IntegerValue: -50}},
		"string": {Type: pb.Property_STRING,
			Value: &pb.Property_StringValue{StringValue: "abc"},
		},
	}
	actual := propertyMap.ToProto()
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected, actual)
	}
}

func TestRecord_Save(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	signature := uuid.MustParse("34E1A605-C0FD-4A3D-A9ED-9BA42CAFAF6E")
	record := &m.Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Properties: m.PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: signature,
		},
	}
	properties, err := record.Save()
	if err != nil {
		t.Fatalf("Save should not return err: %v", err)
	}
	if assert.Len(t, properties, 7, "Save didn't return the expected number of elements.") {
		idx := 2
		assert.Equal(t, properties[:idx], []datastore.Property{
			{
				Name:    "Blob",
				Value:   testBlob,
				NoIndex: true,
			},
			{
				Name:  "BlobSize",
				Value: int64(len(testBlob)),
			},
		})
		assert.Equal(t, properties[idx].Name, "Properties")
		assert.False(t, properties[idx].NoIndex)
		if assert.IsType(t, properties[idx].Value, &datastore.Entity{}) {
			e := properties[idx].Value.(*datastore.Entity)

			assert.Nil(t, e.Key)
			assert.ElementsMatch(t, e.Properties, []datastore.Property{
				{
					Name:  "prop1",
					Value: int64(42),
				},
				{
					Name:  "prop2",
					Value: "value",
				},
			})
		}
		idx++
		assert.Equal(t, properties[idx:], []datastore.Property{
			{
				Name:  "OwnerID",
				Value: "owner",
			},
			{
				Name:  "Tags",
				Value: []interface{}{"a", "b"},
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
		})
	}
}

func TestRecord_Load(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	signature := uuid.MustParse("397F94F5-F851-4969-8BD8-7828ABC473A6")
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
	var record m.Record
	if err := record.Load(properties); err != nil {
		t.Fatalf("Load should not return an error: %v", err)
	}
	expected := m.Record{
		Key:          "",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Properties: m.PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
		Timestamps: m.Timestamps{
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
	record := &m.Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Properties: m.PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Signature: uuid.MustParse("70E894AE-1020-42E8-9710-3E2D408BC356"),
		},
	}
	expected := &pb.Record{
		Key:      "key",
		BlobSize: int64(len(testBlob)),
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
		OwnerId:   "owner",
		Tags:      []string{"a", "b"},
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}
	assert.Equal(t, expected, record.ToProto())
}

func TestRecord_NewRecordFromProto(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	createdAt := time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC)
	updatedAt := time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC)
	proto := &pb.Record{
		Key:      "key",
		BlobSize: int64(len(testBlob)),
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
		OwnerId:   "owner",
		Tags:      []string{"a", "b"},
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}
	expected := &m.Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: uuid.Nil,
		Properties: m.PropertyMap{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
	}
	actual := m.NewRecordFromProto(proto)
	assert.Equal(t, expected, actual)
}

func TestRecord_NewRecordFromProtoNil(t *testing.T) {
	expected := new(m.Record)
	actual := m.NewRecordFromProto(nil)
	assert.NotNil(t, actual)
	assert.Equal(t, expected, actual)
}

func TestRecord_LoadKey(t *testing.T) {
	record := new(m.Record)
	key := datastore.NameKey("kind", "testkey", nil)
	assert.NoError(t, record.LoadKey(key))
	assert.Equal(t, "testkey", record.Key)
}

func TestRecord_TestBlobUUID(t *testing.T) {
	testUUID := uuid.MustParse("F7B0E446-EBBE-48A2-90BA-108C36B44F7C")
	record := &m.Record{
		ExternalBlob: testUUID,
		Properties:   make(m.PropertyMap),
	}
	properties, err := record.Save()
	assert.NoError(t, err, "Save should not return error")
	idx := len(properties) - 1
	assert.Equal(t, "ExternalBlob", properties[idx].Name)
	assert.Equal(t, testUUID.String(), properties[idx].Value)
	assert.Equal(t, false, properties[idx].NoIndex)

	actual := new(m.Record)
	err = actual.Load(properties)
	assert.NoError(t, err, "Load should not return error")
	assert.Equal(t, record, actual)
}
