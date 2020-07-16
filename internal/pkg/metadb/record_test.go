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
	pb "github.com/googleforgames/triton/api"
	m "github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
)

func TestProperty_ToProto(t *testing.T) {
	booleanProperty := &m.Property{Type: pb.Property_BOOLEAN, BooleanValue: true}
	booleanExpected := &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: true},
	}
	assert.Equal(t, booleanExpected, booleanProperty.ToProto())

	integerProperty := &m.Property{Type: pb.Property_INTEGER, IntegerValue: 42}
	integerExpected := &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: 42},
	}
	assert.Equal(t, integerExpected, integerProperty.ToProto())

	stringProperty := &m.Property{Type: pb.Property_STRING, StringValue: "triton"}
	stringExpected := &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: "triton"},
	}
	assert.Equal(t, stringExpected, stringProperty.ToProto())
}

func TestProperty_NewPropertyFromProtoNil(t *testing.T) {
	expected := new(m.Property)
	actual := m.NewPropertyFromProto(nil)
	assert.NotNil(t, expected, actual)
	assert.Equal(t, expected, actual)
}

func TestProperty_NewPropertyFromProto(t *testing.T) {
	booleanProto := &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: true},
	}
	booleanExpected := &m.Property{Type: pb.Property_BOOLEAN, BooleanValue: true}
	assert.Equal(t, booleanExpected, m.NewPropertyFromProto(booleanProto))

	integerProto := &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: 42},
	}
	integerExpected := &m.Property{Type: pb.Property_INTEGER, IntegerValue: 42}
	assert.Equal(t, integerExpected, m.NewPropertyFromProto(integerProto))

	stringProto := &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: "triton"},
	}
	stringExpected := &m.Property{Type: pb.Property_STRING, StringValue: "triton"}
	assert.Equal(t, stringExpected, m.NewPropertyFromProto(stringProto))

}
func TestRecord_Save(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	record := &m.Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: "",
		Properties: map[string]*m.Property{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
	}
	properties, err := record.Save()
	if err != nil {
		t.Fatalf("Save should not return err: %v", err)
	}
	expected := []datastore.Property{
		{
			Name:  "Blob",
			Value: testBlob,
		},
		{
			Name:  "BlobSize",
			Value: int64(len(testBlob)),
		},
		{
			Name:  m.PropertyPrefix + "prop1",
			Value: int64(42),
		},
		{
			Name:  m.PropertyPrefix + "prop2",
			Value: "value",
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
	}
	assert.ElementsMatch(t, expected, properties, "Save didn't return expected values.")
}

func TestRecord_Load(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
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
			Name:  m.PropertyPrefix + "prop1",
			Value: int64(42),
		},
		{
			Name:  m.PropertyPrefix + "prop2",
			Value: "value",
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
		ExternalBlob: "",
		Properties: map[string]*m.Property{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
	}
	assert.Equal(t, expected, record, "Load didn't return the expected value.")
}

func TestRecord_ToProtoSimple(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	record := &m.Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: "",
		Properties: map[string]*m.Property{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
	}
	expected := &pb.Record{
		Key:      "key",
		Blob:     testBlob,
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
		OwnerId: "owner",
		Tags:    []string{"a", "b"},
	}
	assert.Equal(t, expected, record.ToProto())
}

func TestRecord_NewRecordFromProto(t *testing.T) {
	testBlob := []byte{0x24, 0x42, 0x11}
	proto := &pb.Record{
		Key:      "key",
		Blob:     testBlob,
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
		OwnerId: "owner",
		Tags:    []string{"a", "b"},
	}
	expected := &m.Record{
		Key:          "key",
		Blob:         testBlob,
		BlobSize:     int64(len(testBlob)),
		ExternalBlob: "",
		Properties: map[string]*m.Property{
			"prop1": {Type: pb.Property_INTEGER, IntegerValue: 42},
			"prop2": {Type: pb.Property_STRING, StringValue: "value"},
		},
		OwnerID: "owner",
		Tags:    []string{"a", "b"},
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
