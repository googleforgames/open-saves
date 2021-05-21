// Copyright 2021 Google LLC
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

	pb "github.com/googleforgames/open-saves/api"
	"github.com/stretchr/testify/assert"
)

func TestPropertyMap_NewPropertyMapFromProto(t *testing.T) {
	// Handles nil
	assert.Equal(t, make(PropertyMap), record.NewPropertyMapFromProto(nil))

	proto := map[string]*pb.Property{
		"boolean": {Type: pb.Property_BOOLEAN,
			Value: &pb.Property_BooleanValue{BooleanValue: true}},
		"int": {Type: pb.Property_INTEGER,
			Value: &pb.Property_IntegerValue{IntegerValue: -50}},
		"string": {Type: pb.Property_STRING,
			Value: &pb.Property_StringValue{StringValue: "abc"},
		},
	}
	expected := record.PropertyMap{
		"boolean": {Type: pb.Property_BOOLEAN, BooleanValue: true},
		"int":     {Type: pb.Property_INTEGER, IntegerValue: -50},
		"string":  {Type: pb.Property_STRING, StringValue: "abc"},
	}
	actual := record.NewPropertyMapFromProto(proto)
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected, actual)
	}
}

func TestPropertyMap_ToProto(t *testing.T) {
	// Handles nil
	var nilMap record.PropertyMap
	assert.Nil(t, nilMap.ToProto())

	propertyMap := record.PropertyMap{
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
