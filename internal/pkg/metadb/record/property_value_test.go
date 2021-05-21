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

func TestPropertyValue_ToProto(t *testing.T) {
	booleanProperty := &PropertyValue{Type: pb.Property_BOOLEAN, BooleanValue: true}
	booleanExpected := &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: true},
	}
	assert.Equal(t, booleanExpected, booleanProperty.ToProto())

	integerProperty := &PropertyValue{Type: pb.Property_INTEGER, IntegerValue: 42}
	integerExpected := &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: 42},
	}
	assert.Equal(t, integerExpected, integerProperty.ToProto())

	stringProperty := &PropertyValue{Type: pb.Property_STRING, StringValue: "string value"}
	stringExpected := &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: "string value"},
	}
	assert.Equal(t, stringExpected, stringProperty.ToProto())
}

func TestPropertyValue_NewPropertyFromProtoNil(t *testing.T) {
	expected := new(PropertyValue)
	actual := NewPropertyValueFromProto(nil)
	assert.NotNil(t, expected, actual)
	assert.Equal(t, expected, actual)
}

func TestPropertyValue_NewPropertyFromProto(t *testing.T) {
	booleanProto := &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: true},
	}
	booleanExpected := &PropertyValue{Type: pb.Property_BOOLEAN, BooleanValue: true}
	assert.Equal(t, booleanExpected, NewPropertyValueFromProto(booleanProto))

	integerProto := &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: 42},
	}
	integerExpected := &PropertyValue{Type: pb.Property_INTEGER, IntegerValue: 42}
	assert.Equal(t, integerExpected, NewPropertyValueFromProto(integerProto))

	stringProto := &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: "string value"},
	}
	stringExpected := &PropertyValue{Type: pb.Property_STRING, StringValue: "string value"}
	assert.Equal(t, stringExpected, NewPropertyValueFromProto(stringProto))

}
