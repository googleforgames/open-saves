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
	pb "github.com/googleforgames/open-saves/api"
)

// PropertyValue is an internal representation of the user-defined property.
// See the Open Saves API definition for details.
type PropertyValue struct {
	Type         pb.Property_Type
	IntegerValue int64
	StringValue  string
	BooleanValue bool
}

// ToProto converts the struct to a proto.
func (p *PropertyValue) ToProto() *pb.Property {
	switch p.Type {
	case pb.Property_BOOLEAN:
		return NewBooleanPropertyProto(p.BooleanValue)
	case pb.Property_INTEGER:
		return NewIntegerPropertyProto(p.IntegerValue)
	case pb.Property_STRING:
		return NewStringPropertyProto(p.StringValue)
	}
	return new(pb.Property)
}

// NewPropertyValueFromProto creates a new Property instance from a proto.
// Passing nil returns a zero-initialized Property.
func NewPropertyValueFromProto(proto *pb.Property) *PropertyValue {
	if proto == nil {
		return new(PropertyValue)
	}
	ret := &PropertyValue{
		Type: proto.Type,
	}
	switch proto.Type {
	case pb.Property_BOOLEAN:
		ret.BooleanValue = proto.GetBooleanValue()
	case pb.Property_INTEGER:
		ret.IntegerValue = proto.GetIntegerValue()
	case pb.Property_STRING:
		ret.StringValue = proto.GetStringValue()
	}
	return ret
}

// ExtractValue returns the value that is held by the property.
func ExtractValue(p *pb.Property) interface{} {
	switch p.Type {
	case pb.Property_BOOLEAN:
		return p.GetBooleanValue()
	case pb.Property_INTEGER:
		return p.GetIntegerValue()
	case pb.Property_STRING:
		return p.GetStringValue()
	}
	return nil
}

// NewBooleanPropertyProto returns a new pb.Property with boolean value v.
func NewBooleanPropertyProto(v bool) *pb.Property {
	return &pb.Property{
		Type:  pb.Property_BOOLEAN,
		Value: &pb.Property_BooleanValue{BooleanValue: v},
	}
}

// NewIntegerPropertyProto returns a new pb.Property with integer value v.
func NewIntegerPropertyProto(v int64) *pb.Property {
	return &pb.Property{
		Type:  pb.Property_INTEGER,
		Value: &pb.Property_IntegerValue{IntegerValue: v},
	}
}

// NewStringPropertyProto returns a new pb.Property with string value v.
func NewStringPropertyProto(v string) *pb.Property {
	return &pb.Property{
		Type:  pb.Property_STRING,
		Value: &pb.Property_StringValue{StringValue: v},
	}
}
