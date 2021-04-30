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

import pb "github.com/googleforgames/open-saves/api"

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
	ret := &pb.Property{
		Type: p.Type,
	}
	switch p.Type {
	case pb.Property_BOOLEAN:
		ret.Value = &pb.Property_BooleanValue{BooleanValue: p.BooleanValue}
	case pb.Property_INTEGER:
		ret.Value = &pb.Property_IntegerValue{IntegerValue: p.IntegerValue}
	case pb.Property_STRING:
		ret.Value = &pb.Property_StringValue{StringValue: p.StringValue}
	}
	return ret
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
