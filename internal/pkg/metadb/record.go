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

package metadb

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	pb "github.com/googleforgames/triton/api"
)

// PropertyValue is an internal representation of the user-defined property.
// See the Triton API definition for details.
type PropertyValue struct {
	Type         pb.Property_Type
	IntegerValue int64
	StringValue  string
	BooleanValue bool
}

// PropertyMap represents user-defined custom properties.
type PropertyMap map[string]*PropertyValue

// Assert PropertyMap implements PropertyLoadSave.
var _ datastore.PropertyLoadSaver = new(PropertyMap)

// Record represents a Triton record in the metadata database.
// See the Triton API definition for details.
type Record struct {
	Key          string `datastore:"-"`
	Blob         []byte `datastore:",noindex"`
	BlobSize     int64
	ExternalBlob string `datastore:",noindex"`
	Properties   PropertyMap
	OwnerID      string
	Tags         []string

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps Timestamps
}

// Assert Record implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(Record)
var _ datastore.KeyLoader = new(Record)

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

// These functions are Datastore specific but need to be implemented here
// instead of the datastore package because Go doesn't permit to define
// additional receivers in another package.

// Save and Load for Record replicate the default behaviors, however, they are
// explicitly required to implement the KeyLoader interface.

// Save implements the Datastore PropertyLoadSaver interface and converts struct fields
// to Datastore properties.
func (r *Record) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(r)
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datastore
// properties to corresponding struct fields.
func (r *Record) Load(ps []datastore.Property) error {
	// Initialize Properties because the default value is a nil map and there
	// is no way to change it inside PropertyMap.Load().
	r.Properties = make(PropertyMap)
	return datastore.LoadStruct(r, ps)
}

// LoadKey implements the KeyLoader interface and sets the value to the Key field.
func (r *Record) LoadKey(k *datastore.Key) error {
	r.Key = k.Name
	return nil
}

// ToProto converts the struct to a proto.
func (r *Record) ToProto() *pb.Record {
	ret := &pb.Record{
		Key:        r.Key,
		Blob:       r.Blob,
		BlobSize:   r.BlobSize,
		OwnerId:    r.OwnerID,
		Tags:       r.Tags,
		Properties: r.Properties.ToProto(),
	}
	return ret
}

// NewRecordFromProto creates a new Record instance from a proto.
// Passing nil returns a zero-initialized proto.
func NewRecordFromProto(p *pb.Record) *Record {
	if p == nil {
		return new(Record)
	}
	return &Record{
		Key:        p.GetKey(),
		Blob:       p.GetBlob(),
		BlobSize:   p.GetBlobSize(),
		OwnerID:    p.GetOwnerId(),
		Tags:       p.GetTags(),
		Properties: NewPropertyMapFromProto(p.GetProperties()),
	}
}

// NewPropertyMapFromProto creates a new Property instance from a proto.
// Passing nil returns an empty map.
func NewPropertyMapFromProto(proto map[string]*pb.Property) PropertyMap {
	if proto == nil {
		return make(PropertyMap)
	}
	ret := make(PropertyMap)
	for k, v := range proto {
		ret[k] = NewPropertyValueFromProto(v)
	}
	return ret
}

// ToProto converts the struct to a proto.
func (m *PropertyMap) ToProto() map[string]*pb.Property {
	// This may seem wrong, but m is a pointer to a map, which is also a
	// nullable reference type.
	if m == nil || *m == nil {
		return nil
	}
	ret := make(map[string]*pb.Property)
	for k, v := range *m {
		ret[k] = v.ToProto()
	}
	return ret
}

// Save implements the Datastore PropertyLoadSaver interface and converts
// PropertyMap to a slice of datastore Properties.
func (m *PropertyMap) Save() ([]datastore.Property, error) {
	var ps []datastore.Property
	if m == nil {
		return ps, nil
	}
	for name, value := range *m {
		switch value.Type {
		case pb.Property_BOOLEAN:
			ps = append(ps, datastore.Property{
				Name:  name,
				Value: value.BooleanValue,
			})
		case pb.Property_INTEGER:
			ps = append(ps, datastore.Property{
				Name:  name,
				Value: value.IntegerValue,
			})
		case pb.Property_STRING:
			ps = append(ps, datastore.Property{
				Name:  name,
				Value: value.StringValue,
			})
		default:
			return nil,
				fmt.Errorf(
					"Error storeing property, unkown type: name=[%s], type=[%s]",
					name, value.Type.String())
		}
	}
	return ps, nil
}

// Load implements the Datastore PropertyLoadSaver interface and converts
// individual properties to PropertyMap.
func (m *PropertyMap) Load(ps []datastore.Property) error {
	if ps == nil || len(ps) == 0 {
		// No custom properties
		return nil
	}
	if m == nil || *m == nil {
		// I don't think this should happen because the Datastore Go client
		// always zero-initializes the target before calling Load.
		return fmt.Errorf("PropertyMap.Load was called on nil")
	}

	for _, v := range ps {
		var newValue = new(PropertyValue)
		t := reflect.TypeOf(v.Value)
		switch t.Kind() {
		case reflect.Bool:
			newValue.Type = pb.Property_BOOLEAN
			newValue.BooleanValue = v.Value.(bool)
		case reflect.Int64:
			newValue.Type = pb.Property_INTEGER
			newValue.IntegerValue = v.Value.(int64)
		case reflect.String:
			newValue.Type = pb.Property_STRING
			newValue.StringValue = v.Value.(string)
		default:
			return fmt.Errorf(
				"Error loading property, unknown type: name=[%s], type=[%s]",
				v.Name, t.Name())
		}
		(*m)[v.Name] = newValue
	}
	return nil
}
