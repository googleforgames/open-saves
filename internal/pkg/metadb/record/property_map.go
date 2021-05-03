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
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	pb "github.com/googleforgames/open-saves/api"
)

// PropertyMap represents user-defined custom properties.
type PropertyMap map[string]*PropertyValue

// Assert PropertyMap implements PropertyLoadSave.
var _ datastore.PropertyLoadSaver = new(PropertyMap)

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
					"error storing property, unknown type: name=[%s], type=[%s]",
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
				"error loading property, unknown type: name=[%s], type=[%s]",
				v.Name, t.Name())
		}
		(*m)[v.Name] = newValue
	}
	return nil
}
