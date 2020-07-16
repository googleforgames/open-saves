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
	log "github.com/sirupsen/logrus"
)

// Property is an internal representation of the user-defined property.
// See the Triton API definition for details.
type Property struct {
	Type         pb.Property_Type
	IntegerValue int64
	StringValue  string
	BooleanValue bool
}

// Record represents a Triton record in the metadata database.
// See the Triton API definition for details.
type Record struct {
	Key          string `datastore:"-"`
	Blob         []byte
	BlobSize     int64
	ExternalBlob string
	Properties   map[string]*Property `datastore:"-"`
	OwnerID      string
	Tags         []string
}

// Assert Record implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(Record)
var _ datastore.KeyLoader = new(Record)

// ToProto converts the struct to a proto.
func (p *Property) ToProto() *pb.Property {
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

// NewPropertyFromProto creates a new Property instance from a proto.
// Passing nil returns a zero-initialized Property.
func NewPropertyFromProto(p *pb.Property) *Property {
	if p == nil {
		return new(Property)
	}
	ret := &Property{
		Type: p.Type,
	}
	switch p.Type {
	case pb.Property_BOOLEAN:
		ret.BooleanValue = p.GetBooleanValue()
	case pb.Property_INTEGER:
		ret.IntegerValue = p.GetIntegerValue()
	case pb.Property_STRING:
		ret.StringValue = p.GetStringValue()
	}
	return ret
}

// PropertyPrefix is necessary to avoid potential conflict between internal and user defined
// properties.
const PropertyPrefix = "CP"

// These functions need to be implemented here instead of the datastore package because
// go doesn't permit to define additional receivers in another package.

// Save implements the Datastore PropertyLoadSaver interface and converts the properties
// field in the struct to separate Datastore properties.
func (r *Record) Save() ([]datastore.Property, error) {
	dsprop, err := datastore.SaveStruct(r)
	if err != nil {
		return nil, err
	}
	for k, v := range r.Properties {
		name := PropertyPrefix + k
		switch v.Type {
		case pb.Property_BOOLEAN:
			dsprop = append(dsprop, datastore.Property{
				Name:  name,
				Value: v.BooleanValue,
			})
		case pb.Property_INTEGER:
			dsprop = append(dsprop, datastore.Property{
				Name:  name,
				Value: v.IntegerValue,
			})
		case pb.Property_STRING:
			dsprop = append(dsprop, datastore.Property{
				Name:  name,
				Value: v.StringValue,
			})
		default:
			return nil,
				fmt.Errorf(
					"Error storeing property, unkown type: name=[%s], type=[%s]",
					k, v.Type.String())
		}
	}
	return dsprop, nil
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datstore
// properties to the Properties field.
func (r *Record) Load(ps []datastore.Property) error {
	for _, p := range ps {
		switch p.Name {
		case "Blob":
			r.Blob = p.Value.([]byte)
		case "BlobSize":
			r.BlobSize = p.Value.(int64)
		case "ExternalBlob":
			r.ExternalBlob = p.Value.(string)
		case "OwnerID":
			r.OwnerID = p.Value.(string)
		case "Tags":
			tags := []string{}
			for _, tag := range p.Value.([]interface{}) {
				tags = append(tags, tag.(string))
			}
			r.Tags = tags
		default:
			// Everything else is a property
			if PropertyPrefix != p.Name[:len(PropertyPrefix)] {
				log.Warnf("Unknown property found, property name = %s", p.Name)
				continue
			}
			// Allocate a new map only when there are user-defined properties.
			if r.Properties == nil {
				r.Properties = make(map[string]*Property)
			}
			var newprop = new(Property)
			t := reflect.TypeOf(p.Value)
			switch t.Kind() {
			case reflect.Bool:
				newprop.Type = pb.Property_BOOLEAN
				newprop.BooleanValue = p.Value.(bool)
			case reflect.Int64:
				newprop.Type = pb.Property_INTEGER
				newprop.IntegerValue = p.Value.(int64)
			case reflect.String:
				newprop.Type = pb.Property_STRING
				newprop.StringValue = p.Value.(string)
			default:
				return fmt.Errorf(
					"Error loading property, unknown type: name=[%s], type=[%s]",
					p.Name, t.Name())
			}
			name := p.Name[len(PropertyPrefix):]
			r.Properties[name] = newprop
		}
	}
	return nil
}

// LoadKey implements the KeyLoader interface and sets the value to the Key field.
func (r *Record) LoadKey(k *datastore.Key) error {
	r.Key = k.Name
	return nil
}

// ToProto converts the struct to a proto.
func (r *Record) ToProto() *pb.Record {
	ret := &pb.Record{
		Key:      r.Key,
		Blob:     r.Blob,
		BlobSize: r.BlobSize,
		OwnerId:  r.OwnerID,
		Tags:     r.Tags,
	}
	if r.Properties != nil {
		ret.Properties = make(map[string]*pb.Property)
		for k, v := range r.Properties {
			ret.Properties[k] = v.ToProto()
		}
	}
	return ret
}

// NewRecordFromProto creates a new Record instance from a proto.
// Passing nil returns a zero-initialized proto.
func NewRecordFromProto(p *pb.Record) *Record {
	if p == nil {
		return new(Record)
	}
	ret := &Record{
		Key:      p.GetKey(),
		Blob:     p.GetBlob(),
		BlobSize: p.GetBlobSize(),
		OwnerID:  p.GetOwnerId(),
		Tags:     p.GetTags(),
	}
	properties := p.GetProperties()
	if properties != nil {
		ret.Properties = make(map[string]*Property)
		for k, v := range properties {
			ret.Properties[k] = NewPropertyFromProto(v)
		}
	}
	return ret
}
