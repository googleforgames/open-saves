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
	Properties   map[string]Property `datastore:"-"`
	OwnerID      string
	Tags         []string
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
				r.Properties = map[string]Property{}
			}
			var newprop Property
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
