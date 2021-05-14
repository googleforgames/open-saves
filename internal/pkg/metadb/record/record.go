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

package record

import (
	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Assert PropertyMap implements PropertyLoadSave.
var _ datastore.PropertyLoadSaver = new(PropertyMap)

// Record represents a Open Saves record in the metadata database.
// See the Open Saves API definition for details.
type Record struct {
	Key          string `datastore:"-"`
	Blob         []byte `datastore:",noindex"`
	BlobSize     int64
	ExternalBlob uuid.UUID `datastore:"-"`
	Properties   PropertyMap
	OwnerID      string
	Tags         []string
	OpaqueString string `datastore:",noindex"`

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps timestamps.Timestamps
}

// Assert Record implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(Record)
var _ datastore.KeyLoader = new(Record)

const externalBlobPropertyName = "ExternalBlob"

// Save and Load for Record replicate the default behaviors, however, they are
// explicitly required to implement the KeyLoader interface.

// Save implements the Datastore PropertyLoadSaver interface and converts struct fields
// to Datastore properties.
func (r *Record) Save() ([]datastore.Property, error) {
	properties, err := datastore.SaveStruct(r)
	if err != nil {
		return nil, err
	}
	properties = append(properties,
		timestamps.UUIDToDatastoreProperty(externalBlobPropertyName, r.ExternalBlob, false))
	return properties, nil
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datastore
// properties to corresponding struct fields.
func (r *Record) Load(ps []datastore.Property) error {
	externalBlob, ps, err := timestamps.LoadUUID(ps, externalBlobPropertyName)
	if err != nil {
		return err
	}
	r.ExternalBlob = externalBlob

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
		Key:          r.Key,
		BlobSize:     r.BlobSize,
		OwnerId:      r.OwnerID,
		Tags:         r.Tags,
		Properties:   r.Properties.ToProto(),
		OpaqueString: r.OpaqueString,
		CreatedAt:    timestamppb.New(r.Timestamps.CreatedAt),
		UpdatedAt:    timestamppb.New(r.Timestamps.UpdatedAt),
	}
	return ret
}

// FromProto creates a new Record instance from a proto.
// Passing nil returns a zero-initialized proto.
func FromProto(p *pb.Record) *Record {
	if p == nil {
		return new(Record)
	}
	return &Record{
		Key:          p.GetKey(),
		BlobSize:     p.GetBlobSize(),
		OwnerID:      p.GetOwnerId(),
		Tags:         p.GetTags(),
		Properties:   NewPropertyMapFromProto(p.GetProperties()),
		OpaqueString: p.GetOpaqueString(),
		Timestamps: timestamps.Timestamps{
			CreatedAt: p.GetCreatedAt().AsTime(),
			UpdatedAt: p.GetUpdatedAt().AsTime(),
		},
	}
}
