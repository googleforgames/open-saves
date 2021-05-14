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

package store

import (
	"cloud.google.com/go/datastore"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store represents a Open Saves store in the metadata database.
// See the Open Saves API definition for details.
type Store struct {
	Key     string `datastore:"-"`
	Name    string
	Tags    []string
	OwnerID string

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps timestamps.Timestamps
}

// Assert Store implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(Store)
var _ datastore.KeyLoader = new(Store)

// ToProto converts the structure a proto.
func (s *Store) ToProto() *pb.Store {
	return &pb.Store{
		Key:       s.Key,
		Name:      s.Name,
		Tags:      s.Tags,
		OwnerId:   s.OwnerID,
		CreatedAt: timestamppb.New(s.Timestamps.CreatedAt),
		UpdatedAt: timestamppb.New(s.Timestamps.UpdatedAt),
	}
}

// These functions need to be implemented here instead of the datastore package because
// go doesn't permit to define additional receivers in another package.
// Save and Load replicates the default behaviors, however, they are required
// for the KeyLoader interface.

// Save implements the Datastore PropertyLoadSaver interface and converts the properties
// field in the struct to separate Datastore properties.
func (s *Store) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(s)
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datstore
// properties to the Properties field.
func (s *Store) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(s, ps)
}

// LoadKey implements the KeyLoader interface and sets the value to the Key field.
func (s *Store) LoadKey(k *datastore.Key) error {
	s.Key = k.Name
	return nil
}

// FromProto creates a new Store instance from a proto.
// Passing nil returns a zero-initialized Store.
func FromProto(p *pb.Store) *Store {
	if p == nil {
		return new(Store)
	}
	return &Store{
		Key:     p.Key,
		Name:    p.Name,
		Tags:    p.Tags,
		OwnerID: p.OwnerId,
		Timestamps: timestamps.Timestamps{
			CreatedAt: p.GetCreatedAt().AsTime(),
			UpdatedAt: p.GetUpdatedAt().AsTime(),
		},
	}
}
