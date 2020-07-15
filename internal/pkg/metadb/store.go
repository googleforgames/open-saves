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
	pb "github.com/googleforgames/triton/api"
)

// Store represents a Triton store in the metadata database.
// See the Triton API definition for details.
type Store struct {
	Key     string `datastore:"-"`
	Name    string
	Tags    []string
	OwnerID string
}

// ToProto converts the structure a proto.
func (s *Store) ToProto() *pb.Store {
	return &pb.Store{
		Key:     s.Key,
		Name:    s.Name,
		Tags:    s.Tags,
		OwnerId: s.OwnerID,
	}
}

// NewStoreFromProto creates a new Store instance from a proto.
// Passing nil returns a zero-initialized Store.
func NewStoreFromProto(p *pb.Store) *Store {
	if p == nil {
		return new(Store)
	}
	return &Store{
		Key:     p.Key,
		Name:    p.Name,
		Tags:    p.Tags,
		OwnerID: p.OwnerId,
	}
}
