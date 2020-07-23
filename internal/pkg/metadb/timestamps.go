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
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	signaturePropertyName = "Signature"
)

// Timestamps keeps keeps when each record is created or updated as well as
// a randomly generated UUID to keep consistency under concurrent writes.
// It should be embedded to metadata entities such as Record and Store.
type Timestamps struct {
	// CreatedAt is the timestamp of the record creation time
	// Automatically set by MetaDB
	CreatedAt time.Time
	// UpdatedAt is the timestamp of the last modification time
	// Automatically set and managed by MetaDB
	UpdatedAt time.Time
	// Signature is a UUID that is randomly created each time the record is updated
	// Automatically set and managed by MetaDB
	Signature uuid.UUID `datastore:"-"`
}

// Assert Timestamps implements the PropertyLoadSaver interface.
var _ datastore.PropertyLoadSaver = new(Timestamps)

// NewTimestamps sets CreatedAt and UpdatedAt to time.Now() and Signature to uuid.New().
func (t *Timestamps) NewTimestamps() {
	now := time.Now()
	t.CreatedAt = now
	t.UpdatedAt = now
	t.Signature = uuid.New()
}

// UpdateTimestamps updates the UpdatedAt and Signature fields with time.Now() and uuid.New().
func (t *Timestamps) UpdateTimestamps() {
	t.UpdatedAt = time.Now()
	t.Signature = uuid.New()
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datastore
// properties to corresponding struct fields.
func (t *Timestamps) Load(ps []datastore.Property) error {
	for i, p := range ps {
		if p.Name == signaturePropertyName {
			if s, ok := p.Value.(string); ok {
				sig, err := uuid.Parse(s)
				if err != nil {
					return err
				}
				t.Signature = sig
				// Signature needs to be removed from ps before passed to LoadStruct.
				// This overwrites the ps argument but it seems fine with the current library.
				ps[i] = ps[len(ps)-1]
				ps = ps[:len(ps)-1]
				break
			} else {
				return status.Errorf(codes.Internal, "Signature property is not string: %+v", p.Value)
			}
		}
	}
	return datastore.LoadStruct(t, ps)
}

// Save implements the Datastore PropertyLoadSaver interface and converts the properties
// field in the struct to separate Datastore properties.
func (t *Timestamps) Save() ([]datastore.Property, error) {
	ps, err := datastore.SaveStruct(t)
	if err != nil {
		return nil, err
	}
	ps = append(ps, datastore.Property{
		Name:    signaturePropertyName,
		Value:   t.Signature.String(),
		NoIndex: true,
	})
	return ps, nil
}
