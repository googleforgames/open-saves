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
	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func uuidToDatastoreProperty(name string, u uuid.UUID, noIndex bool) datastore.Property {
	// Output an empty string instead of "000000-..." to save space
	if u == uuid.Nil {
		return datastore.Property{
			Name:    name,
			Value:   "",
			NoIndex: noIndex,
		}
	}
	return datastore.Property{
		Name:    name,
		Value:   u.String(),
		NoIndex: noIndex,
	}
}

func datastoreLoadUUID(ps []datastore.Property, name string) (uuid.UUID, []datastore.Property, error) {
	for i, p := range ps {
		if p.Name == name {
			if s, ok := p.Value.(string); ok {
				if s == "" {
					return uuid.Nil, ps, nil
				}
				sig, err := uuid.Parse(s)
				if err != nil {
					return uuid.Nil, ps, err
				}
				// The property needs to be removed from ps before passed to LoadStruct.
				// This overwrites the slice but it seems fine with the current library.
				ps[i] = ps[len(ps)-1]
				ps = ps[:len(ps)-1]
				return sig, ps, nil
			}
			return uuid.Nil, ps, status.Errorf(codes.Internal, "UUID property is not string: %+v", p.Value)
		}
	}
	return uuid.Nil, ps, status.Errorf(codes.Internal, "UUID property is not found, name = %v", name)
}
