// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/golang/protobuf/ptypes/timestamp"
	pb "github.com/googleforgames/open-saves/api"
)

type Cache interface {
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	ListKeys(ctx context.Context) ([]string, error)
	FlushAll(ctx context.Context) error
}

var once sync.Once

// registerProperties is called once and used to register new types
// for gob encoding/decoding.
func registerProperties() {
	gob.Register(&pb.Property_BooleanValue{})
	gob.Register(&pb.Property_IntegerValue{})
	gob.Register(&pb.Property_StringValue{})
	gob.Register(&timestamp.Timestamp{})
}

// FormatKey concatenates store and record keys separated by a backslash.
func FormatKey(storeKey, recordKey string) string {
	return fmt.Sprintf("%s/%s", storeKey, recordKey)
}

// EncodeRecord serializes a Open Saves pb Record with gob.
func EncodeRecord(r *pb.Record) ([]byte, error) {
	once.Do(registerProperties)
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	if err := e.Encode(r); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// EncodeRecord deserializes a Open Saves pb Record with gob.
func DecodeRecord(by []byte) (*pb.Record, error) {
	once.Do(registerProperties)
	r := &pb.Record{}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	if err := d.Decode(&r); err != nil {
		return nil, err
	}
	return r, nil
}
