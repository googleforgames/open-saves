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

	m "github.com/googleforgames/open-saves/internal/pkg/metadb"
)

// Cache interface defines common operations for the cache store.
type Cache interface {
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	ListKeys(ctx context.Context) ([]string, error)
	FlushAll(ctx context.Context) error
}

// FormatKey concatenates store and record keys separated by a backslash.
func FormatKey(storeKey, recordKey string) string {
	return fmt.Sprintf("%s/%s", storeKey, recordKey)
}

// EncodeRecord serializes a Open Saves Record with gob.
func EncodeRecord(r *m.Record) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	if err := e.Encode(r); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// DecodeRecord deserializes a Open Saves Record with gob.
func DecodeRecord(by []byte) (*m.Record, error) {
	r := &m.Record{}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	if err := d.Decode(&r); err != nil {
		return nil, err
	}
	return r, nil
}
