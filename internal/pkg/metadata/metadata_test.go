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

package metadata

import (
	"context"
	"testing"
)

// Driver mock for unit tests.
type driverMock struct {
}

func (_ *driverMock) Connect(ctx context.Context) error {
	return nil
}

func (_ *driverMock) Disconnect(ctx context.Context) error {
	return nil
}

func (_ *driverMock) CreateStore(ctx context.Context, store *Store) error {
	return nil
}

func (_ *driverMock) GetStore(ctx context.Context, key string) (*Store, error) {
	return nil, nil
}

func (_ *driverMock) FindStoreByName(ctx context.Context, name string) (*Store, error) {
	return nil, nil
}

func (_ *driverMock) DeleteStore(ctx context.Context, key string) error {
	return nil
}

func (_ *driverMock) InsertRecord(ctx context.Context, storeKey string, record *Record) error {
	return nil
}

func (_ *driverMock) UpdateRecord(ctx context.Context, storeKey string, record *Record) error {
	return nil
}

func (_ *driverMock) GetRecord(ctx context.Context, storeKey, key string) (*Record, error) {
	return nil, nil
}

func (_ *driverMock) DeleteRecord(ctx context.Context, storeKey, key string) error {
	return nil
}

func TestMetadata_NewMetaDB(t *testing.T) {
	driver := &driverMock{}
	metadb := NewMetaDB(driver)
	if metadb == nil {
		t.Fatal("NewMetaDB() should return a non-nil instance.")
	}
	if metadb.driver != driver {
		t.Fatalf("NewMetaDB should assign the driver variable: actual(%p)", metadb.driver)
	}
}
