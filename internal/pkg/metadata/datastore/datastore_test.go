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

package datastore

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	triton "github.com/googleforgames/triton/api"
	"github.com/googleforgames/triton/internal/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func newDriver(ctx context.Context, t *testing.T) *Driver {
	options := []option.ClientOption{}
	if cred := os.Getenv("TRITON_GCP_CREDENTIALS"); cred != "" {
		options = append(options, option.WithCredentialsFile(cred))
	}
	driver, err := NewDriver(ctx, "triton-for-games-dev", options...)
	if err != nil {
		t.Fatalf("Initializing Datastore driver: %v", err)
	}
	driver.Namespace = "datastore-unittests"
	if err := driver.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to Datastore: %v", err)
	}
	return driver
}

func TestDriver_ConnectDisconnect(t *testing.T) {
	cred := os.Getenv("TRITON_GCP_CREDENTIALS")
	t.Logf("Using credentials at: %s", cred)
	ctx := context.Background()
	// Connect() is tested inside newDriver().
	driver := newDriver(ctx, t)
	if err := driver.Disconnect(ctx); err != nil {
		t.Fatalf("Failed to disconnect: %v", err)
	}
}

func TestDriver_SimpleCreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := "unittest_store" + uuid.New().String()
	storeName := "SimpleCreateGetDeleteStore" + uuid.New().String()
	store := &metadata.Store{
		Key:     storeKey,
		Name:    storeName,
		OwnerID: "triton",
		Tags:    []string{"abc", "def"},
	}
	if err := driver.CreateStore(ctx, store); err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}

	store2, err := driver.GetStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Could not get store (%s): %v", storeKey, err)
	}
	assert.Equal(t, store, store2, "GetStore should return the exact same store.")

	store3, err := driver.FindStoreByName(ctx, storeName)
	if err != nil {
		t.Fatalf("Could not fetch store by name (%s): %v", storeName, err)
	}
	assert.Equal(t, store, store3, "FindStoreByName should return the exact same store.")

	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
	_, err = driver.GetStore(ctx, storeKey)
	if err == nil {
		t.Fatalf("GetStore didn't return an error after deleting a store: %v", err)
	}
}

func TestDriver_SimpleCreateGetDeleteRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := "unittest_store" + uuid.New().String()
	store := &metadata.Store{
		Key:     storeKey,
		Name:    "SimpleCreateGetDeleteRecord",
		OwnerID: "triton",
	}
	if err := driver.CreateStore(ctx, store); err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	recordKey := "unittest_record" + uuid.New().String()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	record := &metadata.Record{
		Key:      recordKey,
		Blob:     blob,
		BlobSize: int64(len(blob)),
		OwnerID:  "Triton",
		Tags:     []string{"abc", "def"},
		Properties: map[string]metadata.Property{
			"BoolTP":   {Type: triton.Property_BOOLEAN, BooleanValue: false},
			"IntTP":    {Type: triton.Property_INTEGER, IntegerValue: 42},
			"StringTP": {Type: triton.Property_STRING, StringValue: "a string value"},
		},
	}
	if err := driver.InsertRecord(ctx, storeKey, record); err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	assert.Equal(t, record, record2, "GetRecord should return the exact same record.")

	err = driver.InsertRecord(ctx, storeKey, record)
	if err == nil {
		t.Fatal("Insert should fail if a record with the same already exists.")
	}

	err = driver.DeleteRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to delete a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	_, err = driver.GetRecord(ctx, storeKey, recordKey)
	if err == nil {
		t.Fatalf("GetRecord didn't return an error after deleting a record (%s)", recordKey)
	}

	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
}

func TestDriver_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := "unittest_store" + uuid.New().String()
	store := &metadata.Store{
		Key:     storeKey,
		Name:    "UpdateRecord",
		OwnerID: "triton",
	}
	if err := driver.CreateStore(ctx, store); err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	recordKey := "unittest_record" + uuid.New().String()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	record := &metadata.Record{
		Key:      recordKey,
		Blob:     blob,
		BlobSize: int64(len(blob)),
		OwnerID:  "Triton",
		Tags:     []string{"abc", "def"},
	}

	if err := driver.UpdateRecord(ctx, storeKey, record); err == nil {
		t.Error("UpdateRecord should return an error if the specified record doesn't exist.")
	}

	if err := driver.InsertRecord(ctx, storeKey, record); err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	record.Tags = append(record.Tags, "ghi")
	record.OwnerID = "NewOwner"
	if err := driver.UpdateRecord(ctx, storeKey, record); err != nil {
		t.Fatalf("Failed to update a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	assert.Equal(t, record, record2, "GetRecord should fetch the updated record.")

	err = driver.DeleteRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to delete a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
}

func TestDriver_DeleteShouldNotFailWithNonExistentKey(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := "unittest_store" + uuid.New().String()
	if err := driver.DeleteStore(ctx, storeKey); err != nil {
		t.Fatalf("DeleteStore failed with a non-existent key: %v", err)
	}
	recordKey := "unittest_record" + uuid.New().String()
	if err := driver.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Fatalf("DeleteRecord failed with a non-existent key: %v", err)
	}
}
