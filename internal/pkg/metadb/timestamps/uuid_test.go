// Copyright 2021 Google LLC
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

package timestamps

import (
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testName       = "test uuid name"
	testNoIndex    = true
	testUUIDString = "bd838bb3-27aa-4b8a-9b6d-14dc87e1a22b"
)

func TestUUID_UUIDToDatastoreProperty(t *testing.T) {
	assert.Equal(t,
		datastore.Property{
			Name:    testName,
			Value:   "",
			NoIndex: testNoIndex,
		},
		UUIDToDatastoreProperty(testName, uuid.Nil, testNoIndex),
		"UUIDToDatastoreProperty should return an empty string for a nil UUID.",
	)

	testUUID := uuid.MustParse(testUUIDString)
	actual := UUIDToDatastoreProperty(testName, testUUID, testNoIndex)
	assert.Equal(t, testUUIDString, actual.Value,
		"UUIDToDatastoreProperty should return the input UUID in string.")
}

func TestUUID_LoadUUIDEmptyString(t *testing.T) {
	// Returns codes.Internal for empty array
	u, ps, err := LoadUUID(make([]datastore.Property, 0), testName)
	assert.Equal(t, uuid.Nil, u, "LoadUUID should return uuid.Nil in case of errors.")
	assert.Empty(t, ps, "LoadUUID should return the input Property array.")
	assert.Equal(t, codes.Internal, status.Code(err),
		"LoadUUID should return codes.Internal in case of errors.")
}

func TestUUID_LoadUUID(t *testing.T) {
	// Returns uuid.Nil for empty string
	u, ps, err := LoadUUID(
		[]datastore.Property{
			{
				Name:  testName,
				Value: "",
			},
		}, testName,
	)
	assert.Equal(t, uuid.Nil, u, "LoadUUID should return uuid.Nil for an empty string.")
	assert.Empty(t, ps, "LoadUUID should remove the UUID Property from the input array.")
	assert.NoError(t, err, "LoadUUID should not return error for an empty string.")

	// Returns the parsed UUID
	anotherProperty := datastore.Property{Name: "another property", Value: "another value"}
	u, ps, err = LoadUUID(
		[]datastore.Property{
			anotherProperty,
			{
				Name:  testName,
				Value: testUUIDString,
			},
		}, testName,
	)
	assert.Equal(t, uuid.MustParse(testUUIDString), u,
		"LoadUUID should return a parsed UUID of the input string.")
	if assert.Len(t, ps, 1, "LoadUUID should return the remaining properties.") {
		assert.Equal(t, anotherProperty, ps[0], "LoadUUID should return the remaining properties.")
	}
	assert.NoError(t, err, "LoadUUID should not return error when successful.")
}

func TestUUID_LoadUUIDMalformedString(t *testing.T) {
	u, ps, err := LoadUUID(
		[]datastore.Property{
			{
				Name:  testName,
				Value: "not a UUID",
			},
		}, testName,
	)
	assert.Equal(t, uuid.Nil, u, "LoadUUID should return uuid.Nil in case of parse errors.")
	assert.Empty(t, ps, "LoadUUID should still remove the UUID property in case of parse errors.")
	assert.Error(t, err, "LoadUUID should return a non-nil err in case of parse errors.")
}
