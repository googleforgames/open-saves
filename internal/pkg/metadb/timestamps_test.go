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

package metadb_test

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	m "github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
)

func TestTimestamps_NewTimestamps(t *testing.T) {
	beforeNew := time.Now()
	var ts m.Timestamps
	ts.NewTimestamps()
	afterNew := time.Now()
	assert.NotNil(t, ts)

	// Check the uuid
	assert.NotEmpty(t, ts.Signature)

	assert.True(t, beforeNew.Before(ts.CreatedAt))
	assert.True(t, beforeNew.Before(ts.UpdatedAt))
	assert.True(t, afterNew.After(ts.CreatedAt))
	assert.True(t, afterNew.After(ts.UpdatedAt))
	assert.True(t, ts.CreatedAt.Equal(ts.UpdatedAt))
}

func TestTimestamps_Update(t *testing.T) {
	var ts m.Timestamps
	ts.NewTimestamps()
	ocreated := ts.CreatedAt
	oupdated := ts.UpdatedAt
	osignature := ts.Signature
	assert.NotNil(t, ts)
	beforeUpdate := time.Now()
	ts.UpdateTimestamps()
	afterUpdate := time.Now()

	assert.True(t, ocreated.Equal(ts.CreatedAt))
	assert.False(t, oupdated.Equal(ts.UpdatedAt))
	assert.True(t, beforeUpdate.Before(ts.UpdatedAt))
	assert.True(t, afterUpdate.After(ts.UpdatedAt))
	assert.NotEqual(t, osignature, ts.Signature)
}

func TestTimestamps_Save(t *testing.T) {
	var ts m.Timestamps
	ts.NewTimestamps()
	actual, err := ts.Save()
	assert.NoError(t, err)
	expected := []datastore.Property{
		{
			Name:  "CreatedAt",
			Value: ts.CreatedAt,
		},
		{
			Name:  "UpdatedAt",
			Value: ts.UpdatedAt,
		},
		{
			Name:    "Signature",
			Value:   ts.Signature.String(),
			NoIndex: true,
		},
	}
	assert.Equal(t, expected, actual)
}

func TestTimestamps_Load(t *testing.T) {
	createdAt := time.Now()
	updatedAt := createdAt.Add(time.Hour)
	uuid := uuid.New()
	properties := []datastore.Property{
		{
			Name:  "CreatedAt",
			Value: createdAt,
		},
		{
			Name:  "UpdatedAt",
			Value: updatedAt,
		},
		{
			Name:    "Signature",
			Value:   uuid.String(),
			NoIndex: true,
		},
	}
	var actual m.Timestamps
	assert.NoError(t, actual.Load(properties))
	expected := m.Timestamps{
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Signature: uuid,
	}
	assert.Equal(t, expected, actual)
}
