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

package redis

import (
	"context"
	"testing"
	"time"

	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/metadbtest"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
)

func TestRedis_All(t *testing.T) {
	ctx := context.Background()

	// Use a local instance of Redis for tests. This
	// requires starting a redis server prior to test
	// invocation.
	r := NewRedis("localhost:6379")

	assert.NoError(t, r.FlushAll(ctx))

	keys, err := r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Empty(t, keys)

	_, err = r.Get(ctx, "unknown")
	assert.Error(t, err)

	by := []byte("byte")
	assert.NoError(t, r.Set(ctx, "hello", by))

	val, err := r.Get(ctx, "hello")
	assert.NoError(t, err)
	assert.Equal(t, val, by)

	keys, err = r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Equal(t, keys, []string{"hello"})

	assert.NoError(t, r.Delete(ctx, "hello"))

	assert.NoError(t, r.FlushAll(ctx))

	keys, err = r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Empty(t, keys)
}

func TestRedis_SerializeRecord(t *testing.T) {
	ctx := context.Background()

	red := NewRedis("localhost:6379")
	t.Cleanup(func() { assert.NoError(t, red.FlushAll(ctx)) })

	rr := []*record.Record{
		{
			Key: "key1",
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Unix(100, 0),
				UpdatedAt: time.Unix(110, 0),
			},
		},
		{
			Key: "key2",
			Properties: record.PropertyMap{
				"prop1": {
					Type:         pb.Property_BOOLEAN,
					BooleanValue: false,
				},
				"prop2": {
					Type:         pb.Property_INTEGER,
					IntegerValue: 200,
				},
				"prop3": {
					Type:        pb.Property_STRING,
					StringValue: "string value",
				},
			},
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Unix(100, 0),
				UpdatedAt: time.Unix(110, 0),
			},
		},
		{
			Key:      "key3",
			Blob:     []byte("some-bytes"),
			BlobSize: 64,
			OwnerID:  "new-owner",
			Tags:     []string{"tag1", "tag2"},
			Timestamps: timestamps.Timestamps{
				CreatedAt: time.Unix(100, 0),
				UpdatedAt: time.Unix(110, 0),
			},
		},
	}

	for _, r := range rr {
		encoded, err := r.EncodeBytes()
		if err != nil {
			t.Fatalf("EncodedBytes failed: %v", err)
		}

		if err := red.Set(ctx, r.Key, encoded); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		cached, err := red.Get(ctx, r.Key)
		if assert.NoError(t, err) {
			assert.Equal(t, encoded, cached)

			decoded := new(record.Record)
			err := decoded.DecodeBytes(cached)
			if assert.NoError(t, err) {
				metadbtest.AssertEqualRecord(t, r, decoded)
			}
		}
	}
}
