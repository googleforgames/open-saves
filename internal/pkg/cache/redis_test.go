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
	"context"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	pb "github.com/googleforgames/triton/api"
	"github.com/stretchr/testify/assert"
)

func TestRedis_All(t *testing.T) {
	ctx := context.Background()

	// Use a local instance of Redis for tests. This
	// requires starting a redis server prior to test
	// invocation.
	r := NewRedis("localhost:6379")

	keys, err := r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Empty(t, keys)

	_, err = r.Get(ctx, "unknown")
	assert.Error(t, err)

	assert.NoError(t, r.Set(ctx, "hello", "triton"))

	val, err := r.Get(ctx, "hello")
	assert.NoError(t, err)
	assert.Equal(t, val, "triton")

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

	rr := []*pb.Record{
		{
			Key: "key1",
			CreatedAt: &timestamp.Timestamp{
				Seconds: 100,
			},
			UpdatedAt: &timestamp.Timestamp{
				Seconds: 110,
			},
		},
		{
			Key: "key2",
			Properties: map[string]*pb.Property{
				"prop1": {
					Type: pb.Property_BOOLEAN,
					Value: &pb.Property_BooleanValue{
						BooleanValue: false,
					},
				},
				"prop2": {
					Type: pb.Property_INTEGER,
					Value: &pb.Property_IntegerValue{
						IntegerValue: 200,
					},
				},
				"prop3": {
					Type: pb.Property_STRING,
					Value: &pb.Property_StringValue{
						StringValue: "triton",
					},
				},
			},
			CreatedAt: &timestamp.Timestamp{
				Seconds: 100,
			},
			UpdatedAt: &timestamp.Timestamp{
				Seconds: 110,
			},
		},
		{
			Key:      "key3",
			Blob:     []byte("some-bytes"),
			BlobSize: 64,
			OwnerId:  "new-owner",
			Tags:     []string{"tag1", "tag2"},
			CreatedAt: &timestamp.Timestamp{
				Seconds: 100,
			},
			UpdatedAt: &timestamp.Timestamp{
				Seconds: 110,
			},
		},
	}

	for _, r := range rr {
		e, err := EncodeRecord(r)
		assert.NoError(t, err)
		d, err := DecodeRecord(e)
		assert.NoError(t, err)
		assertEqualRecord(t, r, d)

		red.Set(ctx, r.Key, e)
		record, err := red.Get(ctx, r.Key)
		assert.Equal(t, e, record)
		decodedRecord, err := DecodeRecord(record)
		assertEqualRecord(t, r, decodedRecord)

		assert.NoError(t, red.FlushAll(ctx))
	}
}
