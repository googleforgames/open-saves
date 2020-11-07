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
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/stretchr/testify/assert"
)

const (
	// The threshold of comparing times.
	// Since the server and client run on the same host for these tests,
	// 1 second should be enough.
	timestampDelta = 1 * time.Second
)

func TestCache_SerializeRecord(t *testing.T) {
	rr := []*pb.Record{
		{
			CreatedAt: &timestamp.Timestamp{
				Seconds: 100,
			},
			UpdatedAt: &timestamp.Timestamp{
				Seconds: 110,
			},
		},
		{
			Key: "some-key",
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
						StringValue: "string value",
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
			Key:      "some-key",
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
	}
}

func assertEqualRecord(t *testing.T, expected, actual *pb.Record) {
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.Blob, actual.Blob)
		assert.Equal(t, expected.BlobSize, actual.BlobSize)
		assert.Equal(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.OwnerId, actual.OwnerId)
		assert.Equal(t, len(expected.Properties), len(actual.Properties))
		for k, v := range expected.Properties {
			if assert.Contains(t, actual.Properties, k) {
				av := actual.Properties[k]
				assert.Equal(t, v.Type, av.Type)
				assert.Equal(t, v.Value, av.Value)
			}
		}
		assert.NotNil(t, actual.GetCreatedAt())
		assert.WithinDuration(t, expected.GetUpdatedAt().AsTime(),
			actual.GetUpdatedAt().AsTime(), timestampDelta)
		assert.NotNil(t, actual.GetUpdatedAt())
		assert.WithinDuration(t, expected.GetUpdatedAt().AsTime(),
			actual.GetUpdatedAt().AsTime(), timestampDelta)
	}
}
