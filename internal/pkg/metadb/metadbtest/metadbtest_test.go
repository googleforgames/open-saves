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

package metadbtest

import (
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
)

func TestMetaDBTest_TestAssertEqualStore(t *testing.T) {
	AssertEqualStore(t, nil, nil)
	AssertEqualStoreWithinDuration(t, nil, nil, time.Duration(100))
	original := &store.Store{
		Key:     "abc",
		OwnerID: "owner",
		Tags:    []string{"tag1", "tag2"},
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Unix(100, 0),
			UpdatedAt: time.Unix(110, 0),
			Signature: uuid.MustParse("BF6B705A-D14E-414D-A40F-58F6311119B9"),
		},
	}
	AssertEqualStore(t, original, original)
	copied := new(store.Store)
	*copied = *original
	copied.Tags = []string{"tag2", "tag1"}
	copied.Timestamps.CreatedAt = copied.Timestamps.CreatedAt.Add(time.Duration(42))
	copied.Timestamps.UpdatedAt = copied.Timestamps.UpdatedAt.Add(time.Duration(42))
	AssertEqualStoreWithinDuration(t, original, copied, time.Duration(42))
}

func TestMetaDBTest_TestAssertEqualRecord(t *testing.T) {
	AssertEqualRecord(t, nil, nil)
	AssertEqualRecordWithinDuration(t, nil, nil, time.Duration(100))
	original := &record.Record{
		Key:      "abc",
		Blob:     []byte{1, 2, 3},
		BlobSize: 3,
		OwnerID:  "owner",
		Tags:     []string{"tag1", "tag2"},
		Properties: record.PropertyMap{
			"prop1": {
				Type:         pb.Property_INTEGER,
				IntegerValue: 42,
			},
			"prop2": {
				Type:        pb.Property_STRING,
				StringValue: "string value",
			},
		},
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Unix(100, 0),
			UpdatedAt: time.Unix(110, 0),
			Signature: uuid.MustParse("BF6B705A-D14E-414D-A40F-58F6311119B9"),
		},
	}
	AssertEqualRecord(t, original, original)
	copied := new(record.Record)
	*copied = *original
	copied.Tags = []string{"tag2", "tag1"}
	copied.Timestamps.CreatedAt = copied.Timestamps.CreatedAt.Add(time.Duration(42))
	copied.Timestamps.UpdatedAt = copied.Timestamps.UpdatedAt.Add(time.Duration(42))
	AssertEqualRecordWithinDuration(t, original, copied, time.Duration(42))
}

func TestMetaDBTest_TestAssertEqualBlobRef(t *testing.T) {
	original := &blobref.BlobRef{
		Key:       uuid.MustParse("35B7DABC-9523-45E1-995A-D76F3EF29F79"),
		Size:      12345,
		Status:    blobref.StatusInitializing,
		StoreKey:  "storeKey",
		RecordKey: "recordKey",
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Unix(100, 0),
			UpdatedAt: time.Unix(200, 0),
			Signature: uuid.MustParse("D4F08F0D-0FD1-49E4-9681-A0569A608FC9"),
		},
	}
	AssertEqualBlobRef(t, original, original)
	copied := new(blobref.BlobRef)
	*copied = *original
	copied.Timestamps.CreatedAt = original.Timestamps.CreatedAt.Add(time.Duration(42))
	copied.Timestamps.UpdatedAt = original.Timestamps.UpdatedAt.Add(time.Duration(42))
	AssertEqualBlobRefWithinDuration(t, original, copied, time.Duration(42))
}
