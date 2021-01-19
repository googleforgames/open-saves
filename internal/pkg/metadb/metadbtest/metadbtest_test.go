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
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
)

func TestMetaDBTest_TestAssertEqualStore(t *testing.T) {
	AssertEqualStore(t, nil, nil)
	AssertEqualStoreWithinDuration(t, nil, nil, time.Duration(100))
	store := &metadb.Store{
		Key:     "abc",
		OwnerID: "owner",
		Tags:    []string{"tag1", "tag2"},
		Timestamps: metadb.Timestamps{
			CreatedAt: time.Unix(100, 0),
			UpdatedAt: time.Unix(110, 0),
			Signature: uuid.MustParse("BF6B705A-D14E-414D-A40F-58F6311119B9"),
		},
	}
	AssertEqualStore(t, store, store)
	store2 := new(metadb.Store)
	*store2 = *store
	store2.Tags = []string{"tag2", "tag1"}
	store2.Timestamps.CreatedAt = store2.Timestamps.CreatedAt.Add(time.Duration(42))
	store2.Timestamps.UpdatedAt = store2.Timestamps.UpdatedAt.Add(time.Duration(42))
	AssertEqualStoreWithinDuration(t, store, store2, time.Duration(42))
}

func TestMetaDBTest_TestAssertEqualRecord(t *testing.T) {
	AssertEqualRecord(t, nil, nil)
	AssertEqualRecordWithinDuration(t, nil, nil, time.Duration(100))
	record := &metadb.Record{
		Key:      "abc",
		Blob:     []byte{1, 2, 3},
		BlobSize: 3,
		OwnerID:  "owner",
		Tags:     []string{"tag1", "tag2"},
		Properties: metadb.PropertyMap{
			"prop1": {
				Type:         pb.Property_INTEGER,
				IntegerValue: 42,
			},
			"prop2": {
				Type:        pb.Property_STRING,
				StringValue: "string value",
			},
		},
		Timestamps: metadb.Timestamps{
			CreatedAt: time.Unix(100, 0),
			UpdatedAt: time.Unix(110, 0),
			Signature: uuid.MustParse("BF6B705A-D14E-414D-A40F-58F6311119B9"),
		},
	}
	AssertEqualRecord(t, record, record)
	record2 := new(metadb.Record)
	*record2 = *record
	record2.Tags = []string{"tag2", "tag1"}
	record2.Timestamps.CreatedAt = record2.Timestamps.CreatedAt.Add(time.Duration(42))
	record2.Timestamps.UpdatedAt = record2.Timestamps.UpdatedAt.Add(time.Duration(42))
	AssertEqualRecordWithinDuration(t, record, record2, time.Duration(42))
}

func TestMetaDBTest_TestAssertEqualBlobRef(t *testing.T) {
	blob := &metadb.BlobRef{
		Key:        uuid.MustParse("35B7DABC-9523-45E1-995A-D76F3EF29F79"),
		Size:       12345,
		ObjectName: "test",
		Status:     metadb.BlobRefStatusInitializing,
		StoreKey:   "storeKey",
		RecordKey:  "recordKey",
		Timestamps: metadb.Timestamps{
			CreatedAt: time.Unix(100, 0),
			UpdatedAt: time.Unix(200, 0),
			Signature: uuid.MustParse("D4F08F0D-0FD1-49E4-9681-A0569A608FC9"),
		},
	}
	AssertEqualBlobRef(t, blob, blob)
	blob2 := new(metadb.BlobRef)
	*blob2 = *blob
	blob2.Timestamps.CreatedAt = blob.Timestamps.CreatedAt.Add(time.Duration(42))
	blob2.Timestamps.UpdatedAt = blob.Timestamps.UpdatedAt.Add(time.Duration(42))
	AssertEqualBlobRefWithinDuration(t, blob, blob2, time.Duration(42))
}
