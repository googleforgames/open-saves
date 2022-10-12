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

package blobref

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBlobRef_New(t *testing.T) {
	t.Parallel()

	want := &BlobRef{
		StoreKey:   "store",
		RecordKey:  "record",
		ChunkCount: 42,
		Status:     StatusInitializing,
		Chunked:    true,
	}
	got := NewChunkedBlobRef("store", "record", 42)
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(BlobRef{}, "Key", "Timestamps")); diff != "" {
		t.Errorf("NewChunkedBlobRef() = (-want, +got):\n%s", diff)
	}
	if got.Key == uuid.Nil {
		t.Errorf("NewChunkedBlobRef() should set Key, got = %v", got.Key.String())
	}
}

func TestBlobRef_LoadKey(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		key     string
		want    BlobRef
		wantErr bool
	}{
		{
			"valid",
			"d13c289c-8845-485f-b582-c87342d5dade",
			BlobRef{Key: uuid.MustParse("d13c289c-8845-485f-b582-c87342d5dade")},
			false,
		},
		{
			"invalid",
			"not valid",
			BlobRef{},
			true,
		},
	}
	for _, tc := range testCases {
		t.Run(t.Name(), func(t *testing.T) {
			got := BlobRef{}
			err := got.LoadKey(datastore.NameKey("blob", tc.key, nil))
			if tc.wantErr {
				if err == nil {
					t.Errorf("LoadKey() should return error, got = %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("LoadKey() should succeed, got = %v", err)
				}
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("LoadKey() = (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestBlobRef_Save(t *testing.T) {
	t.Parallel()

	blob := BlobRef{
		Size:       123,
		Status:     StatusInitializing,
		StoreKey:   "store",
		RecordKey:  "record",
		ChunkCount: 42,
		Checksums: checksums.Checksums{
			MD5:       []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e},
			CRC32C:    0x307ABEB0,
			HasCRC32C: true,
		},
		Timestamps: timestamps.Timestamps{
			CreatedAt: time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC),
			UpdatedAt: time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC),
			Signature: uuid.MustParse("397f94f5-f851-4969-8bd8-7828abc473a6"),
		},
	}

	want := []datastore.Property{
		{
			Name:  "Size",
			Value: int64(123),
		},
		{
			Name:  "Status",
			Value: int64(StatusInitializing),
		},
		{
			Name:  "StoreKey",
			Value: "store",
		},
		{
			Name:  "RecordKey",
			Value: "record",
		},
		{
			Name:  "Chunked",
			Value: false,
		},
		{
			Name:  "ChunkCount",
			Value: int64(42),
		},
		{
			Name:    "MD5",
			Value:   []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e},
			NoIndex: true,
		},
		{
			Name:    "CRC32C",
			Value:   int64(0x307ABEB0),
			NoIndex: true,
		},
		{
			Name:    "HasCRC32C",
			Value:   true,
			NoIndex: true,
		},
		{
			Name: "Timestamps",
			Value: &datastore.Entity{
				Properties: []datastore.Property{
					{
						Name:  "CreatedAt",
						Value: time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC),
					},
					{
						Name:  "UpdatedAt",
						Value: time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC),
					},
					{
						Name:    "Signature",
						Value:   "397f94f5-f851-4969-8bd8-7828abc473a6",
						NoIndex: true,
					},
				},
			},
			NoIndex: true,
		},
	}
	got, err := blob.Save()
	if err != nil {
		t.Errorf("Save() failed: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Save() = (-want, +got):\n%s", diff)
	}
}

func TestBlobRef_Load(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		ps   []datastore.Property
		want *BlobRef
	}{
		{
			name: "canonical",
			ps: []datastore.Property{
				{
					Name:  "Size",
					Value: int64(123),
				},
				{
					Name:  "Status",
					Value: int64(StatusReady),
				},
				{
					Name:  "StoreKey",
					Value: "store key",
				},
				{
					Name:  "RecordKey",
					Value: "record key",
				},
				{
					Name:  "Chunked",
					Value: true,
				},
				{
					Name:  "ChunkCount",
					Value: int64(551),
				},
				{
					Name:    "MD5",
					Value:   []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e},
					NoIndex: true,
				},
				{
					Name:    "CRC32C",
					Value:   int64(0x307ABEB0),
					NoIndex: true,
				},
				{
					Name:    "HasCRC32C",
					Value:   true,
					NoIndex: true,
				},
				{
					Name: "Timestamps",
					Value: &datastore.Entity{
						Properties: []datastore.Property{
							{
								Name:  "CreatedAt",
								Value: time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC),
							},
							{
								Name:  "UpdatedAt",
								Value: time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC),
							},
							{
								Name:    "Signature",
								Value:   "397f94f5-f851-4969-8bd8-7828abc473a6",
								NoIndex: true,
							},
						},
					},
				},
			},
			want: &BlobRef{
				Size:       123,
				Status:     StatusReady,
				StoreKey:   "store key",
				RecordKey:  "record key",
				Chunked:    true,
				ChunkCount: 551,
				Checksums: checksums.Checksums{
					MD5:       []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e},
					CRC32C:    0x307ABEB0,
					HasCRC32C: true,
				},
				Timestamps: timestamps.Timestamps{
					CreatedAt: time.Date(1992, 1, 15, 3, 15, 55, 0, time.UTC),
					UpdatedAt: time.Date(1992, 11, 27, 1, 3, 11, 0, time.UTC),
					Signature: uuid.MustParse("397f94f5-f851-4969-8bd8-7828abc473a6"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := &BlobRef{}
			if err := got.Load(tc.ps); err != nil {
				t.Errorf("Load() failed: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Load() = (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestBlobRef_GetObjectPath(t *testing.T) {
	t.Parallel()

	const want = "1ddbad65-d5f4-4d7f-9756-ce6a6339b6b8"
	blob := BlobRef{
		Key: uuid.MustParse(want),
	}
	if got := blob.ObjectPath(); got != want {
		t.Errorf("ObjectPath() = %v, want = %v", got, want)
	}
}

func TestBlobRef_ToProto(t *testing.T) {
	t.Parallel()

	blob := BlobRef{
		Size:       123,
		Status:     StatusReady,
		StoreKey:   "store key",
		RecordKey:  "record key",
		Chunked:    true,
		ChunkCount: 551,
		Checksums: checksums.Checksums{
			MD5:       []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e},
			CRC32C:    0x307ABEB0,
			HasCRC32C: true,
		},
	}
	want := &pb.BlobMetadata{
		StoreKey:   "store key",
		RecordKey:  "record key",
		Size:       123,
		Chunked:    true,
		ChunkCount: 551,
		Crc32C:     0x307ABEB0,
		HasCrc32C:  true,
		Md5:        []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e},
	}
	got := blob.ToProto()
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("ToProto() = (-want, +got):\n%s", diff)
	}
}
