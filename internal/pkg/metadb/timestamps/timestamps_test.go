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

package timestamps

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTimestamps_NewTimestamps(t *testing.T) {
	t.Parallel()

	beforeNew := time.Now().Truncate(Precision)
	ts := New()
	afterNew := time.Now()

	// Check the uuid
	if ts.Signature == uuid.Nil {
		t.Errorf("New(): uuid should not be Nil, got = %v", ts.Signature)
	}

	for _, got := range []time.Time{ts.CreatedAt, ts.UpdatedAt} {
		if got.Location() != time.UTC {
			t.Errorf("New() Location = got %v, want UTC", got.Location())
		}
		if !got.Equal(got.Truncate(Precision)) {
			t.Errorf("New() = %v, should be truncated to %v", got, Precision)
		}
		if got.Before(beforeNew) || got.After(afterNew) {
			t.Errorf("New() = %v,  want between %v and %v", got, beforeNew, afterNew)
		}
	}
	if diff := cmp.Diff(ts.CreatedAt, ts.UpdatedAt); diff != "" {
		t.Errorf("New(): CreatedAt and UpdatedAt should be equal: (-CreatedAt, +UpdatedAt):\n%s", diff)
	}
}

func TestTimestamps_TimeToProto(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		time time.Time
		want *timestamppb.Timestamp
	}{
		{
			"Zero",
			time.Time{},
			nil},
		{
			"Non Zero",
			time.Unix(423748110, 1).UTC(),
			&timestamppb.Timestamp{
				Seconds: 423748110,
				Nanos:   1,
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := TimeToProto(tc.time)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("TimeToProto() = (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestTimestamps_Update(t *testing.T) {
	testTime := time.Unix(123456, int64(42*Precision))
	testUUID := uuid.MustParse("db94be80-e036-4ca8-a9c0-2259b8a67acc")

	ts := Timestamps{
		CreatedAt: testTime,
		UpdatedAt: testTime,
		Signature: testUUID,
	}
	beforeUpdate := time.Now().Truncate(Precision)
	ts.Update()
	afterUpdate := time.Now()

	if diff := cmp.Diff(testTime, ts.CreatedAt); diff != "" {
		t.Errorf("Update() should not change CreatedAt, (-want, +got):\n%s", diff)
	}
	if got := ts.UpdatedAt.Location(); got != time.UTC {
		t.Errorf("Update() Location = got %v, want UTC", got)
	}
	if ts.UpdatedAt.Before(beforeUpdate) || ts.UpdatedAt.After(afterUpdate) {
		t.Errorf("Update() Updated At = %v, want between %v and %v", ts.UpdatedAt, beforeUpdate, afterUpdate)
	}
	if cmp.Equal(testUUID, ts.Signature) {
		t.Errorf("Update() Signature got = %v, want updated value", ts.Signature)
	}
}

func TestTimestamps_Save(t *testing.T) {
	ts := Timestamps{
		CreatedAt: time.Unix(123456, int64(42*Precision)),
		UpdatedAt: time.Unix(1234567, int64(24*Precision)),
		Signature: uuid.MustParse("db94be80-e036-4ca8-a9c0-2259b8a67acc"),
	}
	want := []datastore.Property{
		{
			Name:    "CreatedAt",
			Value:   ts.CreatedAt,
			NoIndex: true,
		},
		{
			Name:    "UpdatedAt",
			Value:   ts.UpdatedAt,
			NoIndex: true,
		},
		{
			Name:    "Signature",
			Value:   ts.Signature.String(),
			NoIndex: true,
		},
	}
	got, err := ts.Save()
	if err != nil {
		t.Errorf("Save() failed: %v", err)
	}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(func(x, y datastore.Property) bool { return x.Name < y.Name })); diff != "" {
		t.Errorf("Save() = (-want, +got):\n%s", diff)
	}
}

func TestTimestamps_Load(t *testing.T) {
	createdAt := time.Unix(123456, int64(42*Precision))
	updatedAt := time.Unix(1234567, int64(24*Precision))
	testUUID := uuid.MustParse("db94be80-e036-4ca8-a9c0-2259b8a67acc")

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
			Value:   testUUID.String(),
			NoIndex: true,
		},
	}
	var got Timestamps
	if err := got.Load(properties); err != nil {
		t.Errorf("Load() failed: %v", err)
	}
	want := Timestamps{
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Signature: testUUID,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Load() = (-want, +got):\n%s", diff)
	}
}
