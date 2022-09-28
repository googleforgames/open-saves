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
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testUUIDString = "bd838bb3-27aa-4b8a-9b6d-14dc87e1a22b"
)

func TestUUID_UUIDToDatastoreProperty(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		uuid uuid.UUID
		want string
	}{
		{"Nil", uuid.Nil, ""},
		{"Not Nil", uuid.MustParse(testUUIDString), testUUIDString},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			want := datastore.Property{
				Name:    tc.name,
				Value:   tc.want,
				NoIndex: true,
			}
			got := UUIDToDatastoreProperty(tc.name, tc.uuid, true)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("UUIDToDatastoreProperty() = (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestUUID_LoadUUID(t *testing.T) {
	t.Parallel()

	testUUID := uuid.MustParse(testUUIDString)
	testCases := []struct {
		name      string
		props     []datastore.Property
		wantCode  codes.Code
		wantProps []datastore.Property
		wantUUID  uuid.UUID
	}{
		{"empty array", []datastore.Property{}, codes.Internal, []datastore.Property{}, uuid.Nil},
		{
			"empty string",
			[]datastore.Property{
				{
					Name:  "empty string",
					Value: "",
				},
			},
			codes.OK,
			[]datastore.Property{},
			uuid.Nil,
		},
		{
			"malformed string",
			[]datastore.Property{
				{
					Name:  "malformed string",
					Value: "this is not UUID",
				},
			},
			codes.Unknown, // TODO(yuryu): Unknown is probably not appropriate
			[]datastore.Property{},
			uuid.Nil,
		},
		{
			"valid",
			[]datastore.Property{
				{
					Name:    "valid",
					Value:   testUUIDString,
					NoIndex: true,
				},
			},
			codes.OK,
			[]datastore.Property{},
			testUUID,
		},
		{
			"valid with extra properties",
			[]datastore.Property{
				{
					Name:    "valid with extra properties",
					Value:   testUUIDString,
					NoIndex: true,
				},
				{
					Name: "unrelated",
				},
			},
			codes.OK,
			[]datastore.Property{
				{
					Name: "unrelated",
				},
			},
			testUUID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uuid, ps, err := LoadUUID(tc.props, tc.name)
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("LoadUUID() status code got = %v, want %v", got, tc.wantCode)
			}
			if !cmp.Equal(tc.wantUUID, uuid) {
				t.Errorf("LoadUUID() uuid = got %v, want%v", uuid, tc.wantUUID)
			}
			if diff := cmp.Diff(tc.wantProps, ps); diff != "" {
				t.Errorf("LoadUUID() props = (-want, +got):\n%s", diff)
			}
		})
	}
}
