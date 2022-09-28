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

package blobref

import (
	"testing"
)

func TestStatus_Ready(t *testing.T) {
	testCases := []struct {
		name    string
		start   Status
		want    Status
		wantErr error
	}{
		{
			name:    "StatusInitializing",
			start:   StatusInitializing,
			want:    StatusReady,
			wantErr: nil,
		},
		{
			name:    "StatusPendingDeletion",
			start:   StatusPendingDeletion,
			want:    StatusPendingDeletion,
			wantErr: ErrReadyNotInitializing,
		},
		{
			name:    "StatusReady",
			start:   StatusReady,
			want:    StatusReady,
			wantErr: ErrReadyNotInitializing,
		},
		{
			name:    "StatusError",
			start:   StatusError,
			want:    StatusError,
			wantErr: ErrReadyNotInitializing,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.start
			if err := got.Ready(); err != tc.wantErr {
				t.Errorf("Ready() returned %v, want = %v", got, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Ready() changed status to %v, want = %v", got, tc.want)
			}
		})
	}
}

func TestStatus_MarkForDeletion(t *testing.T) {
	testCases := []struct {
		name    string
		start   Status
		want    Status
		wantErr error
	}{
		{
			name:    "StatusInitializing",
			start:   StatusInitializing,
			want:    StatusPendingDeletion,
			wantErr: nil,
		},
		{
			name:    "StatusPendingDeletion",
			start:   StatusPendingDeletion,
			want:    StatusPendingDeletion,
			wantErr: ErrMarkForDeletionNotReady,
		},
		{
			name:    "StatusReady",
			start:   StatusReady,
			want:    StatusPendingDeletion,
			wantErr: nil,
		},
		{
			name:    "StatusError",
			start:   StatusError,
			want:    StatusError,
			wantErr: ErrMarkForDeletionNotReady,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.start
			if err := got.MarkForDeletion(); err != tc.wantErr {
				t.Errorf("MarkForDeletion() returned %v, want = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("MarkForDeletion() changed status to %v, want = %v", got, tc.want)
			}
		})
	}
}

func TestStatus_Fail(t *testing.T) {
	testCases := []struct {
		name string
		s    Status
	}{
		{"StatusUnknown", StatusUnknown}, // Fail should work for BlobStatusUnknown too.
		{"StatusInitializing", StatusInitializing},
		{"StatusPendingDeletion", StatusPendingDeletion},
		{"StatusReady", StatusReady},
		{"StatusError", StatusError},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.s
			got.Fail()
			if got != StatusError {
				t.Errorf("Fail() should set StatusError, got = %v", got)
			}
		})
	}
}
