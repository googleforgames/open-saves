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

import "errors"

// Status represents a current blob status.
//
// Life of a Blob
//
// [New Record created] --> [new Status entity with StatusInitializing]
//                         /               \
//                        / fail            \  success
//                       v                   v
//                [StatusError]            [StatusReady]
//                       |       x            |
//      Upload new blob  |        \ fail      | Record deleted or new blob uploaded
//            or         |         \          v
//     delete the record |          -------[StatusPendingDeletion]
//                       v                  /
//  [Delete the blob entity] <-------------/   Garbage collection
type Status int16

const (
	// StatusUnknown represents internal error.
	StatusUnknown Status = iota
	// StatusInitializing means the blob is currently being prepared, i.e.
	// being uploaded to the blob store.
	StatusInitializing
	// StatusReady means the blob is committed and ready for use.
	StatusReady
	// StatusPendingDeletion means the blob is no longer referenced by
	// any Record entities and needs to be deleted.
	StatusPendingDeletion
	// StatusError means the blob was not uploaded due to client or server
	// errors and the corresponding record needs to be updated (either by
	// retrying blob upload or deleting the entry).
	StatusError
)

var (
	ErrReadyNotInitializing    = errors.New("Ready was called when Status is not Initializing")
	ErrMarkForDeletionNotReady = errors.New("MarkForDeletion was called when Status is not either Initializing or Ready")
)

// Ready changes Status to StatusReady.
// It returns an error if the current Status is not StatusInitializing.
func (s *Status) Ready() error {
	if *s != StatusInitializing {
		return ErrReadyNotInitializing
	}
	*s = StatusReady
	return nil
}

// MarkForDeletion marks the Status as StatusPendingDeletion.
// Returns an error if the current Status is not StatusReady.
func (s *Status) MarkForDeletion() error {
	if *s != StatusInitializing && *s != StatusReady {
		return ErrMarkForDeletionNotReady
	}
	*s = StatusPendingDeletion
	return nil
}

// Fail marks the Status as StatusError.
// Any state can transition to StatusError.
func (s *Status) Fail() {
	*s = StatusError
}
