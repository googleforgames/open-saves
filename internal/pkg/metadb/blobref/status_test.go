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

	"github.com/stretchr/testify/assert"
)

func TestStatus_LifeCycle(t *testing.T) {
	status := StatusInitializing

	// Mark for deletion
	assert.NoError(t, status.MarkForDeletion())
	assert.Equal(t, StatusPendingDeletion, status)

	// Start over
	status = StatusInitializing

	// Ready
	assert.NoError(t, status.Ready())
	assert.Equal(t, StatusReady, status)

	// Invalid transitions
	assert.Error(t, status.Ready())

	// Mark for deletion
	assert.NoError(t, status.MarkForDeletion())
	assert.Equal(t, StatusPendingDeletion, status)

	// Invalid transitions
	assert.Error(t, status.MarkForDeletion())
	assert.Error(t, status.Ready())
}

func TestStatus_Fail(t *testing.T) {
	status := StatusUnknown

	// Fail should work for BlobStatusUnknown too.
	status.Fail()
	assert.Equal(t, StatusError, status)

	status = StatusInitializing
	status.Fail()
	assert.Equal(t, StatusError, status)

	status = StatusInitializing
	status.Fail()
	assert.Equal(t, StatusError, status)

	status = StatusPendingDeletion
	status.Fail()
	assert.Equal(t, StatusError, status)

	status = StatusReady
	status.Fail()
	assert.Equal(t, StatusError, status)

	status = StatusError
	status.Fail()
	assert.Equal(t, StatusError, status)
}
