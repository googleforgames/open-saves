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

package chunkref

import (
	ds "cloud.google.com/go/datastore"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
)

var (
	ErrIteratorNil = blobref.ErrIteratorNil
)

// ChunkRefCursor is a database cursor for ChunkRef.
type ChunkRefCursor struct {
	iter *ds.Iterator
}

func NewCursor(i *ds.Iterator) *ChunkRefCursor {
	return &ChunkRefCursor{iter: i}
}

// Next advances the iterator and returns the next value.
// Returns nil and and iterator.Done at the end of the iterator.
// Returns ErrIteratorNil if the iterator is nil.
func (i *ChunkRefCursor) Next() (*ChunkRef, error) {
	if i == nil {
		return nil, ErrIteratorNil
	}
	if i.iter == nil {
		return nil, ErrIteratorNil
	}
	var chunk ChunkRef
	_, err := i.iter.Next(&chunk)
	if err != nil {
		return nil, err
	}
	return &chunk, nil
}
