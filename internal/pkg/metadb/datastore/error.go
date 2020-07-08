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

package datastore

import (
	ds "cloud.google.com/go/datastore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func datastoreErrToGRPCStatus(err error) (grpcErr error) {
	switch err {
	case nil:
		grpcErr = nil
	case ds.ErrNoSuchEntity:
		grpcErr = status.Error(codes.NotFound, err.Error())
	case ds.ErrConcurrentTransaction:
		grpcErr = status.Error(codes.Aborted, err.Error())
	case ds.ErrInvalidEntityType:
		// Datastore returns ErrInvalidEntity when Get is called with wrong structs.
		// All structs passed to the Datastore library are managed by the server code
		// and this should be treated as an internal error.
		grpcErr = status.Error(codes.Internal, err.Error())
	case ds.ErrInvalidKey:
		grpcErr = status.Error(codes.InvalidArgument, err.Error())
	default:
		grpcErr = status.Convert(err).Err()
	}
	return
}
