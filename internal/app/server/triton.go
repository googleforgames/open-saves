// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	tritonpb "github.com/googleforgames/triton/api"
	"github.com/googleforgames/triton/internal/pkg/blob"
	"github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/googleforgames/triton/internal/pkg/metadb/datastore"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tritonServer struct {
	cloud     string
	blobStore blob.BlobStore
	metaDB    *metadb.MetaDB
}

// newTritonServer creates a new instance of the triton server.
func newTritonServer(ctx context.Context, cloud string, project string, bucket string) (tritonpb.TritonServer, error) {
	switch cloud {
	case "gcp":
		log.Infoln("Instantiating Triton server on GCP")
		gcs, err := blob.NewBlobGCP(bucket)
		if err != nil {
			return nil, err
		}
		datastore, err := datastore.NewDriver(ctx, project)
		if err != nil {
			return nil, err
		}
		metadb := metadb.NewMetaDB(datastore)
		if err := metadb.Connect(ctx); err != nil {
			log.Fatalf("Failed to connect to the metadata server: %v", err)
			return nil, err
		}
		triton := &tritonServer{
			cloud:     cloud,
			blobStore: gcs,
			metaDB:    metadb,
		}
		return triton, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cloud)
	}
}

func (s *tritonServer) CreateStore(ctx context.Context, req *tritonpb.CreateStoreRequest) (*tritonpb.Store, error) {
	store := metadb.Store{
		Key:     req.Store.Key,
		Name:    req.Store.Name,
		Tags:    req.Store.Tags,
		OwnerID: req.Store.OwnerId,
	}
	if err := s.metaDB.CreateStore(ctx, &store); err != nil {
		log.Warnf("CreateStore failed for store (%s): %v", store.Key, err)
		return nil, status.Convert(err).Err()
	}
	log.Infof("Created store: %+v", store)
	return req.Store, nil
}

func (s *tritonServer) CreateRecord(ctx context.Context, req *tritonpb.CreateRecordRequest) (*tritonpb.Record, error) {
	record := metadb.NewRecordFromProto(req.Record)
	err := s.metaDB.InsertRecord(ctx, req.StoreKey, record)
	if err != nil {
		log.Warnf("CreateRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.Record.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Infof("Created record: %+v", record)
	return req.Record, nil
}

func (s *tritonServer) DeleteRecord(ctx context.Context, req *tritonpb.DeleteRecordRequest) (*empty.Empty, error) {
	err := s.metaDB.DeleteRecord(ctx, req.GetStoreKey(), req.GetKey())
	if err != nil {
		log.Warnf("DeleteRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Infof("Deleted record: store (%s), record (%s)",
		req.GetStoreKey(), req.GetKey())
	return new(empty.Empty), nil
}

func (s *tritonServer) GetStore(ctx context.Context, req *tritonpb.GetStoreRequest) (*tritonpb.Store, error) {
	store, err := s.metaDB.GetStore(ctx, req.GetKey())
	if err != nil {
		log.Warnf("GetStore failed for store (%s): %v", req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	return store.ToProto(), nil
}

func (s *tritonServer) ListStores(ctx context.Context, req *tritonpb.ListStoresRequest) (*tritonpb.ListStoresResponse, error) {
	store, err := s.metaDB.FindStoreByName(ctx, req.Name)
	if err != nil {
		log.Warnf("ListStores failed: %v", err)
		return nil, status.Convert(err).Err()
	}
	storeProtos := []*tritonpb.Store{store.ToProto()}
	res := &tritonpb.ListStoresResponse{
		Stores: storeProtos,
	}
	return res, nil
}

func (s *tritonServer) DeleteStore(ctx context.Context, req *tritonpb.DeleteStoreRequest) (*empty.Empty, error) {
	err := s.metaDB.DeleteStore(ctx, req.GetKey())
	if err != nil {
		log.Warnf("DeleteStore failed for store (%s): %v", req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Infof("Deletes store: %s", req.GetKey())
	return new(empty.Empty), nil
}

func (s *tritonServer) GetRecord(ctx context.Context, req *tritonpb.GetRecordRequest) (*tritonpb.Record, error) {
	record, err := s.metaDB.GetRecord(ctx, req.GetStoreKey(), req.GetKey())
	if err != nil {
		log.Warnf("GetRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Infof("Got record: %+v", record)
	return record.ToProto(), nil
}

func (s *tritonServer) UpdateRecord(ctx context.Context, req *tritonpb.UpdateRecordRequest) (*tritonpb.Record, error) {
	record := metadb.NewRecordFromProto(req.GetRecord())
	err := s.metaDB.UpdateRecord(ctx, req.GetStoreKey(), record)
	if err != nil {
		log.Warnf("UpdateRecord failed for store(%s), record (%s): %v",
			req.GetStoreKey(), req.GetRecord().GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	return record.ToProto(), nil
}

func (s *tritonServer) QueryRecords(ctx context.Context, req *tritonpb.QueryRecordsRequest) (*tritonpb.QueryRecordsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "QueryRecords is not implemented yet.")
}
