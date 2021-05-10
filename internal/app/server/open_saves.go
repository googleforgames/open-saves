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
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/blob"
	"github.com/googleforgames/open-saves/internal/pkg/cache"
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

// TODO(hongalex): make this a configurable field for users.
const (
	maxRecordSizeToCache int = 10 * 1024 * 1024 // 10 MB
	maxInlineBlobSize    int = 64 * 1024        // 64 KiB
	streamBufferSize     int = 1 * 1024 * 1024  // 1 MiB
	opaqueStringLimit    int = 32 * 1024        // 32 KiB
)

type openSavesServer struct {
	cloud      string
	blobStore  blob.BlobStore
	metaDB     *metadb.MetaDB
	cacheStore cache.Cache

	pb.UnimplementedOpenSavesServer
}

// Assert openSavesServer implements pb.OpenSavesServer
var _ pb.OpenSavesServer = new(openSavesServer)

// newOpenSavesServer creates a new instance of the Open Saves server.
func newOpenSavesServer(ctx context.Context, cloud, project, bucket, cacheAddr string) (*openSavesServer, error) {
	log.Infof("Creating a new Open Saves server instance: cloud = %v, project = %v, bucket = %v, cache address = %v",
		cloud, project, bucket, cacheAddr)

	switch cloud {
	case "gcp":
		log.Infoln("Instantiating Open Saves server on GCP")
		gcs, err := blob.NewBlobGCP(bucket)
		if err != nil {
			return nil, err
		}
		metadb, err := metadb.NewMetaDB(ctx, project)
		if err != nil {
			log.Fatalf("Failed to create a MetaDB instance: %v", err)
			return nil, err
		}
		redis := cache.NewRedis(cacheAddr)
		server := &openSavesServer{
			cloud:      cloud,
			blobStore:  gcs,
			metaDB:     metadb,
			cacheStore: redis,
		}
		return server, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cloud)
	}
}

func (s *openSavesServer) CreateStore(ctx context.Context, req *pb.CreateStoreRequest) (*pb.Store, error) {
	store := store.Store{
		Key:     req.Store.Key,
		Name:    req.Store.Name,
		Tags:    req.Store.Tags,
		OwnerID: req.Store.OwnerId,
	}
	newStore, err := s.metaDB.CreateStore(ctx, &store)
	if err != nil {
		log.Warnf("CreateStore failed for store (%s): %v", store.Key, err)
		return nil, status.Convert(err).Err()
	}
	log.Debugf("Created store: %+v", store)
	return newStore.ToProto(), nil
}

func (s *openSavesServer) CreateRecord(ctx context.Context, req *pb.CreateRecordRequest) (*pb.Record, error) {
	record := record.NewRecordFromProto(req.Record)
	if err := checkRecord(record); err != nil {
		return nil, err
	}
	newRecord, err := s.metaDB.InsertRecord(ctx, req.StoreKey, record)
	if err != nil {
		log.Warnf("CreateRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.Record.GetKey(), err)
		return nil, status.Convert(err).Err()
	}

	if shouldCache(req.Hint) {
		k := cache.FormatKey(req.GetStoreKey(), req.GetRecord().GetKey())
		s.storeRecordInCache(ctx, k, newRecord)
	}
	return newRecord.ToProto(), nil
}

func (s *openSavesServer) DeleteRecord(ctx context.Context, req *pb.DeleteRecordRequest) (*empty.Empty, error) {
	err := s.metaDB.DeleteRecord(ctx, req.GetStoreKey(), req.GetKey())
	if err != nil {
		log.Warnf("DeleteRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Debugf("Deleted record: store (%s), record (%s)",
		req.GetStoreKey(), req.GetKey())

	// Purge record from cache store.
	k := cache.FormatKey(req.GetStoreKey(), req.GetKey())
	if err := s.cacheStore.Delete(ctx, k); err != nil {
		log.Errorf("failed to purge cache for key (%s): %v", k, err)
	}

	return new(empty.Empty), nil
}

func (s *openSavesServer) GetStore(ctx context.Context, req *pb.GetStoreRequest) (*pb.Store, error) {
	store, err := s.metaDB.GetStore(ctx, req.GetKey())
	if err != nil {
		log.Warnf("GetStore failed for store (%s): %v", req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	return store.ToProto(), nil
}

func (s *openSavesServer) ListStores(ctx context.Context, req *pb.ListStoresRequest) (*pb.ListStoresResponse, error) {
	store, err := s.metaDB.FindStoreByName(ctx, req.Name)
	if err != nil {
		log.Warnf("ListStores failed: %v", err)
		return nil, status.Convert(err).Err()
	}
	storeProtos := []*pb.Store{store.ToProto()}
	res := &pb.ListStoresResponse{
		Stores: storeProtos,
	}
	return res, nil
}

func (s *openSavesServer) DeleteStore(ctx context.Context, req *pb.DeleteStoreRequest) (*empty.Empty, error) {
	err := s.metaDB.DeleteStore(ctx, req.GetKey())
	if err != nil {
		log.Warnf("DeleteStore failed for store (%s): %v", req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Debugf("Deletes store: %s", req.GetKey())
	return new(empty.Empty), nil
}

func (s *openSavesServer) GetRecord(ctx context.Context, req *pb.GetRecordRequest) (*pb.Record, error) {
	k := cache.FormatKey(req.GetStoreKey(), req.GetKey())

	if shouldCheckCache(req.Hint) {
		r, err := s.getRecordFromCache(ctx, k)
		if err != nil {
			log.Debug("cache miss")
		} else if r != nil {
			return r.ToProto(), nil
		}
	}

	record, err := s.metaDB.GetRecord(ctx, req.GetStoreKey(), req.GetKey())
	if err != nil {
		log.Warnf("GetRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.GetKey(), err)
		return nil, status.Convert(err).Err()
	}
	log.Debugf("Got record %+v", record)

	// Update cache store.
	if shouldCache(req.Hint) {
		s.storeRecordInCache(ctx, k, record)
	}

	return record.ToProto(), nil
}

func (s *openSavesServer) UpdateRecord(ctx context.Context, req *pb.UpdateRecordRequest) (*pb.Record, error) {
	updateTo := record.NewRecordFromProto(req.GetRecord())
	if err := checkRecord(updateTo); err != nil {
		return nil, err
	}
	newRecord, err := s.metaDB.UpdateRecord(ctx, req.GetStoreKey(), updateTo.Key,
		func(r *record.Record) (*record.Record, error) {
			r.OwnerID = updateTo.OwnerID
			r.Properties = updateTo.Properties
			r.Tags = updateTo.Tags
			r.Blob = updateTo.Blob
			r.BlobSize = updateTo.BlobSize
			r.OpaqueString = updateTo.OpaqueString
			return r, nil
		})
	if err != nil {
		log.Warnf("UpdateRecord failed for store(%s), record (%s): %v",
			req.GetStoreKey(), req.GetRecord().GetKey(), err)
		return nil, status.Convert(err).Err()
	}

	// Update cache store.
	if shouldCache(req.Hint) {
		k := cache.FormatKey(req.GetStoreKey(), req.GetRecord().GetKey())
		s.storeRecordInCache(ctx, k, newRecord)
	}

	return newRecord.ToProto(), nil
}

func (s *openSavesServer) QueryRecords(ctx context.Context, stream *pb.QueryRecordsRequest) (*pb.QueryRecordsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "QueryRecords is not implemented yet.")
}

func (s *openSavesServer) GetAggregation(ctx context.Context, req *pb.GetAggregationRequest) (*pb.Record, error) {
	r, err := s.metaDB.GetAggregation(ctx, req.Aggregation.String(), req.StoreKey, req.Field)
	if err != nil {
		return nil, err
	}
	return r.ToProto(), nil
}

func (s *openSavesServer) insertInlineBlob(ctx context.Context, stream pb.OpenSaves_CreateBlobServer, meta *pb.BlobMetadata) error {
	log.Debugf("Inserting inline blob: %v\n", meta)
	// Receive the blob
	size := meta.GetSize()
	buffer := bytes.NewBuffer(make([]byte, 0, size))
	recvd := 0
	for {
		if int64(recvd) > size {
			break
		}
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fragment := req.GetContent()
		if fragment == nil {
			return status.Error(codes.InvalidArgument, "Subsequent input messages must contain blob content")
		}
		n, err := buffer.Write(fragment)
		if err != nil {
			return err
		}
		recvd += n
	}

	if int64(buffer.Len()) != size {
		log.Errorf("Blob length didn't match the metadata: metadata = %v, actual = %v",
			meta.GetSize(), buffer.Len())
		return status.Errorf(codes.InvalidArgument,
			"Blob length didn't match the metadata: metadata = %v, actual = %v",
			meta.GetSize(), buffer.Len(),
		)
	}
	blob := buffer.Bytes()
	record, err := s.metaDB.UpdateRecord(ctx, meta.GetStoreKey(), meta.GetRecordKey(),
		func(record *record.Record) (*record.Record, error) {
			record.Blob = blob
			record.BlobSize = size
			return record, nil
		})
	if err != nil {
		return nil
	}
	cacheKey := cache.FormatKey(meta.GetStoreKey(), meta.GetRecordKey())
	if shouldCache(meta.Hint) {
		s.storeRecordInCache(ctx, cacheKey, record)
	} else {
		if err := s.cacheStore.Delete(ctx, cacheKey); err != nil {
			log.Errorf("failed to purge cache for key (%s): %v", cacheKey, err)
		}
	}
	return stream.SendAndClose(meta)
}

func (s *openSavesServer) blobRefFail(ctx context.Context, blobref *blobref.BlobRef) {
	blobref.Fail()
	_, err := s.metaDB.UpdateBlobRef(ctx, blobref)
	if err != nil {
		log.Errorf("Failed to mark the blobref (%v) as Failed: %v", blobref.Key, err)
	}
}

func (s *openSavesServer) insertExternalBlob(ctx context.Context, stream pb.OpenSaves_CreateBlobServer, meta *pb.BlobMetadata) error {
	log.Debugf("Inserting external blob: %v\n", meta)
	// Create a blob reference based on the metadata.
	blobref := blobref.NewBlobRef(meta.GetSize(), meta.GetStoreKey(), meta.GetRecordKey())
	blobref, err := s.metaDB.InsertBlobRef(ctx, blobref)
	if err != nil {
		return err
	}
	writer, err := s.blobStore.NewWriter(ctx, blobref.ObjectPath())
	if err != nil {
		return err
	}
	defer func() {
		if writer != nil {
			writer.Close()
			// This means an abnormal exit, so make sure to mark the blob as Fail.
			s.blobRefFail(ctx, blobref)
		}
	}()

	written := int64(0)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("CreateBlob stream recv error: %v", err)
			return err
		}
		fragment := req.GetContent()
		if fragment == nil {
			return status.Error(codes.InvalidArgument, "Subsequent input messages must contain blob content")
		}
		n, err := writer.Write(fragment)
		if err != nil {
			log.Errorf("CreateBlob BlobStore write error: %v", err)
			return err
		}
		written += int64(n)
	}
	err = writer.Close()
	writer = nil
	if err != nil {
		log.Errorf("writer.Close() failed on blob object %v: %v", blobref.ObjectPath(), err)
		s.blobRefFail(ctx, blobref)
		// The object can be deleted immediately.
		if derr := s.blobStore.Delete(ctx, blobref.ObjectPath()); derr != nil {
			log.Errorf("Delete blob failed after writer.Close() error in insertExternalBlob: %v", derr)
		}
		return err
	}
	if written != meta.GetSize() {
		log.Errorf("Written byte length (%v) != blob length in metadata sent from client (%v)", written, meta.GetSize())
		s.blobRefFail(ctx, blobref)
		return status.Errorf(codes.DataLoss,
			"Written byte length (%v) != blob length in metadata sent from client (%v)", written, meta.GetSize())
	}
	record, _, err := s.metaDB.PromoteBlobRefToCurrent(ctx, blobref)
	if err != nil {
		log.Errorf("PromoteBlobRefToCurrent failed for object %v: %v", blobref.ObjectPath(), err)
		// Do not delete the blob object here. Leave it to the garbage collector.
		return err
	}
	cacheKey := cache.FormatKey(meta.GetStoreKey(), meta.GetRecordKey())
	if shouldCache(meta.Hint) {
		s.storeRecordInCache(ctx, cacheKey, record)
	} else {
		if err := s.cacheStore.Delete(ctx, cacheKey); err != nil {
			log.Errorf("failed to purge cache for key (%s): %v", cacheKey, err)
		}
	}
	return stream.SendAndClose(meta)
}

func (s *openSavesServer) CreateBlob(stream pb.OpenSaves_CreateBlobServer) error {
	log.Debug("Creating blob stream\n")
	ctx := stream.Context()

	// The first message must be metadata.
	req, err := stream.Recv()
	if err != nil {
		log.Errorf("CreateBlob stream recv error: %v", err)
		return err
	}
	meta := req.GetMetadata()
	if meta == nil {
		log.Error("CreateBlob: first message was not metadata")
		return status.Error(codes.InvalidArgument, "The first message must be metadata.")
	}
	log.Debugf("Got metadata from stream: store(%s), record(%s), blob size(%d)\n",
		meta.GetStoreKey(), meta.GetRecordKey(), meta.GetSize())

	// TODO(yuryu): Make the threshold configurable
	if meta.GetSize() <= int64(maxInlineBlobSize) {
		return s.insertInlineBlob(ctx, stream, meta)
	}
	return s.insertExternalBlob(ctx, stream, meta)
}

func (s *openSavesServer) getExternalBlob(ctx context.Context, req *pb.GetBlobRequest, stream pb.OpenSaves_GetBlobServer, record *record.Record) error {
	log.Debugf("Reading external blob %v", record.ExternalBlob)
	blobref, err := s.metaDB.GetBlobRef(ctx, record.ExternalBlob)
	if err != nil {
		log.Errorf("GetBlobRef returned error for blob ref (%v): %v", record.ExternalBlob, err)
		return err
	}

	reader, err := s.blobStore.NewReader(ctx, blobref.ObjectPath())
	if err != nil {
		log.Errorf("BlobStore.NewReader returned error for object (%v): %v", blobref.ObjectPath(), err)
		return err
	}
	defer reader.Close()
	buf := make([]byte, streamBufferSize)
	sent := int64(0)
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("GetBlob: BlobStore Reader returned error for object (%v): %v", blobref.ObjectPath(), err)
			return err
		}
		err = stream.Send(&pb.GetBlobResponse{Response: &pb.GetBlobResponse_Content{
			Content: buf[:n],
		}})
		if err != nil {
			log.Errorf("GetBlob: Stream send error for object (%v): %v", blobref.ObjectPath(), err)
			return err
		}
		sent += int64(n)
	}
	if sent != blobref.Size {
		log.Errorf("GetBlob: Blob size sent (%v) and stored in the metadata (%v) don't match.", sent, blobref.Size)
		return status.Errorf(codes.DataLoss,
			"GetBlob: Blob size sent (%v) and stored in the metadata (%v) don't match.", sent, blobref.Size)
	}
	return nil
}

func (s *openSavesServer) GetBlob(req *pb.GetBlobRequest, stream pb.OpenSaves_GetBlobServer) error {
	ctx := stream.Context()

	var record *record.Record
	var err error
	if shouldCheckCache(req.Hint) {
		record, _ = s.getRecordFromCache(ctx, cache.FormatKey(req.GetStoreKey(), req.GetRecordKey()))
	}
	if record != nil {
		record, err = s.metaDB.GetRecord(ctx, req.GetStoreKey(), req.GetRecordKey())
		if err != nil {
			log.Errorf("GetBlob: GetRecord failed for store (%v), record (%v): %v",
				req.GetStoreKey(), req.GetRecordKey(), err)
		}
		if shouldCache(req.Hint) {
			s.storeRecordInCache(ctx, cache.FormatKey(req.GetStoreKey(), req.GetRecordKey()), record)
		}
	}
	meta := &pb.BlobMetadata{
		StoreKey:  req.GetStoreKey(),
		RecordKey: record.Key,
		Size:      record.BlobSize,
	}
	stream.Send(&pb.GetBlobResponse{Response: &pb.GetBlobResponse_Metadata{Metadata: meta}})
	if record.ExternalBlob != uuid.Nil {
		return s.getExternalBlob(ctx, req, stream, record)
	}
	err = stream.Send(&pb.GetBlobResponse{Response: &pb.GetBlobResponse_Content{Content: record.Blob}})
	if err != nil {
		log.Errorf("GetBlob: Stream send error for store (%v), record (%v): %v", req.GetRecordKey(), record.Key, err)
	}
	return err
}

func (s *openSavesServer) DeleteBlob(ctx context.Context, req *pb.DeleteBlobRequest) (*empty.Empty, error) {
	record, _, err := s.metaDB.RemoveBlobFromRecord(ctx, req.GetStoreKey(), req.GetRecordKey())
	if err != nil {
		log.Errorf("DeleteBlob: RemoveBlobFromRecord failed, store = %v, record = %v: %v",
			req.GetStoreKey(), req.GetRecordKey(), err)
	} else {
		k := cache.FormatKey(req.GetStoreKey(), req.GetRecordKey())
		if shouldCache(req.Hint) {
			s.storeRecordInCache(ctx, k, record)
		} else {
			if err := s.cacheStore.Delete(ctx, k); err != nil {
				log.Errorf("failed to purge cache for key (%s): %v", k, err)
			}
		}
	}
	return new(empty.Empty), err
}

func (s *openSavesServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Pong: req.GetPing(),
	}, nil
}

func (s *openSavesServer) getRecordFromCache(ctx context.Context, key string) (*record.Record, error) {
	r, err := s.cacheStore.Get(ctx, key)
	if err != nil {
		// cache miss.
		return nil, err
	}
	// cache hit, use value from cache store.
	record, err := cache.DecodeRecord(r)
	if err != nil {
		return nil, err
	}
	log.Debugf("cache hit: %+v", record)
	return record, nil
}

func (s *openSavesServer) storeRecordInCache(ctx context.Context, key string, record *record.Record) {
	by, err := cache.EncodeRecord(record)
	if err != nil {
		// Cache fails should be logged but not return error.
		log.Warnf("failed to encode record for cache for key (%s): %v", key, err)
	} else {
		if len(by) < maxRecordSizeToCache {
			if err := s.cacheStore.Set(ctx, key, by); err != nil {
				log.Warnf("failed to update cache for key (%s): %v", key, err)
			}
		}
		// TODO(yuryu): should delete the entry anyway in the case
		// record size grows and gets too big to cache but still needs to be invalidated.
	}
}

// shouldCache returns whether or not Open Saves should try to store
// the record in the cache store. Default behavior is to cache
// if hint is not specified.
func shouldCache(hint *pb.Hint) bool {
	if hint == nil {
		return true
	}
	return !hint.DoNotCache
}

// shouldCheckCache returns whether or not Open Saves should try to check
// the record in the cache store. Default behavior is to check
// the cache if hint is not specified.
func shouldCheckCache(hint *pb.Hint) bool {
	if hint == nil {
		return true
	}
	return !hint.SkipCache
}

// checkRecord checks a record against size limits.
// Returns nil if the record satisfies conditions.
// Returns codes.InvalidArgument if it does not.
func checkRecord(record *record.Record) error {
	if len(record.OpaqueString) > opaqueStringLimit {
		return status.Errorf(codes.InvalidArgument, "The length of OpaqueString exceeds %v bytes (actual = %v bytes)",
			opaqueStringLimit, len(record.OpaqueString))
	}
	return nil
}
