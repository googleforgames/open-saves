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
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/blob"
	"github.com/googleforgames/open-saves/internal/pkg/cache"
	"github.com/googleforgames/open-saves/internal/pkg/cache/redis"
	"github.com/googleforgames/open-saves/internal/pkg/config"
	"github.com/googleforgames/open-saves/internal/pkg/metadb"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	empty "google.golang.org/protobuf/types/known/emptypb"
	"io"
	"time"
)

// TODO(hongalex): make this a configurable field for users.
const (
	maxRecordSizeToCache int = 10 * 1024 * 1024       // 10 MB
	streamBufferSize     int = 1 * 1024 * 1024        // 1 MiB
	chunkSizeLimit       int = 1 * 1024 * 1024 * 1024 // 1 GiB
)

type openSavesServer struct {
	cloud      string
	blobStore  blob.BlobStore
	metaDB     *metadb.MetaDB
	cacheStore *cache.Cache
	config.ServiceConfig

	pb.UnimplementedOpenSavesServer
}

// Assert openSavesServer implements pb.OpenSavesServer
var _ pb.OpenSavesServer = new(openSavesServer)

// newOpenSavesServer creates a new instance of the Open Saves server.
func newOpenSavesServer(ctx context.Context, cfg *config.ServiceConfig) (*openSavesServer, error) {
	log.Infof("Creating a new Open Saves server instance: cloud = %v, project = %v, bucket = %v, cache address = %v",
		cfg.ServerConfig.Cloud, cfg.ServerConfig.Project, cfg.ServerConfig.Bucket, cfg.RedisConfig.Address)

	switch cfg.ServerConfig.Cloud {
	case "gcp":
		log.Infoln("Instantiating Open Saves server on GCP")
		gcs, err := blob.NewBlobGCP(ctx, cfg.ServerConfig.Bucket)
		if err != nil {
			return nil, err
		}
		metadb, err := metadb.NewMetaDB(ctx, cfg.ServerConfig.Project)
		if err != nil {
			log.Fatalf("Failed to create a MetaDB instance: %v", err)
			return nil, err
		}
		cache := cache.New(redis.NewRedisWithConfig(&cfg.RedisConfig), &cfg.CacheConfig)
		server := &openSavesServer{
			cloud:         cfg.ServerConfig.Cloud,
			blobStore:     gcs,
			metaDB:        metadb,
			cacheStore:    cache,
			ServiceConfig: *cfg,
		}
		return server, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cfg.ServerConfig.Cloud)
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
	s.storeCache(ctx, &store)
	return newStore.ToProto(), nil
}

func (s *openSavesServer) CreateRecord(ctx context.Context, req *pb.CreateRecordRequest) (*pb.Record, error) {
	record, err := record.FromProto(req.GetStoreKey(), req.GetRecord())
	if err != nil {
		log.Errorf("Invalid record proto for store (%s), record (%s): %v", req.GetStoreKey(), req.GetRecord().GetKey(), err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid record proto: %v", err)
	}
	newRecord, err := s.metaDB.InsertRecord(ctx, req.StoreKey, record)
	if err != nil {
		log.Warnf("CreateRecord failed for store (%s), record (%s): %v",
			req.GetStoreKey(), req.Record.GetKey(), err)
		return nil, status.Convert(err).Err()
	}

	s.cacheRecord(ctx, newRecord, req.GetHint())
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
	if err := s.cacheStore.Delete(ctx, record.CacheKey(req.GetStoreKey(), req.GetKey())); err != nil {
		log.Errorf("failed to purge cache for store (%s), record (%s): %v",
			req.GetStoreKey(), req.GetKey(), err)
	}

	return new(empty.Empty), nil
}

func (s *openSavesServer) GetStore(ctx context.Context, req *pb.GetStoreRequest) (*pb.Store, error) {
	str, err := s.getStoreAndCache(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	return str.ToProto(), nil
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
	record, err := s.getRecordAndCache(ctx, req.GetStoreKey(), req.GetKey(), req.GetHint())
	if err != nil {
		return nil, err
	}

	return record.ToProto(), nil
}

func (s *openSavesServer) UpdateRecord(ctx context.Context, req *pb.UpdateRecordRequest) (*pb.Record, error) {
	updateTo, err := record.FromProto(req.GetStoreKey(), req.GetRecord())
	if err != nil {
		log.Errorf("Invalid proto for store (%s), record (%s): %v", req.GetStoreKey(), req.GetRecord().GetKey(), err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid record proto: %v", err)
	}
	newRecord, err := s.metaDB.UpdateRecord(ctx, req.GetStoreKey(), updateTo.Key,
		func(r *record.Record) (*record.Record, error) {
			if updateTo.Timestamps.Signature != uuid.Nil && r.Timestamps.Signature != updateTo.Timestamps.Signature {
				return nil, status.Errorf(codes.Aborted, "Signature mismatch: expected (%v), actual (%v)",
					updateTo.Timestamps.Signature.String(), r.Timestamps.Signature.String())
			}
			r.OwnerID = updateTo.OwnerID
			r.Properties = updateTo.Properties
			r.Tags = updateTo.Tags
			r.OpaqueString = updateTo.OpaqueString
			// If the update request does not include a new ExpiresAt value
			// then here it will be time's zero value and thus it will be ignored when saving.
			// In other words, once expiration is set it cannot be removed via updates.
			r.ExpiresAt = updateTo.ExpiresAt
			return r, nil
		})
	if err != nil {
		log.Warnf("UpdateRecord failed for store(%s), record (%s): %v",
			req.GetStoreKey(), req.GetRecord().GetKey(), err)
		return nil, err
	}

	// Update cache store.
	s.cacheRecord(ctx, newRecord, req.GetHint())

	return newRecord.ToProto(), nil
}

func (s *openSavesServer) QueryRecords(ctx context.Context, req *pb.QueryRecordsRequest) (*pb.QueryRecordsResponse, error) {
	records, err := s.metaDB.QueryRecords(ctx, req)
	if err != nil {
		log.Warnf("QueryRecords failed for store(%s), filters(%+v): %v",
			req.StoreKey, req.Filters, err)
		return nil, err
	}
	var rr []*pb.Record
	var storeKeys []string
	for _, r := range records {
		rr = append(rr, r.ToProto())
		storeKeys = append(storeKeys, r.StoreKey)
	}
	return &pb.QueryRecordsResponse{
		Records:   rr,
		StoreKeys: storeKeys,
	}, nil
}

func (s *openSavesServer) GetRecords(ctx context.Context, req *pb.GetRecordsRequest) (*pb.GetRecordsResponse, error) {
	records, err := s.metaDB.GetRecords(ctx, req.GetStoreKeys(), req.GetKeys())

	// Check if there was an unexpected error
	var errors datastore.MultiError
	if err != nil {
		var ok bool
		if errors, ok = err.(datastore.MultiError); !ok {
			log.Errorf("GetRecords unable to retrieve records: %v", err)
			return nil, err
		}
	}

	response := &pb.GetRecordsResponse{
		Results: []*pb.GetRecordsResponse_Result{},
	}
	for i, rec := range records {
		st := status.New(codes.OK, codes.OK.String())

		if errors != nil && errors[i] != nil {
			st = status.Convert(errors[i])
		}
		result := &pb.GetRecordsResponse_Result{
			Record:   rec.ToProto(),
			StoreKey: rec.GetStoreKey(),
			Status:   st.Proto(),
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func (s *openSavesServer) insertInlineBlob(ctx context.Context, stream pb.OpenSaves_CreateBlobServer, meta *pb.BlobMetadata) error {
	log.Debugf("Inserting inline blob: %v\n", meta)
	// Receive the blob
	size := meta.GetSize()
	buffer := new(bytes.Buffer)
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
	digest := checksums.NewDigest()
	digest.Write(blob)
	checksums := digest.Checksums()
	if err := checksums.ValidateIfPresent(meta); err != nil {
		log.Error(err)
		return err
	}
	// UpdateRecord also marks any associated external blob for deletion.
	record, err := s.metaDB.UpdateRecord(ctx, meta.GetStoreKey(), meta.GetRecordKey(),
		func(record *record.Record) (*record.Record, error) {
			record.Blob = blob
			record.BlobSize = size
			record.Checksums = checksums
			return record, nil
		})
	if err != nil {
		return nil
	}
	s.cacheRecord(ctx, record, meta.GetHint())
	return stream.SendAndClose(meta)
}

func (s *openSavesServer) blobRefFail(ctx context.Context, blobref *blobref.BlobRef) {
	blobref.Fail()
	blobref.MarkAsExpired()
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
	digest := checksums.NewDigest()
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
		digest.Write(fragment)
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
	blobref.Checksums = digest.Checksums()
	if err := blobref.ValidateIfPresent(meta); err != nil {
		log.Error(err)
		return err
	}
	record, _, err := s.metaDB.PromoteBlobRefToCurrent(ctx, blobref)
	if err != nil {
		log.Errorf("PromoteBlobRefToCurrent failed for object %v: %v", blobref.ObjectPath(), err)
		// Do not delete the blob object here. Leave it to the garbage collector.
		return err
	}
	s.cacheRecord(ctx, record, meta.GetHint())
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

	if meta.GetSize() <= int64(s.BlobConfig.MaxInlineSize) {
		return s.insertInlineBlob(ctx, stream, meta)
	}
	return s.insertExternalBlob(ctx, stream, meta)
}

func (s *openSavesServer) getExternalBlob(ctx context.Context, stream pb.OpenSaves_GetBlobServer, record *record.Record) error {
	log.Debugf("Reading external blob %v", record.ExternalBlob)
	blobref, err := s.metaDB.GetBlobRef(ctx, record.ExternalBlob)
	if err != nil {
		log.Errorf("GetBlobRef returned error for blob ref (%v): %v", record.ExternalBlob, err)
		return err
	}

	meta := blobref.ToProto()
	stream.Send(&pb.GetBlobResponse{Response: &pb.GetBlobResponse_Metadata{Metadata: meta}})

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

	rr, err := s.getRecordAndCache(ctx, req.GetStoreKey(), req.GetRecordKey(), req.GetHint())
	if err != nil {
		log.Errorf("Failed to get record for store (%s), record(%s): %v",
			req.GetStoreKey(), req.GetRecordKey(), err)
		return err
	}

	if rr.ExternalBlob != uuid.Nil {
		return s.getExternalBlob(ctx, stream, rr)
	}

	// Handle the inline blob here.
	meta := rr.GetInlineBlobMetadata()
	stream.Send(&pb.GetBlobResponse{Response: &pb.GetBlobResponse_Metadata{Metadata: meta}})
	err = stream.Send(&pb.GetBlobResponse{Response: &pb.GetBlobResponse_Content{Content: rr.Blob}})
	if err != nil {
		log.Errorf("GetBlob: Stream send error for store (%v), record (%v): %v", req.GetRecordKey(), rr.Key, err)
	}
	return err
}

func (s *openSavesServer) DeleteBlob(ctx context.Context, req *pb.DeleteBlobRequest) (*empty.Empty, error) {
	rr, _, err := s.metaDB.RemoveBlobFromRecord(ctx, req.GetStoreKey(), req.GetRecordKey())
	if err != nil {
		log.Errorf("DeleteBlob: RemoveBlobFromRecord failed, store = %v, record = %v: %v",
			req.GetStoreKey(), req.GetRecordKey(), err)
	} else {
		s.cacheRecord(ctx, rr, req.GetHint())
	}
	return new(empty.Empty), err
}

func (s *openSavesServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Pong: req.GetPing(),
	}, nil
}

func (s *openSavesServer) CreateChunkedBlob(ctx context.Context, req *pb.CreateChunkedBlobRequest) (*pb.CreateChunkedBlobResponse, error) {
	// Initialize the Blob with a temporary expire time as 1 week from now.

	b := blobref.NewChunkedBlobRef(req.GetStoreKey(), req.GetRecordKey(), req.GetChunkCount())
	// Set the temporary ExpiresAt to allow clean up of incomplete uploads.
	expiresAt := time.Now().Add(s.BlobConfig.DefaultGarbageCollectionTTL)
	b.ExpiresAt = expiresAt

	b, err := s.metaDB.InsertBlobRef(ctx, b)
	if err != nil {
		log.Errorf("CreateChunkedBlob failed for store (%v), record (%v): %v", req.GetStoreKey(), req.GetRecordKey(), err)
		return nil, err
	}
	return &pb.CreateChunkedBlobResponse{
		SessionId: b.Key.String(),
	}, nil
}

func (s *openSavesServer) CreateChunkUrls(ctx context.Context, req *pb.CreateChunkUrlsRequest) (*pb.CreateChunkUrlsResponse, error) {
	blobRef, err := s.metaDB.GetCurrentBlobRef(ctx, req.GetStoreKey(), req.GetKey())
	if err != nil {
		return nil, err
	}

	var urls []string
	cur := s.metaDB.GetChildChunkRefs(ctx, blobRef.Key)
	for {
		chunk, err := cur.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Errorf("cursor.Next() returned error: %v", err)
			return nil, err
		}

		url, err := s.blobStore.SignUrl(ctx, chunk.Key.String(), req.GetTtlInSeconds(), "GET")
		if err != nil {
			log.Errorf("CreateChunkUrls failed to get sign url for chunkNumber(%v), Bucket(%v), ChunkKey(%v) :%v", chunk.Number, s.ServerConfig.Bucket, chunk.Key, err)
			return nil, err
		}
		urls = append(urls, url)
	}

	if len(urls) == 0 {
		log.Errorf("CreateChunkUrls: not found any available chunk blobRef(%s) for record(%s)", blobRef.Key, req.GetKey())
		return nil, status.Errorf(codes.NotFound, "CreateChunkUrls: not found any available chunk record(%s) on store(%s)", req.GetKey(), req.GetStoreKey())
	}

	return &pb.CreateChunkUrlsResponse{ChunkUrls: urls}, nil
}

// deleteObjectOnExit deletes the object from GS if there are errors uploading the data or inserting a chunkref
func (s *openSavesServer) deleteObjectOnExit(ctx context.Context, path string) error {
	err := s.blobStore.Delete(ctx, path)
	if err != nil {
		log.Errorf("Delete chunk failed after writer.Close() error: %v", err)
	}
	return err
}

func (s *openSavesServer) deleteSameNumberChunks(ctx context.Context, chunk *chunkref.ChunkRef) error {
	otherChunks, err := s.metaDB.FindBlobChunkRefsByNumber(ctx, chunk.BlobRef, chunk.Number)
	if err != nil {
		return err
	}
	log.Debugf("Found (%d) matches for chunkref (%s), blobref (%s) with same number (%d)",
		len(otherChunks), chunk.Key, chunk.BlobRef, chunk.Number)
	for _, o := range otherChunks {
		log.Debugf("Deleting chunkref (%s) for blobref (%s) with same number (%d)", chunk.Key, chunk.BlobRef, chunk.Number)
		err := s.deleteObjectOnExit(ctx, o.ObjectPath())
		if err != nil {
			return err
		}
		err = s.metaDB.DeleteChunkRef(ctx, o.BlobRef, o.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *openSavesServer) UploadChunk(stream pb.OpenSaves_UploadChunkServer) error {
	ctx := stream.Context()
	req, err := stream.Recv()
	if err != nil {
		log.Errorf("Recv() returned error: %v", err)
		return err
	}
	meta := req.GetMetadata()
	if meta == nil {
		log.Error("GetMetadata() returned nil. First message should be a ChunkMetadata.")
		return status.Error(codes.InvalidArgument, "First message should be a ChunkMetadata.")
	}
	blobKey, err := uuid.Parse(meta.GetSessionId())
	if err != nil {
		log.Errorf("SessionId is not a valid UUID string: %v", err)
		return status.Errorf(codes.InvalidArgument, "SessionId is not a valid UUID string: %v", err)
	}

	// Create a chunk reference based on the metadata. Do not add to blobref right away to minimize writes
	chunk := chunkref.New(blobKey, int32(meta.GetNumber()))
	blob, err := s.metaDB.ValidateChunkRefPreconditions(ctx, chunk)
	if err != nil {
		return err
	}

	contextWithCancel, cancel := context.WithCancel(ctx)
	writer, err := s.blobStore.NewWriter(contextWithCancel, chunk.ObjectPath())
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		if writer != nil {
			cancel()
			_ = writer.Close()
		}
	}()

	written := 0
	digest := checksums.NewDigest()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("UploadChunk: stream recv error: %v", err)
			return err
		}
		fragment := req.GetContent()
		if fragment == nil {
			return status.Error(codes.InvalidArgument, "Subsequent input messages must contain chunk content")
		}
		n, err := writer.Write(fragment)
		if err != nil {
			log.Errorf("UploadChunk: BlobStore write error: %v", err)
			return err
		}
		_, _ = digest.Write(fragment)
		written += n
		// TODO(yuryu): This is not suitable for unit tests until we make the value
		// configurable, or have a BlobStore mock.
		if written > chunkSizeLimit {
			err := status.Errorf(codes.ResourceExhausted, "UploadChunk: Received chunk size (%v) exceed the limit (%v)", written, chunkSizeLimit)
			log.Error(err)
			return err
		}
	}
	err = writer.Close()
	writer = nil
	if err != nil {
		log.Errorf("writer.Close() failed on chunk object %v: %v", chunk.ObjectPath(), err)
		return err
	}

	// Update the chunk size based on the actual bytes written
	chunk.Size = int32(written)
	chunk.Checksums = digest.Checksums()
	chunk.Timestamps.Update()

	if err := chunk.ValidateIfPresent(meta); err != nil {
		_ = s.deleteObjectOnExit(ctx, chunk.ObjectPath())
		log.Error(err)
		return err
	}

	// Delete all the related chunks with the same number, if any
	if err := s.deleteSameNumberChunks(ctx, chunk); err != nil {
		_ = s.deleteObjectOnExit(ctx, chunk.ObjectPath())
		log.Error(err)
		return err
	}

	if err := s.metaDB.InsertChunkRef(ctx, blob, chunk); err != nil {
		_ = s.deleteObjectOnExit(ctx, chunk.ObjectPath())
		log.Errorf("Failed to insert chunkref metadata (%v), blobref (%v): %v", chunk.Key, chunk.BlobRef, err)
		return err
	}
	return stream.SendAndClose(chunk.ToProto())
}

func (s *openSavesServer) CommitChunkedUpload(ctx context.Context, req *pb.CommitChunkedUploadRequest) (*pb.BlobMetadata, error) {
	blobKey, err := uuid.Parse(req.GetSessionId())
	if err != nil {
		log.Errorf("SessionId is not a valid UUID string: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "SessionId is not a valid UUID string: %v", err)
	}
	blob, err := s.metaDB.GetBlobRef(ctx, blobKey)
	if err != nil {
		log.Errorf("Cannot retrieve chunked blob metadata for session (%v): %v", blobKey, err)
		return nil, err
	}
	// The record in the request is optional
	var updateTo *record.Record
	storeKey := blob.StoreKey
	pbRecord := req.GetRecord()
	if pbRecord != nil {
		updateTo, err = record.FromProto(storeKey, pbRecord)
		if err != nil {
			log.Errorf("Invalid proto for store (%s), record (%s): %v", storeKey, pbRecord.GetKey(), err)
			return nil, status.Errorf(codes.InvalidArgument, "Invalid record proto: %v", err)
		}
	}
	var updatedRecord *record.Record
	if updateTo != nil {
		updatedRecord, _, err = s.metaDB.PromoteBlobRefWithRecordUpdater(ctx, blob, updateTo, func(r *record.Record) (*record.Record, error) {
			r.BlobSize = blob.Size
			r.ExternalBlob = blob.Key
			r.Chunked = blob.Chunked
			r.Timestamps.Update()
			r.OwnerID = updateTo.OwnerID
			r.Properties = updateTo.Properties
			r.Tags = updateTo.Tags
			r.OpaqueString = updateTo.OpaqueString
			// If the update request does not include a new ExpiresAt value
			// then here it will be time's zero value and thus it will be ignored when saving.
			// In other words, once expiration is set it cannot be removed via updates.
			r.ExpiresAt = updateTo.ExpiresAt
			return r, nil
		})
		if err != nil {
			log.Errorf("PromoteBlobRefWithRecordUpdater failed for object %v: %v", blob.ObjectPath(), err)
			// Do not delete the blob object here. Leave it to the garbage collector.
			return nil, err
		}
	} else {
		updatedRecord, _, err = s.metaDB.PromoteBlobRefToCurrent(ctx, blob)
		if err != nil {
			log.Errorf("PromoteBlobRefToCurrent failed for object %v: %v", blob.ObjectPath(), err)
			// Do not delete the blob object here. Leave it to the garbage collector.
			return nil, err
		}
	}
	s.cacheRecord(ctx, updatedRecord, req.GetHint())
	return blob.ToProto(), nil
}

func (s *openSavesServer) AbortChunkedUpload(ctx context.Context, req *pb.AbortChunkedUploadRequest) (*empty.Empty, error) {
	id, err := uuid.Parse(req.GetSessionId())
	if err != nil {
		log.Errorf("SessionId is not a valid UUID: %v", err)
		return new(empty.Empty), status.Errorf(codes.InvalidArgument, "SessionId is not a valid UUID: %v", err)
	}
	err = s.metaDB.MarkUncommittedBlobForDeletion(ctx, id)
	if err != nil {
		log.Errorf("AbortChunkedUpload failed for session (%v): %v", id, err)
	}
	return new(empty.Empty), err
}

func (s *openSavesServer) GetBlobChunk(req *pb.GetBlobChunkRequest, response pb.OpenSaves_GetBlobChunkServer) error {
	ctx := response.Context()

	chunk, err := s.metaDB.FindChunkRefByNumber(ctx, req.GetStoreKey(), req.GetRecordKey(), int32(req.GetChunkNumber()))
	if err != nil {
		log.Errorf("GetBlobChunk failed to get chunk metadata for store (%v), record (%v), number (%v): %v",
			req.GetStoreKey(), req.GetRecordKey(), req.GetChunkNumber(), err)
		return err
	}

	err = response.Send(&pb.GetBlobChunkResponse{
		Response: &pb.GetBlobChunkResponse_Metadata{
			Metadata: chunk.ToProto(),
		},
	})
	if err != nil {
		log.Errorf("Send failed for metadata: %v", err)
	}

	reader, err := s.blobStore.NewReader(ctx, chunk.ObjectPath())
	if err != nil {
		log.Errorf("GetBlobChunk failed to create a reader for chunk (%v): %v", chunk.ObjectPath(), err)
		return err
	}
	defer reader.Close()

	buf := make([]byte, streamBufferSize)
	sent := 0
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("GetBlobChunk: BlobStore Reader returned error for object (%v): %v", chunk.ObjectPath(), err)
			return err
		}
		err = response.Send(&pb.GetBlobChunkResponse{Response: &pb.GetBlobChunkResponse_Content{
			Content: buf[:n],
		}})
		if err != nil {
			log.Errorf("GetBlobChunk: Stream send error for object (%v): %v", chunk.ObjectPath(), err)
			return err
		}
		sent += n
	}
	if sent != int(chunk.Size) {
		log.Errorf("GetBlobChunk: Chunk size sent (%v) and stored in the metadata (%v) don't match.", sent, chunk.Size)
		return status.Errorf(codes.DataLoss,
			"GetBlobChunk: Chunk size sent (%v) and stored in the metadata (%v) don't match.", sent, chunk.Size)
	}
	return nil
}

func (s *openSavesServer) CompareAndSwap(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	log.Infof("CompareAndSwap: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	res := &pb.CompareAndSwapResponse{Updated: false}
	updatedRecord, err := s.metaDB.UpdateRecord(ctx, req.GetStoreKey(), req.GetRecordKey(),
		func(r *record.Record) (*record.Record, error) {
			property, ok := r.Properties[req.GetPropertyName()]
			if !ok {
				return nil, status.Errorf(codes.NotFound, "property (%v) was not found", req.GetPropertyName())
			}
			// Save the current property value.
			res.Value = property.ToProto()
			oldValue := record.NewPropertyValueFromProto(req.GetOldValue())
			value := record.NewPropertyValueFromProto(req.GetValue())

			if property.Type == oldValue.Type {
				switch property.Type {
				case pb.Property_BOOLEAN:
					if property.BooleanValue == oldValue.BooleanValue {
						res.Updated = true
					}
				case pb.Property_INTEGER:
					if property.IntegerValue == oldValue.IntegerValue {
						res.Updated = true
					}
				case pb.Property_STRING:
					if property.StringValue == oldValue.StringValue {
						res.Updated = true
					}
				}
			}

			if !res.GetUpdated() {
				// ErrNoUpdate aborts the transaction safely.
				return nil, metadb.ErrNoUpdate
			}
			r.Properties[req.GetPropertyName()] = value
			return r, nil
		})
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if res.GetUpdated() {
		s.cacheRecord(ctx, updatedRecord, req.GetHint())
	}
	return res, nil
}

// Called as updateIntCallback(property, value) and should return (updated, new value to set)
// Transaction is aborted if the returned bool is false and the returned int64 is discarded.
type updateIntCallback = func(property, value int64) (bool, int64)

func (s *openSavesServer) doAtomicUpdatePropertyInt(ctx context.Context, req *pb.AtomicIntRequest, callback updateIntCallback) (*pb.AtomicIntResponse, error) {
	res := &pb.AtomicIntResponse{
		Updated: false,
	}
	updatedRecord, err := s.metaDB.UpdateRecord(ctx, req.GetStoreKey(), req.GetRecordKey(),
		func(r *record.Record) (*record.Record, error) {
			property, ok := r.Properties[req.GetPropertyName()]
			if !ok {
				return nil, status.Errorf(codes.NotFound, "property (%v) was not found", req.GetPropertyName())
			}
			if property.Type != pb.Property_INTEGER {
				return nil, status.Errorf(codes.InvalidArgument, "the value type of property (%v) was not integer: %v",
					req.GetPropertyName(), property.Type.String())
			}
			// Save the old value.
			res.Value = property.IntegerValue
			updated, newValue := callback(property.IntegerValue, req.GetValue())
			if !updated {
				// ErrNoUpdate aborts the transaction safely.
				return nil, metadb.ErrNoUpdate
			}
			res.Updated = updated
			property.IntegerValue = newValue
			return r, nil
		})
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if res.GetUpdated() {
		s.cacheRecord(ctx, updatedRecord, req.GetHint())
	}
	return res, nil
}

func (s *openSavesServer) CompareAndSwapGreaterInt(ctx context.Context, req *pb.AtomicIntRequest) (*pb.AtomicIntResponse, error) {
	log.Infof("CompareAndSwapGreaterInt: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	return s.doAtomicUpdatePropertyInt(ctx, req, func(property, value int64) (bool, int64) {
		if value > property {
			return true, value
		}
		return false, property
	})
}

func (s *openSavesServer) CompareAndSwapLessInt(ctx context.Context, req *pb.AtomicIntRequest) (*pb.AtomicIntResponse, error) {
	log.Infof("CompareAndSwapLessInt: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	return s.doAtomicUpdatePropertyInt(ctx, req, func(property, value int64) (bool, int64) {
		if value < property {
			return true, value
		}
		return false, property
	})
}

func (s *openSavesServer) AtomicAddInt(ctx context.Context, req *pb.AtomicIntRequest) (*pb.AtomicIntResponse, error) {
	log.Infof("AtomicAddInt: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	return s.doAtomicUpdatePropertyInt(ctx, req, func(property, value int64) (bool, int64) {
		return true, property + value
	})
}

func (s *openSavesServer) AtomicSubInt(ctx context.Context, req *pb.AtomicIntRequest) (*pb.AtomicIntResponse, error) {
	log.Infof("AtomicSubInt: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	return s.doAtomicUpdatePropertyInt(ctx, req, func(property, value int64) (bool, int64) {
		return true, property - value
	})
}

// atomicIncCallback is called as (current property value, lower_bound, upper_bound) and should return
// the new property value.
type atomicIncCallback = func(value, lower, upper int64) int64

func (s *openSavesServer) doAtomicIncDec(ctx context.Context, req *pb.AtomicIncRequest, callback atomicIncCallback) (*pb.AtomicIntResponse, error) {
	res := &pb.AtomicIntResponse{
		Updated: true,
	}
	updatedRecord, err := s.metaDB.UpdateRecord(ctx, req.GetStoreKey(), req.GetRecordKey(),
		func(r *record.Record) (*record.Record, error) {
			property, ok := r.Properties[req.GetPropertyName()]
			if !ok {
				return nil, status.Errorf(codes.NotFound, "property (%v) was not found", req.GetPropertyName())
			}
			if property.Type != pb.Property_INTEGER {
				return nil, status.Errorf(codes.InvalidArgument, "the value type of property (%v) was not integer: %v",
					req.GetPropertyName(), property.Type.String())
			}
			// Save the old value.
			res.Value = property.IntegerValue
			property.IntegerValue = callback(property.IntegerValue, req.GetLowerBound(), req.GetUpperBound())
			return r, nil
		})
	if err != nil {
		log.Error(err)
		return nil, err
	}
	s.cacheRecord(ctx, updatedRecord, req.GetHint())
	return res, nil
}

func (s *openSavesServer) AtomicInc(ctx context.Context, req *pb.AtomicIncRequest) (*pb.AtomicIntResponse, error) {
	log.Infof("AtomicInc: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	return s.doAtomicIncDec(ctx, req, func(value, lower, upper int64) int64 {
		if value < upper {
			return value + 1
		}
		return lower
	})
}

func (s *openSavesServer) AtomicDec(ctx context.Context, req *pb.AtomicIncRequest) (*pb.AtomicIntResponse, error) {
	log.Infof("AtomicDec: store (%v), record (%v), property (%v)",
		req.GetStoreKey(), req.GetRecordKey(), req.GetPropertyName())
	return s.doAtomicIncDec(ctx, req, func(value, lower, upper int64) int64 {
		if lower < value {
			return value - 1
		}
		return upper
	})
}

func (s *openSavesServer) getRecordAndCache(ctx context.Context, storeKey, key string, hint *pb.Hint) (*record.Record, error) {
	if shouldCheckCache(hint) {
		r := new(record.Record)
		if err := s.cacheStore.Get(ctx, record.CacheKey(storeKey, key), r); err == nil {
			log.Debug("cache hit")
			return r, nil
		}
		log.Debug("cache miss")
	}

	r, err := s.metaDB.GetRecord(ctx, storeKey, key)
	if err != nil {
		log.Warnf("GetRecord failed for store (%s), record (%s): %v",
			storeKey, key, err)
		return nil, status.Convert(err).Err()
	}
	log.Tracef("Got record %+v", r)
	s.cacheRecord(ctx, r, hint)
	return r, nil
}

func (s *openSavesServer) cacheRecord(ctx context.Context, r *record.Record, hint *pb.Hint) error {
	var err error
	if shouldCache(hint) {
		if err = s.cacheStore.Set(ctx, r); err != nil {
			log.Warnf("failed to encode record for cache for store (%s), record(%s): %v",
				r.StoreKey, r.Key, err)
		}
	} else {
		if err = s.cacheStore.Delete(ctx, r.CacheKey()); err != nil {
			log.Errorf("failed to purge cache for store (%s), record (%s): %v",
				r.StoreKey, r.Key, err)
		}
	}
	return err
}

func (s *openSavesServer) getStoreAndCache(ctx context.Context, storeKey string) (*store.Store, error) {
	st := new(store.Store)
	if err := s.cacheStore.Get(ctx, store.CacheKey(storeKey), st); err == nil {
		log.Debug("store cache hit")
		return st, nil
	}
	log.Debug("store cache miss")

	str, err := s.metaDB.GetStore(ctx, storeKey)
	if err != nil {
		log.Warnf("GetStore failed for store (%s): %v",
			storeKey, err)
		return nil, status.Convert(err).Err()
	}
	s.storeCache(ctx, str)
	log.Tracef("Got store %+v", s)
	return str, nil
}

func (s *openSavesServer) storeCache(ctx context.Context, st *store.Store) error {
	var err error
	if err = s.cacheStore.Set(ctx, st); err != nil {
		log.Warnf("failed to encode record for cache for store (%s): %v", st.Key, err)
	}
	return err
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
