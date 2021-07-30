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

package utils

import (
	"context"

	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func CreateTempStore(ctx context.Context, conn *grpc.ClientConn) (*pb.Store, error) {
	client := pb.NewOpenSavesClient(conn)
	s, err := client.CreateStore(ctx, &pb.CreateStoreRequest{
		Store: &pb.Store{
			Key:  uuid.NewString(),
			Name: "load testing",
		},
	})
	if err != nil {
		log.Errorf("error creating store: %v", err)
		return nil, err
	}
	log.Printf("successfully created store: %v", s)
	return s, nil
}

func DeleteStore(ctx context.Context, conn *grpc.ClientConn, store *pb.Store) {
	client := pb.NewOpenSavesClient(conn)
	client.DeleteStore(ctx, &pb.DeleteStoreRequest{Key: store.Key})
}

func CreateTempRecord(ctx context.Context, conn *grpc.ClientConn, store *pb.Store) (*pb.Record, error) {
	client := pb.NewOpenSavesClient(conn)
	record, err := client.CreateRecord(ctx, &pb.CreateRecordRequest{StoreKey: store.GetKey(),
		Record: &pb.Record{
			Key: uuid.NewString(),
		},
	})
	if err != nil {
		log.Errorf("error creating record: %v", err)
		return nil, err
	}
	log.Infof("successfully created record: %v", record)
	return record, nil
}

func DeleteRecord(ctx context.Context, conn *grpc.ClientConn, store *pb.Store, record *pb.Record) {
	client := pb.NewOpenSavesClient(conn)
	client.DeleteRecord(ctx, &pb.DeleteRecordRequest{StoreKey: store.GetKey(), Key: record.GetKey()})
}
