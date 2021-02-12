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

package main

import (
	"context"
	"flag"
	"log"

	"github.com/google/uuid"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"

	pb "github.com/googleforgames/open-saves/api"
)

var (
	address  = flag.String("address", "localhost:6000", "Address of Open Saves server")
	insecure = flag.Bool("insecure", false, "Dial grpc server insecurely")
)

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("localhost:6000"),
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()

	opts := defaultClientOptions()
	if *insecure {
		opts = append(opts, option.WithGRPCDialOption(grpc.WithInsecure()), option.WithoutAuthentication())
	}
	if *address != "" {
		opts = append(opts, option.WithEndpoint(*address))
	}

	connPool, err := gtransport.DialPool(ctx, opts...)
	if err != nil {
		log.Fatalf("got err dialing conn pool: %v", err)
	}

	c := pb.NewOpenSavesClient(connPool)

	store := &pb.Store{
		Key:     uuid.New().String(),
		Name:    "test",
		OwnerId: "test-user",
	}
	req := &pb.CreateStoreRequest{
		Store: store,
	}

	s, err := c.CreateStore(ctx, req)
	if err != nil {
		log.Fatalf("err creating store: %v", err)
	}
	log.Printf("successfully created store: %v", s)

	record := &pb.Record{
		Key:     "some-key",
		OwnerId: "some-id",
	}
	rec, err := c.CreateRecord(ctx, &pb.CreateRecordRequest{
		StoreKey: s.Key,
		Record:   record,
	})
	if err != nil {
		log.Fatalf("err creating record: %v", err)
	}

	log.Printf("created record: %v", rec)
	got, err := c.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: s.Key,
		Key:      record.Key,
	})
	if err != nil {
		log.Fatalf("err get record: %v", err)
	}
	log.Printf("got record: %v", got)
}
