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
	"bytes"
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
		Name:    "user-store",
		OwnerId: "admin1",
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
		Key:     "user-1234",
		OwnerId: "admin1",
		Properties: map[string]*pb.Property{
			"username": {
				Type: pb.Property_STRING,
				Value: &pb.Property_StringValue{
					StringValue: "",
				},
			},
			""
		},
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

	record.Key = "inventory-data-1234"
	record.Properties = nil
	rec2, err := c.CreateRecord(ctx, &pb.CreateRecordRequest{
		StoreKey: s.Key,
		Record:   record,
	})
	if err != nil {
		log.Fatalf("err creating record2: %v", err)
	}
	log.Printf("created record: %v", rec2)

	got, err = c.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: s.Key,
		Key:      rec2.Key,
	})
	if err != nil {
		log.Fatalf("err get record: %v", err)
	}
	log.Printf("got record: %v", got)

	video := bytes.Repeat([]byte{'A'}, 64*1000*1000)
	err := createBlob(ctx, c, s.Key rec2.Key, &video)
	if err != nil {
		log.Fatalf("got error creating blob: %v", err)
	}
}


func createBlob(ctx context.Context, c *pb.OpenSavesClient, storeKey, recordKey string, blob *byte[]) error {
	cbc, err := c.CreateBlob(ctx)
	if err != nil {
		return err
	}

	err = cbc.Send(&pb.CreateBlobRequest{
		Request: &pb.CreateBlobRequest_Metadata{
			Metadata: &pb.BlobMetadata{
				StoreKey:  s.Key,
				RecordKey: rec2.Key,
				Size:      int64(len(video)),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateBlobClient.Send failed on sending metadata: %w", err)
	}

	sent := 0
	streamBufferSize := 1 * 1024 // 1 KiB
	for {
		if sent >= len(content) {
			log.Printf("finished sending blob\n")
			cbc.CloseSend()
			break
		}
		toSend := streamBufferSize
		if toSend > len(content)-sent {
			toSend = len(content) - sent
		}
		err = cbc.Send(&pb.CreateBlobRequest{
			Request: &pb.CreateBlobRequest_Content{
				Content: content[sent : sent+toSend],
			},
		})
		if err != nil {
			return fmt.Errorf("CreateBlobClient.Send failed on sending content: %w", err)
		}
		sent += toSend
	}
	log.Printf("sent %d bytes for blob on stream\n", sent)
}