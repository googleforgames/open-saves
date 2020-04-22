package main

import (
	"context"
	"log"

	tritonpb "github.com/googleforgames/triton/api"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
)

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("localhost:6000"),
		option.WithGRPCDialOption(grpc.WithInsecure()),
		option.WithoutAuthentication(),
		// 	option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
		// 		grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func main() {
	ctx := context.Background()

	opts := defaultClientOptions()
	connPool, err := gtransport.DialPool(ctx, opts...)
	if err != nil {
		log.Fatalf("got err dialing conn pool: %v", err)
	}

	c := tritonpb.NewTritonClient(connPool)

	req := &tritonpb.CreateStoreRequest{
		Name: "test",
	}

	s, err := c.CreateStore(ctx, req)
	if err != nil {
		log.Fatalf("err creating store: %v", err)
	}
	log.Printf("successfully created store: %v", s)
}
