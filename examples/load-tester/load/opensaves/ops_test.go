package opensaves

import (
	"context"
	"net"
	"testing"

	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/app/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	testProject    = "triton-for-games-dev"
	testBucket     = "gs://triton-integration"
	testBufferSize = 1024 * 1024
	testCacheAddr  = "localhost:6379"
)

func getOpenSavesServer(ctx context.Context, t *testing.T, cloud string) (pb.OpenSavesServer, *bufconn.Listener) {
	t.Helper()

	impl, err := server.NewOpenSavesServer(ctx, cloud, testProject, testBucket, testCacheAddr)
	if err != nil {
		t.Fatalf("Failed to create a new Open Saves server instance: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterOpenSavesServer(server, impl)
	listener := bufconn.Listen(testBufferSize)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("Server exited with error: %v", err)
		}
	}()
	t.Cleanup(func() { server.Stop() })
	return impl, listener
}

func getTestClient(ctx context.Context, t *testing.T, cloud string) (*grpc.ClientConn, pb.OpenSavesClient) {
	t.Helper()

	_, listener := getOpenSavesServer(ctx, t, cloud)
	conn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(
		func(_ context.Context, _ string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("Failed to create a gRPC connection: %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	client := pb.NewOpenSavesClient(conn)
	return conn, client
}

func TestOps_TempStoreRecord(t *testing.T) {
	ctx := context.Background()
	conn, client := getTestClient(ctx, t, "gcp")
	store, err := CreateTempStore(ctx, conn)

	require.NoError(t, err)
	t.Cleanup(func() {
		client.DeleteStore(ctx, &pb.DeleteStoreRequest{Key: store.GetKey()})
	})
	aStore, err := client.GetStore(ctx, &pb.GetStoreRequest{Key: store.GetKey()})
	if assert.NoError(t, err) {
		assert.Equal(t, store.GetKey(), aStore.GetKey())
	}

	record, err := CreateTempRecord(ctx, conn, store)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.DeleteRecord(ctx, &pb.DeleteRecordRequest{Key: record.GetKey()})
	})
	aRecord, err := client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.GetKey(), Key: record.GetKey(),
	})
	if assert.NoError(t, err) {
		assert.Equal(t, record.GetKey(), aRecord.GetKey())
	}

	DeleteRecord(ctx, conn, store, record)
	_, err = client.GetRecord(ctx, &pb.GetRecordRequest{
		StoreKey: store.GetKey(), Key: record.GetKey(),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	DeleteStore(ctx, conn, store)
	_, err = client.GetStore(ctx, &pb.GetStoreRequest{
		Key: store.GetKey(),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
