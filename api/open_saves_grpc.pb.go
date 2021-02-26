// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package opensaves

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// OpenSavesClient is the client API for OpenSaves service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type OpenSavesClient interface {
	// CreateStore creates and returns a new store.
	CreateStore(ctx context.Context, in *CreateStoreRequest, opts ...grpc.CallOption) (*Store, error)
	// GetStore fetches store with the specified key.
	GetStore(ctx context.Context, in *GetStoreRequest, opts ...grpc.CallOption) (*Store, error)
	// ListStore returns stores matching the provided criteria.
	ListStores(ctx context.Context, in *ListStoresRequest, opts ...grpc.CallOption) (*ListStoresResponse, error)
	// DeleteStore deletes a single store with the specified key.
	DeleteStore(ctx context.Context, in *DeleteStoreRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// CreateRecord creates a new record. This returns an error if the
	// specified key already exists.
	CreateRecord(ctx context.Context, in *CreateRecordRequest, opts ...grpc.CallOption) (*Record, error)
	// GetRecord returns a record with the specified key.
	GetRecord(ctx context.Context, in *GetRecordRequest, opts ...grpc.CallOption) (*Record, error)
	// QueryRecords performs a query and returns matching records.
	QueryRecords(ctx context.Context, in *QueryRecordsRequest, opts ...grpc.CallOption) (*QueryRecordsResponse, error)
	// UpdateRecord updates an existing record. This returns an error and
	// does not create a new record if the key doesn't exist.
	UpdateRecord(ctx context.Context, in *UpdateRecordRequest, opts ...grpc.CallOption) (*Record, error)
	// DeleteRecord deletes a single record with the specified key.
	DeleteRecord(ctx context.Context, in *DeleteRecordRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// CreateBlob adds a new blob to a record.
	CreateBlob(ctx context.Context, opts ...grpc.CallOption) (OpenSaves_CreateBlobClient, error)
	// GetBlob retrieves a blob object in a record.
	GetBlob(ctx context.Context, in *GetBlobRequest, opts ...grpc.CallOption) (OpenSaves_GetBlobClient, error)
	// DeleteBlob removes an blob object from a record.
	DeleteBlob(ctx context.Context, in *DeleteBlobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Ping returns the same string provided by the client.
	// The string is optional and the server returns an empty string if omitted.
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
}

type openSavesClient struct {
	cc grpc.ClientConnInterface
}

func NewOpenSavesClient(cc grpc.ClientConnInterface) OpenSavesClient {
	return &openSavesClient{cc}
}

func (c *openSavesClient) CreateStore(ctx context.Context, in *CreateStoreRequest, opts ...grpc.CallOption) (*Store, error) {
	out := new(Store)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/CreateStore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) GetStore(ctx context.Context, in *GetStoreRequest, opts ...grpc.CallOption) (*Store, error) {
	out := new(Store)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/GetStore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) ListStores(ctx context.Context, in *ListStoresRequest, opts ...grpc.CallOption) (*ListStoresResponse, error) {
	out := new(ListStoresResponse)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/ListStores", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) DeleteStore(ctx context.Context, in *DeleteStoreRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/DeleteStore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) CreateRecord(ctx context.Context, in *CreateRecordRequest, opts ...grpc.CallOption) (*Record, error) {
	out := new(Record)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/CreateRecord", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) GetRecord(ctx context.Context, in *GetRecordRequest, opts ...grpc.CallOption) (*Record, error) {
	out := new(Record)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/GetRecord", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) QueryRecords(ctx context.Context, in *QueryRecordsRequest, opts ...grpc.CallOption) (*QueryRecordsResponse, error) {
	out := new(QueryRecordsResponse)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/QueryRecords", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) UpdateRecord(ctx context.Context, in *UpdateRecordRequest, opts ...grpc.CallOption) (*Record, error) {
	out := new(Record)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/UpdateRecord", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) DeleteRecord(ctx context.Context, in *DeleteRecordRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/DeleteRecord", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) CreateBlob(ctx context.Context, opts ...grpc.CallOption) (OpenSaves_CreateBlobClient, error) {
	stream, err := c.cc.NewStream(ctx, &OpenSaves_ServiceDesc.Streams[0], "/opensaves.OpenSaves/CreateBlob", opts...)
	if err != nil {
		return nil, err
	}
	x := &openSavesCreateBlobClient{stream}
	return x, nil
}

type OpenSaves_CreateBlobClient interface {
	Send(*CreateBlobRequest) error
	CloseAndRecv() (*BlobMetadata, error)
	grpc.ClientStream
}

type openSavesCreateBlobClient struct {
	grpc.ClientStream
}

func (x *openSavesCreateBlobClient) Send(m *CreateBlobRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *openSavesCreateBlobClient) CloseAndRecv() (*BlobMetadata, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(BlobMetadata)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *openSavesClient) GetBlob(ctx context.Context, in *GetBlobRequest, opts ...grpc.CallOption) (OpenSaves_GetBlobClient, error) {
	stream, err := c.cc.NewStream(ctx, &OpenSaves_ServiceDesc.Streams[1], "/opensaves.OpenSaves/GetBlob", opts...)
	if err != nil {
		return nil, err
	}
	x := &openSavesGetBlobClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type OpenSaves_GetBlobClient interface {
	Recv() (*GetBlobResponse, error)
	grpc.ClientStream
}

type openSavesGetBlobClient struct {
	grpc.ClientStream
}

func (x *openSavesGetBlobClient) Recv() (*GetBlobResponse, error) {
	m := new(GetBlobResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *openSavesClient) DeleteBlob(ctx context.Context, in *DeleteBlobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/DeleteBlob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *openSavesClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, "/opensaves.OpenSaves/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// OpenSavesServer is the server API for OpenSaves service.
// All implementations must embed UnimplementedOpenSavesServer
// for forward compatibility
type OpenSavesServer interface {
	// CreateStore creates and returns a new store.
	CreateStore(context.Context, *CreateStoreRequest) (*Store, error)
	// GetStore fetches store with the specified key.
	GetStore(context.Context, *GetStoreRequest) (*Store, error)
	// ListStore returns stores matching the provided criteria.
	ListStores(context.Context, *ListStoresRequest) (*ListStoresResponse, error)
	// DeleteStore deletes a single store with the specified key.
	DeleteStore(context.Context, *DeleteStoreRequest) (*emptypb.Empty, error)
	// CreateRecord creates a new record. This returns an error if the
	// specified key already exists.
	CreateRecord(context.Context, *CreateRecordRequest) (*Record, error)
	// GetRecord returns a record with the specified key.
	GetRecord(context.Context, *GetRecordRequest) (*Record, error)
	// QueryRecords performs a query and returns matching records.
	QueryRecords(context.Context, *QueryRecordsRequest) (*QueryRecordsResponse, error)
	// UpdateRecord updates an existing record. This returns an error and
	// does not create a new record if the key doesn't exist.
	UpdateRecord(context.Context, *UpdateRecordRequest) (*Record, error)
	// DeleteRecord deletes a single record with the specified key.
	DeleteRecord(context.Context, *DeleteRecordRequest) (*emptypb.Empty, error)
	// CreateBlob adds a new blob to a record.
	CreateBlob(OpenSaves_CreateBlobServer) error
	// GetBlob retrieves a blob object in a record.
	GetBlob(*GetBlobRequest, OpenSaves_GetBlobServer) error
	// DeleteBlob removes an blob object from a record.
	DeleteBlob(context.Context, *DeleteBlobRequest) (*emptypb.Empty, error)
	// Ping returns the same string provided by the client.
	// The string is optional and the server returns an empty string if omitted.
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	mustEmbedUnimplementedOpenSavesServer()
}

// UnimplementedOpenSavesServer must be embedded to have forward compatible implementations.
type UnimplementedOpenSavesServer struct {
}

func (UnimplementedOpenSavesServer) CreateStore(context.Context, *CreateStoreRequest) (*Store, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateStore not implemented")
}
func (UnimplementedOpenSavesServer) GetStore(context.Context, *GetStoreRequest) (*Store, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStore not implemented")
}
func (UnimplementedOpenSavesServer) ListStores(context.Context, *ListStoresRequest) (*ListStoresResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListStores not implemented")
}
func (UnimplementedOpenSavesServer) DeleteStore(context.Context, *DeleteStoreRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteStore not implemented")
}
func (UnimplementedOpenSavesServer) CreateRecord(context.Context, *CreateRecordRequest) (*Record, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateRecord not implemented")
}
func (UnimplementedOpenSavesServer) GetRecord(context.Context, *GetRecordRequest) (*Record, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRecord not implemented")
}
func (UnimplementedOpenSavesServer) QueryRecords(context.Context, *QueryRecordsRequest) (*QueryRecordsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryRecords not implemented")
}
func (UnimplementedOpenSavesServer) UpdateRecord(context.Context, *UpdateRecordRequest) (*Record, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateRecord not implemented")
}
func (UnimplementedOpenSavesServer) DeleteRecord(context.Context, *DeleteRecordRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteRecord not implemented")
}
func (UnimplementedOpenSavesServer) CreateBlob(OpenSaves_CreateBlobServer) error {
	return status.Errorf(codes.Unimplemented, "method CreateBlob not implemented")
}
func (UnimplementedOpenSavesServer) GetBlob(*GetBlobRequest, OpenSaves_GetBlobServer) error {
	return status.Errorf(codes.Unimplemented, "method GetBlob not implemented")
}
func (UnimplementedOpenSavesServer) DeleteBlob(context.Context, *DeleteBlobRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteBlob not implemented")
}
func (UnimplementedOpenSavesServer) Ping(context.Context, *PingRequest) (*PingResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedOpenSavesServer) mustEmbedUnimplementedOpenSavesServer() {}

// UnsafeOpenSavesServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to OpenSavesServer will
// result in compilation errors.
type UnsafeOpenSavesServer interface {
	mustEmbedUnimplementedOpenSavesServer()
}

func RegisterOpenSavesServer(s grpc.ServiceRegistrar, srv OpenSavesServer) {
	s.RegisterService(&OpenSaves_ServiceDesc, srv)
}

func _OpenSaves_CreateStore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateStoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).CreateStore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/CreateStore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).CreateStore(ctx, req.(*CreateStoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_GetStore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetStoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).GetStore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/GetStore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).GetStore(ctx, req.(*GetStoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_ListStores_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListStoresRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).ListStores(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/ListStores",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).ListStores(ctx, req.(*ListStoresRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_DeleteStore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteStoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).DeleteStore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/DeleteStore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).DeleteStore(ctx, req.(*DeleteStoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_CreateRecord_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateRecordRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).CreateRecord(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/CreateRecord",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).CreateRecord(ctx, req.(*CreateRecordRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_GetRecord_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRecordRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).GetRecord(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/GetRecord",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).GetRecord(ctx, req.(*GetRecordRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_QueryRecords_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryRecordsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).QueryRecords(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/QueryRecords",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).QueryRecords(ctx, req.(*QueryRecordsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_UpdateRecord_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateRecordRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).UpdateRecord(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/UpdateRecord",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).UpdateRecord(ctx, req.(*UpdateRecordRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_DeleteRecord_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRecordRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).DeleteRecord(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/DeleteRecord",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).DeleteRecord(ctx, req.(*DeleteRecordRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_CreateBlob_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(OpenSavesServer).CreateBlob(&openSavesCreateBlobServer{stream})
}

type OpenSaves_CreateBlobServer interface {
	SendAndClose(*BlobMetadata) error
	Recv() (*CreateBlobRequest, error)
	grpc.ServerStream
}

type openSavesCreateBlobServer struct {
	grpc.ServerStream
}

func (x *openSavesCreateBlobServer) SendAndClose(m *BlobMetadata) error {
	return x.ServerStream.SendMsg(m)
}

func (x *openSavesCreateBlobServer) Recv() (*CreateBlobRequest, error) {
	m := new(CreateBlobRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _OpenSaves_GetBlob_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetBlobRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OpenSavesServer).GetBlob(m, &openSavesGetBlobServer{stream})
}

type OpenSaves_GetBlobServer interface {
	Send(*GetBlobResponse) error
	grpc.ServerStream
}

type openSavesGetBlobServer struct {
	grpc.ServerStream
}

func (x *openSavesGetBlobServer) Send(m *GetBlobResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _OpenSaves_DeleteBlob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteBlobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).DeleteBlob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/DeleteBlob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).DeleteBlob(ctx, req.(*DeleteBlobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OpenSaves_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OpenSavesServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opensaves.OpenSaves/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OpenSavesServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// OpenSaves_ServiceDesc is the grpc.ServiceDesc for OpenSaves service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var OpenSaves_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "opensaves.OpenSaves",
	HandlerType: (*OpenSavesServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateStore",
			Handler:    _OpenSaves_CreateStore_Handler,
		},
		{
			MethodName: "GetStore",
			Handler:    _OpenSaves_GetStore_Handler,
		},
		{
			MethodName: "ListStores",
			Handler:    _OpenSaves_ListStores_Handler,
		},
		{
			MethodName: "DeleteStore",
			Handler:    _OpenSaves_DeleteStore_Handler,
		},
		{
			MethodName: "CreateRecord",
			Handler:    _OpenSaves_CreateRecord_Handler,
		},
		{
			MethodName: "GetRecord",
			Handler:    _OpenSaves_GetRecord_Handler,
		},
		{
			MethodName: "QueryRecords",
			Handler:    _OpenSaves_QueryRecords_Handler,
		},
		{
			MethodName: "UpdateRecord",
			Handler:    _OpenSaves_UpdateRecord_Handler,
		},
		{
			MethodName: "DeleteRecord",
			Handler:    _OpenSaves_DeleteRecord_Handler,
		},
		{
			MethodName: "DeleteBlob",
			Handler:    _OpenSaves_DeleteBlob_Handler,
		},
		{
			MethodName: "Ping",
			Handler:    _OpenSaves_Ping_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "CreateBlob",
			Handler:       _OpenSaves_CreateBlob_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "GetBlob",
			Handler:       _OpenSaves_GetBlob_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "api/open_saves.proto",
}
