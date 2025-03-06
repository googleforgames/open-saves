# Key Terms

This page describes concepts upon which Open Saves is built. For an overview of Open Saves, see [Overview](./docs/overview.md).

### Store

A [store](https://pkg.go.dev/github.com/googleforgames/open-saves@v0.2.0-beta1/api#Store) is an immutable container for a collection of related records. You create and receive stores using a unary RPC.

### Record

A [record](https://pkg.go.dev/github.com/googleforgames/open-saves@v0.2.0-beta1/api#Record) is a mutable data entity that belongs to a store. You create and retrieve records using a unary RPC.

A record can consist of the following:

* [Property](https://pkg.go.dev/github.com/googleforgames/open-saves@v0.2.0-beta1/api#Property): the field where typed data is stored as an integer, string, or boolean.

* [Blob](https://pkg.go.dev/github.com/googleforgames/open-saves@v0.2.0-beta1/api#BlobMetadata): a single large object used for storing images, videos, game state data, or other types of data in Cloud Storage. Blobs are created and retrieved by a streaming RPC (`Create/GetBlob`).

* [Chunked blob](https://pkg.go.dev/github.com/googleforgames/open-saves@v0.2.0-beta1/api#ChunkMetadata): a large blob split into smaller chunks that are uploaded in parallel.
To upload a chunked blob, call `CreateChunkedUploadRequest` with the chunk size and get a session ID to upload chunks via a streaming API. When downloading a chunked Blob, a streaming API is also used after you call `GetBlobChunkRequest`.

Blobs and chunked blobs are treated as separate types of data.

### Owner

You can specify the owner of a store or record using the `OwnerId` field.

### Tags

Tags are a list of strings associated with stores or records that you can use to organize and query your resources. You can edit the tags using `UpdateStore` or `UpdateRecord`.

### Hints

You can use [hints](https://pkg.go.dev/github.com/googleforgames/open-saves@v0.2.0-beta1/api#Hint) to indicate how you want the server to behave. For example, use cache hints to instruct the server to skip the cache when retrieving records or to not cache records.
You can also use blob hints to force data to be stored in Cloud Storage or force a blob to be stored in the metadata server. Blob and cache hints can be used in tandem, such as when you want to skip the cache when storing a record but also force the property as an inline blob.

### TTL
Records support automatic deletion handled by Datastore using the TTL feature (https://cloud.google.com/datastore/docs/ttl).
Records have the `ExpiresAt` field at the top level to represent the moment it can be collected, though it must be manually set in the Datastore instance.

Blobs inherit the `ExpiresAt` value from the records owing them, ensuring both are collected at the same time, though the TTL rule must be configured in the Datastore instance too.
