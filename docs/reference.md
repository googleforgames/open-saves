# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [api/open_saves.proto](#api_open_saves-proto)
    - [AbortChunkedUploadRequest](#opensaves-AbortChunkedUploadRequest)
    - [AtomicIncRequest](#opensaves-AtomicIncRequest)
    - [AtomicIntRequest](#opensaves-AtomicIntRequest)
    - [AtomicIntResponse](#opensaves-AtomicIntResponse)
    - [BlobMetadata](#opensaves-BlobMetadata)
    - [ChunkMetadata](#opensaves-ChunkMetadata)
    - [CommitChunkedUploadRequest](#opensaves-CommitChunkedUploadRequest)
    - [CompareAndSwapRequest](#opensaves-CompareAndSwapRequest)
    - [CompareAndSwapResponse](#opensaves-CompareAndSwapResponse)
    - [CreateBlobRequest](#opensaves-CreateBlobRequest)
    - [CreateChunkedBlobRequest](#opensaves-CreateChunkedBlobRequest)
    - [CreateChunkedBlobResponse](#opensaves-CreateChunkedBlobResponse)
    - [CreateRecordRequest](#opensaves-CreateRecordRequest)
    - [CreateStoreRequest](#opensaves-CreateStoreRequest)
    - [DeleteBlobRequest](#opensaves-DeleteBlobRequest)
    - [DeleteRecordRequest](#opensaves-DeleteRecordRequest)
    - [DeleteStoreRequest](#opensaves-DeleteStoreRequest)
    - [GetBlobChunkRequest](#opensaves-GetBlobChunkRequest)
    - [GetBlobChunkResponse](#opensaves-GetBlobChunkResponse)
    - [GetBlobRequest](#opensaves-GetBlobRequest)
    - [GetBlobResponse](#opensaves-GetBlobResponse)
    - [GetRecordRequest](#opensaves-GetRecordRequest)
    - [GetStoreRequest](#opensaves-GetStoreRequest)
    - [Hint](#opensaves-Hint)
    - [ListStoresRequest](#opensaves-ListStoresRequest)
    - [ListStoresResponse](#opensaves-ListStoresResponse)
    - [PingRequest](#opensaves-PingRequest)
    - [PingResponse](#opensaves-PingResponse)
    - [Property](#opensaves-Property)
    - [QueryFilter](#opensaves-QueryFilter)
    - [QueryRecordsRequest](#opensaves-QueryRecordsRequest)
    - [QueryRecordsResponse](#opensaves-QueryRecordsResponse)
    - [Record](#opensaves-Record)
    - [Record.PropertiesEntry](#opensaves-Record-PropertiesEntry)
    - [SortOrder](#opensaves-SortOrder)
    - [Store](#opensaves-Store)
    - [UpdateRecordRequest](#opensaves-UpdateRecordRequest)
    - [UploadChunkRequest](#opensaves-UploadChunkRequest)
  
    - [FilterOperator](#opensaves-FilterOperator)
    - [Property.Type](#opensaves-Property-Type)
    - [SortOrder.Direction](#opensaves-SortOrder-Direction)
    - [SortOrder.Property](#opensaves-SortOrder-Property)
  
    - [OpenSaves](#opensaves-OpenSaves)
  
- [Scalar Value Types](#scalar-value-types)



<a name="api_open_saves-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## api/open_saves.proto



<a name="opensaves-AbortChunkedUploadRequest"></a>

### AbortChunkedUploadRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [string](#string) |  | session_id is the ID of a chunked upload session to abort. |






<a name="opensaves-AtomicIncRequest"></a>

### AtomicIncRequest
AtomicIncRequest is used by atomic increment/decrement methods.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record that the property belongs to. |
| property_name | [string](#string) |  | The name of the integer property to perform the operation to. |
| lower_bound | [int64](#int64) |  | lower_bound is the lower bound of the interval. |
| upper_bound | [int64](#int64) |  | upper_bound is the upper bound of the interval. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-AtomicIntRequest"></a>

### AtomicIntRequest
AtomicIntRequest is used by atomic arithmetic methods (CompareAndSwap{Greater,Less}Int,
Atomic{Add,Sub}Int).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record that the property belongs to. |
| property_name | [string](#string) |  | The name of the integer property to perform the operation to. |
| value | [int64](#int64) |  | value is the operand for each method (see method descriptions). |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-AtomicIntResponse"></a>

### AtomicIntResponse
AtomicIntResponse indicates whether an atomic operation has updated a property
and includes the old value of the property.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updated | [bool](#bool) |  | updated indicates that the condition is satisfied and the property has been updated. |
| value | [int64](#int64) |  | value is the property value before the atomic operation. |






<a name="opensaves-BlobMetadata"></a>

### BlobMetadata
BlobMetadata contains necessary metadata
when creating (uploading) a new blob object.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | store_key is the key of the store that the record belongs to. |
| record_key | [string](#string) |  | record_key is the key of the record the new blob object belongs to. |
| size | [int64](#int64) |  | size is the byte length of the object. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints (write only). |
| md5 | [bytes](#bytes) |  | md5 is the MD5 hash of the blob content. If supplied for uploads, the server validates the content using the hash value. For downloads, the server returns the stored hash value of the blob content. The length of the hash value is 0 (not present) or 16 (present) bytes. |
| crc32c | [uint32](#uint32) |  | crc32c is the CRC32C checksum of the blob content. Specifically, it uses the Castagnoli polynomial. https://pkg.go.dev/hash/crc32#pkg-constants If supplied for uploads, the server validates the content using the checksum. For downloads, the server returns the checksum of the blob content. Open Saves provides both MD5 and CRC32C because CRC32C is often used by Cloud object storage services. |
| has_crc32c | [bool](#bool) |  | has_crc32c indicates if crc32c is present. |






<a name="opensaves-ChunkMetadata"></a>

### ChunkMetadata



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [string](#string) |  | session_id is the ID of a chunk upload session. Not used for downloads. |
| number | [int64](#int64) |  | number is the number |
| size | [int64](#int64) |  | size is a byte size of the chunk. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints (write only). |
| md5 | [bytes](#bytes) |  | md5 is the MD5 hash of the chunk content. If supplied for uploads, the server validates the content using the hash value. For downloads, the server returns the stored hash value of the chunk content. The length of the hash value is 0 (not present) or 16 (present) bytes. |
| crc32c | [uint32](#uint32) |  | crc32c is the CRC32C checksum of the chunk content. Specifically, it uses the Castagnoli polynomial. https://pkg.go.dev/hash/crc32#pkg-constants If supplied for uploads, the server validates the content using the checksum. For downloads, the server returns the checksum of the chunk content. Open Saves provides both MD5 and CRC32C because CRC32C is often used by Cloud object storage services. |
| has_crc32c | [bool](#bool) |  | has_crc32c indicates if crc32c is present. |






<a name="opensaves-CommitChunkedUploadRequest"></a>

### CommitChunkedUploadRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [string](#string) |  | session_id is the ID of a chunked upload session to commit. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-CompareAndSwapRequest"></a>

### CompareAndSwapRequest
CompareAndSwapRequest is used by the CompareAndSwap methods to atomically update a property value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record that the property belongs to. |
| property_name | [string](#string) |  | The name of the integer property to perform the operation to. |
| value | [Property](#opensaves-Property) |  | value is the new property value to set. |
| old_value | [Property](#opensaves-Property) |  | old_value is compared against the current property value. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-CompareAndSwapResponse"></a>

### CompareAndSwapResponse
CompareAndSwapResponse is returned by CompareAndSwap and indicates whether the request
has updated the property value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updated | [bool](#bool) |  | updated indicates that the condition is satisfied and the property has been updated. |
| value | [Property](#opensaves-Property) |  | value is the old value of the property. |






<a name="opensaves-CreateBlobRequest"></a>

### CreateBlobRequest
CreateBlobRequest is used for CreateBlob, a client-streaming method.
The first message should contain metadata, and the subsequent messages
should contain content.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [BlobMetadata](#opensaves-BlobMetadata) |  | metadata is the metadata used to initialize the blob object with. |
| content | [bytes](#bytes) |  | content is the binary content of the blob object. |






<a name="opensaves-CreateChunkedBlobRequest"></a>

### CreateChunkedBlobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | store_key is the key of the store that the record belongs to. |
| record_key | [string](#string) |  | record_key is the key of the record the new blob object belongs to. |
| chunk_size | [int64](#int64) |  | Size of each chunk |






<a name="opensaves-CreateChunkedBlobResponse"></a>

### CreateChunkedBlobResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [string](#string) |  | session_id is a chunked |






<a name="opensaves-CreateRecordRequest"></a>

### CreateRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store to create the record into. |
| record | [Record](#opensaves-Record) |  | The record to create. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-CreateStoreRequest"></a>

### CreateStoreRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store | [Store](#opensaves-Store) |  | Store to create. |






<a name="opensaves-DeleteBlobRequest"></a>

### DeleteBlobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record to delete a blob from. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-DeleteRecordRequest"></a>

### DeleteRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| key | [string](#string) |  | The key of the record to delete. |






<a name="opensaves-DeleteStoreRequest"></a>

### DeleteStoreRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the store to delete. |






<a name="opensaves-GetBlobChunkRequest"></a>

### GetBlobChunkRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record to get. |
| chunk_number | [int64](#int64) |  | chunk_number is the number of the chunk to get. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-GetBlobChunkResponse"></a>

### GetBlobChunkResponse
GetBlobChunkResponse is a server-streaming response to return metadata and
content of a chunked blob object. The first message contains metadata and the
subsequent messages contain the binary data of the chunk inthe content field.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [ChunkMetadata](#opensaves-ChunkMetadata) |  |  |
| content | [bytes](#bytes) |  |  |






<a name="opensaves-GetBlobRequest"></a>

### GetBlobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record to get. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-GetBlobResponse"></a>

### GetBlobResponse
GetBlobResponse is a server-streaming response to return metadata and
content of a blob object. The first message contains metadata and the
subsequent messages contain the rest of the binary blob in the content
field.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [BlobMetadata](#opensaves-BlobMetadata) |  | metadata is the metadata used to initialize the blob object with. |
| content | [bytes](#bytes) |  | content is the binary content of the blob object. |






<a name="opensaves-GetRecordRequest"></a>

### GetRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| key | [string](#string) |  | The key of the record to get. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-GetStoreRequest"></a>

### GetStoreRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the store to retrieve. |






<a name="opensaves-Hint"></a>

### Hint
Performance optimization hints for the server.
The server may silently ignore the hints when not feasible.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| do_not_cache | [bool](#bool) |  | If true, do not cache the record for future requests. |
| skip_cache | [bool](#bool) |  | If true, skip the cache check and always check the metadata server. If false, allow file size to determine cache checks. |
| force_blob_store | [bool](#bool) |  | If true, always store the blob in blob storage, rather than in the metadata server. If false, allow file size to determine where to store the blob. |
| force_inline_blob | [bool](#bool) |  | Tells the server to not use blob storage. Always store the blob into the metadata entity. The server will return an error if the blob is too large. The exact size limit depends on the backend implementation. |






<a name="opensaves-ListStoresRequest"></a>

### ListStoresRequest
ListStoresRequest contains conditions to filter stores.
Multiple conditions are AND&#39;ed together.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Store name. This is an exact match. |
| tags | [string](#string) | repeated | List of tags |
| owner_id | [string](#string) |  | owner_id is the owner of stores, represented as an external user ID. |






<a name="opensaves-ListStoresResponse"></a>

### ListStoresResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stores | [Store](#opensaves-Store) | repeated | List of stores that match the provided criteria. |






<a name="opensaves-PingRequest"></a>

### PingRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ping | [string](#string) |  | An optional string to send with the ping message. |






<a name="opensaves-PingResponse"></a>

### PingResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pong | [string](#string) |  | An optional string that is copied from PingRequest. |






<a name="opensaves-Property"></a>

### Property
Property represents typed data in Open Saves.
This is limited to one type per key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Property.Type](#opensaves-Property-Type) |  |  |
| integer_value | [int64](#int64) |  |  |
| string_value | [string](#string) |  |  |
| boolean_value | [bool](#bool) |  |  |






<a name="opensaves-QueryFilter"></a>

### QueryFilter
QueryFilter is a filtering condition when querying records.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property_name | [string](#string) |  | Propety name of the filter. |
| operator | [FilterOperator](#opensaves-FilterOperator) |  | Operator of the filter. |
| value | [Property](#opensaves-Property) |  | Value to compare the property with. |






<a name="opensaves-QueryRecordsRequest"></a>

### QueryRecordsRequest
QueryRecordsRequest contains conditions to search particular records.
Multiple conditions are AND&#39;ed together.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | store_key is the primary key of the store. Optional and the method queries all stores when omitted. |
| filters | [QueryFilter](#opensaves-QueryFilter) | repeated | List of filters |
| tags | [string](#string) | repeated | List of tags |
| owner_id | [string](#string) |  | owner_id is the owner of records, represented as an external user ID. |
| sort_orders | [SortOrder](#opensaves-SortOrder) | repeated | List of sort orders to return records. These SortOrders are applied in sequence. |
| limit | [int32](#int32) |  | the limit of the number of records to return. |






<a name="opensaves-QueryRecordsResponse"></a>

### QueryRecordsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [Record](#opensaves-Record) | repeated | List of records that match the criteria. |
| store_keys | [string](#string) | repeated | List of store keys that each of the records belongs to. The order of keys is the same as the records field, e.g. store_keys[0] is the store for records[0], and so on. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. Query caching is not supported at the moment |






<a name="opensaves-Record"></a>

### Record
Record represents each entity in the Open Saves database.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | key is the user defined primary key. It is recommended to use uniformally distributed key (e.g. UUID) rather than monotonically increasing values for performance reasons. See https://cloud.google.com/datastore/docs/best-practices#keys for details. |
| blob_size | [int64](#int64) |  | Byte length of the blob (read-only) |
| properties | [Record.PropertiesEntry](#opensaves-Record-PropertiesEntry) | repeated | Typed values that are indexed and searchable. The number of properties allowed in a record is backend dependent. |
| owner_id | [string](#string) |  | owner_id is the owner of the record, represented as an external user ID. Open Saves doesn&#39;t maintain list of valid users and it is the responsibility of the client to keep track of user IDs. |
| tags | [string](#string) | repeated | tags are queryable strings to categorize records Examples: &#34;player:1&#34;, &#34;system&#34;, &#34;inventory:xxx&#34; |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | created_at is the point in time in UTC when the Record is created on the Open Saves server. It is managed and set by the server. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at is the point in time in UTC when the Record is updated on the Open Saves server. It is managed by the server and updated every time the Record is updated. |
| chunked | [bool](#bool) |  | chunked is set true if the attached blob is chunked, otherwise false. |
| chunk_count | [int64](#int64) |  | The number of chunks in the associated chunked blob. |
| opaque_string | [string](#string) |  | Opaque string where you can store any utf-8 string (e.g. JSON) that is too big and does not fit in the properties. This will not be indexed or queryable. The current size limit is 32KiB. |
| signature | [bytes](#bytes) |  | Signature is a server-generated unique UUID that is updated each time the server updates the record. The server returns the current signature for read requests. The client may optionally populate this field and send update requests, and the server will check the value against the latest value and abort the request if they don&#39;t match. |






<a name="opensaves-Record-PropertiesEntry"></a>

### Record.PropertiesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Property](#opensaves-Property) |  |  |






<a name="opensaves-SortOrder"></a>

### SortOrder
SortOrder is a way to order records returned by QueryRecords.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| direction | [SortOrder.Direction](#opensaves-SortOrder-Direction) |  | Direction to sort by (either ascending or descending). |
| property | [SortOrder.Property](#opensaves-SortOrder-Property) |  | Property to sort by. If using a user defined property, user_property_name must be passed in as well. |
| user_property_name | [string](#string) |  | The name of the user defined property. |






<a name="opensaves-Store"></a>

### Store
Store represents an internal bucket to contain records.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | key is the user defined primary key. It is recommended to use uniformally distributed key (e.g. UUID) rather than monotonically increasing values for performance reasons. See https://cloud.google.com/datastore/docs/best-practices#keys for details. |
| name | [string](#string) |  | name is a user defined name of the store. It is indexed and queriable with ListStore, but is otherwise opaque to the server. |
| tags | [string](#string) | repeated | tags are queryable strings to categorize stores Examples: &#34;player:1&#34;, &#34;system&#34;, &#34;inventory:xxx&#34; |
| owner_id | [string](#string) |  | owner_id is the owner of the store, represented as an external user ID. Open Saves doesn&#39;t maintain list of valid users and it is the responsibility of the client to keep track of user IDs. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | created_at is the point in time in UTC when the Store is created on the Open Saves server. It is managed and set by the server. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at is the point in time in UTC when the Store is updated on the Open Saves server. It is managed by the server and updated every time the Store is updated. |






<a name="opensaves-UpdateRecordRequest"></a>

### UpdateRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record | [Record](#opensaves-Record) |  | The content to update the record with. |
| hint | [Hint](#opensaves-Hint) |  | Performance hints. |






<a name="opensaves-UploadChunkRequest"></a>

### UploadChunkRequest
UploadChunkRequest is used by UploadChunk, which is a client-streaming
method. The first message should contain the metadata, and the subsequent
messages should contain the content field until EOF.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [ChunkMetadata](#opensaves-ChunkMetadata) |  | metadata is the metadata to associate the chunk to. |
| content | [bytes](#bytes) |  | content is the binary content of the chunk. |





 


<a name="opensaves-FilterOperator"></a>

### FilterOperator
FilterOperator has a list of comperators.

| Name | Number | Description |
| ---- | ------ | ----------- |
| EQUAL | 0 | = |
| GREATER | 1 | &gt; |
| LESS | 2 | &lt; |
| GREATER_OR_EQUAL | 3 | &gt;= |
| LESS_OR_EQUAL | 4 | &lt;= |



<a name="opensaves-Property-Type"></a>

### Property.Type
Supported structured data types

| Name | Number | Description |
| ---- | ------ | ----------- |
| DATATYPE_UNDEFINED | 0 |  |
| INTEGER | 1 |  |
| STRING | 2 |  |
| BOOLEAN | 3 |  |



<a name="opensaves-SortOrder-Direction"></a>

### SortOrder.Direction
Direction is the ways to sort a list of records returned by querying.

| Name | Number | Description |
| ---- | ------ | ----------- |
| ASC | 0 | Sort by ascending order. |
| DESC | 1 | Sort by descending order. |



<a name="opensaves-SortOrder-Property"></a>

### SortOrder.Property
Property to sort by, which can either be system defined properties
or a user property.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UPDATED_AT | 0 |  |
| CREATED_AT | 1 |  |
| USER_PROPERTY | 2 |  |


 

 


<a name="opensaves-OpenSaves"></a>

### OpenSaves
Public interface of the Open Saves service.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateStore | [CreateStoreRequest](#opensaves-CreateStoreRequest) | [Store](#opensaves-Store) | CreateStore creates and returns a new store. |
| GetStore | [GetStoreRequest](#opensaves-GetStoreRequest) | [Store](#opensaves-Store) | GetStore fetches store with the specified key. |
| ListStores | [ListStoresRequest](#opensaves-ListStoresRequest) | [ListStoresResponse](#opensaves-ListStoresResponse) | ListStore returns stores matching the provided criteria. |
| DeleteStore | [DeleteStoreRequest](#opensaves-DeleteStoreRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | DeleteStore deletes a single store with the specified key. |
| CreateRecord | [CreateRecordRequest](#opensaves-CreateRecordRequest) | [Record](#opensaves-Record) | CreateRecord creates a new record. This returns an error if the specified key already exists. |
| GetRecord | [GetRecordRequest](#opensaves-GetRecordRequest) | [Record](#opensaves-Record) | GetRecord returns a record with the specified key. |
| QueryRecords | [QueryRecordsRequest](#opensaves-QueryRecordsRequest) | [QueryRecordsResponse](#opensaves-QueryRecordsResponse) | QueryRecords performs a query and returns matching records. |
| UpdateRecord | [UpdateRecordRequest](#opensaves-UpdateRecordRequest) | [Record](#opensaves-Record) | UpdateRecord updates an existing record. This returns an error and does not create a new record if the key doesn&#39;t exist. |
| DeleteRecord | [DeleteRecordRequest](#opensaves-DeleteRecordRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | DeleteRecord deletes a single record with the specified key. |
| CreateBlob | [CreateBlobRequest](#opensaves-CreateBlobRequest) stream | [BlobMetadata](#opensaves-BlobMetadata) | CreateBlob adds a new blob to a record. |
| CreateChunkedBlob | [CreateChunkedBlobRequest](#opensaves-CreateChunkedBlobRequest) | [CreateChunkedBlobResponse](#opensaves-CreateChunkedBlobResponse) | CreateChunkedBlob starts a new chunked blob upload session. |
| UploadChunk | [UploadChunkRequest](#opensaves-UploadChunkRequest) stream | [ChunkMetadata](#opensaves-ChunkMetadata) | UploadChunk uploads and stores each each chunk. |
| CommitChunkedUpload | [CommitChunkedUploadRequest](#opensaves-CommitChunkedUploadRequest) | [BlobMetadata](#opensaves-BlobMetadata) | CommitChunkedUpload commits a chunked blob upload session and makes the blob available for reads. |
| AbortChunkedUpload | [AbortChunkedUploadRequest](#opensaves-AbortChunkedUploadRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | AbortChunkedUploads aborts a chunked blob upload session and discards temporary objects. |
| GetBlob | [GetBlobRequest](#opensaves-GetBlobRequest) | [GetBlobResponse](#opensaves-GetBlobResponse) stream | GetBlob retrieves a blob object in a record. Currently this method does not support chunked blobs and returns an UNIMPLEMENTED error if called for chunked blobs. TODO(yuryu): Support chunked blobs and return such objects entirely. |
| GetBlobChunk | [GetBlobChunkRequest](#opensaves-GetBlobChunkRequest) | [GetBlobChunkResponse](#opensaves-GetBlobChunkResponse) stream | GetBlobChunk returns a chunk of a blob object uploaded using CreateChunkedBlob. It returns an INVALID_ARGUMENT error if the blob is not a chunked object. |
| DeleteBlob | [DeleteBlobRequest](#opensaves-DeleteBlobRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | DeleteBlob removes an blob object from a record. |
| Ping | [PingRequest](#opensaves-PingRequest) | [PingResponse](#opensaves-PingResponse) | Ping returns the same string provided by the client. The string is optional and the server returns an empty string if omitted. |
| CompareAndSwap | [CompareAndSwapRequest](#opensaves-CompareAndSwapRequest) | [CompareAndSwapResponse](#opensaves-CompareAndSwapResponse) | CompareAndSwap compares the property to old_value and updates the property to value if the old_value and the current property are equal. The updated field in CompareAndSwapResponse is set to true if the swap is executed. For example, CompareAndSwap(property, value = 42, old_value = 24) will set the property to 42 if the current value is 24. CompareAndSwap also supports swapping with a value of another type, e.g. CompareAndSwap(property, value = &#34;42&#34;, old_value = 24). Otherwise it will not update the property and return the current (unchanged) value and updated = false. The operation is executed atomically. Errors: - NotFound: the requested record or property was not found. |
| CompareAndSwapGreaterInt | [AtomicIntRequest](#opensaves-AtomicIntRequest) | [AtomicIntResponse](#opensaves-AtomicIntResponse) | CompareAndSwapGreaterInt compares the number of an integer property to value and updates the property if the new value is greater than the current value. The updated field in AtomicResponse is set to true if the swap is executed. For example, CompareAndSwapGreaterInt(property, value = 42) will replace property with 42 and return {value = old value, updated = true} if 42 &gt; property. Otherwise it will not update the property and return the current (unchanged) value and updated = false. The operation is executed atomically. Errors: - NotFound: the requested record or property was not found. - InvalidArgument: the requested property was not an integer. |
| CompareAndSwapLessInt | [AtomicIntRequest](#opensaves-AtomicIntRequest) | [AtomicIntResponse](#opensaves-AtomicIntResponse) | CompareAndSwapLessInt does the same operation as CompareAndSwapGreaterInt except the condition is the new value is less than the old value. |
| AtomicAddInt | [AtomicIntRequest](#opensaves-AtomicIntRequest) | [AtomicIntResponse](#opensaves-AtomicIntResponse) | AtomicAddInt adds a number to an integer property atomically. For example, AtomicAdd(property, 42) will run property &#43;= 42 and return the old value. The updated field in AtomicIntResponse is always set to true. Errors: - NotFound: the requested record or property was not found. - InvalidArgument: the requested property was not an integer. |
| AtomicSubInt | [AtomicIntRequest](#opensaves-AtomicIntRequest) | [AtomicIntResponse](#opensaves-AtomicIntResponse) | AtomicSubInt does the same except it subtracts a number. |
| AtomicInc | [AtomicIncRequest](#opensaves-AtomicIncRequest) | [AtomicIntResponse](#opensaves-AtomicIntResponse) | AtomicInc increments the number of an integer property if less than upper_bound. Otherwise it resets the property to lower_bound. if (property &lt; upper_bound) { property&#43;&#43; } else { property = lower_bound } This makes the property an incrementing counter between [lower_bound, upper_bound]. It returns the old value of the property. The updated field in AtomicIntResponse is set to true. Errors: - NotFound: the requested record or property was not found. - InvalidArgument: the requested property was not an integer. |
| AtomicDec | [AtomicIncRequest](#opensaves-AtomicIncRequest) | [AtomicIntResponse](#opensaves-AtomicIntResponse) | AtomicDec decrements the number of an integer property by one if more than lower_bound. Otherwise it resets the property to upper_bound. if (lower_bound &lt; property) { property-- } else { property = upper_bound } This makes the property a decrementing counter between [lower_bound, upper_bound]. It returns the old value of the property. The updated field in AtomicIntResponse is always set to true. Errors: - NotFound: the requested record or property was not found. - InvalidArgument: the requested property was not an integer. |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

