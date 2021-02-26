# Reference
<a name="top"></a>

## Table of Contents

- [api/open_saves.proto](../api/open_saves.proto)
    - [BlobMetadata](#opensaves.BlobMetadata)
    - [CreateBlobRequest](#opensaves.CreateBlobRequest)
    - [CreateRecordRequest](#opensaves.CreateRecordRequest)
    - [CreateStoreRequest](#opensaves.CreateStoreRequest)
    - [DeleteBlobRequest](#opensaves.DeleteBlobRequest)
    - [DeleteRecordRequest](#opensaves.DeleteRecordRequest)
    - [DeleteStoreRequest](#opensaves.DeleteStoreRequest)
    - [GetBlobRequest](#opensaves.GetBlobRequest)
    - [GetBlobResponse](#opensaves.GetBlobResponse)
    - [GetRecordRequest](#opensaves.GetRecordRequest)
    - [GetStoreRequest](#opensaves.GetStoreRequest)
    - [Hint](#opensaves.Hint)
    - [ListStoresRequest](#opensaves.ListStoresRequest)
    - [ListStoresResponse](#opensaves.ListStoresResponse)
    - [PingRequest](#opensaves.PingRequest)
    - [PingResponse](#opensaves.PingResponse)
    - [Property](#opensaves.Property)
    - [QueryFilter](#opensaves.QueryFilter)
    - [QueryRecordsRequest](#opensaves.QueryRecordsRequest)
    - [QueryRecordsResponse](#opensaves.QueryRecordsResponse)
    - [Record](#opensaves.Record)
    - [Record.PropertiesEntry](#opensaves.Record.PropertiesEntry)
    - [Store](#opensaves.Store)
    - [UpdateRecordRequest](#opensaves.UpdateRecordRequest)
  
    - [FilterOperator](#opensaves.FilterOperator)
    - [Property.Type](#opensaves.Property.Type)
  
    - [OpenSaves](#opensaves.OpenSaves)
  
- [Scalar Value Types](#scalar-value-types)



<a name="api/open_saves.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## api/open_saves.proto



<a name="opensaves.BlobMetadata"></a>

### BlobMetadata
BlobMetadata contains necessary metadata
when creating (uploading) a new blob object.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | store_key is the key of the store that the record belongs to. |
| record_key | [string](#string) |  | record_key is the key of the record the new blob object belongs to. |
| size | [int64](#int64) |  | size is the byte length of the object. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints (write only). |






<a name="opensaves.CreateBlobRequest"></a>

### CreateBlobRequest
CreateBlobRequest is used for CreateBlob, a client-streaming method.
The first message should contain metadata, and the subsequent messages
should contain content.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [BlobMetadata](#opensaves.BlobMetadata) |  | metadata is the metadata used to initialize the blob object with. |
| content | [bytes](#bytes) |  | content is the binary content of the blob object. |






<a name="opensaves.CreateRecordRequest"></a>

### CreateRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store to create the record into. |
| record | [Record](#opensaves.Record) |  | The record to create. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints. |






<a name="opensaves.CreateStoreRequest"></a>

### CreateStoreRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store | [Store](#opensaves.Store) |  | Store to create. |






<a name="opensaves.DeleteBlobRequest"></a>

### DeleteBlobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record to delete a blob from. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints. |






<a name="opensaves.DeleteRecordRequest"></a>

### DeleteRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| key | [string](#string) |  | The key of the record to delete. |






<a name="opensaves.DeleteStoreRequest"></a>

### DeleteStoreRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the store to delete. |






<a name="opensaves.GetBlobRequest"></a>

### GetBlobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record_key | [string](#string) |  | The key of the record to get. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints. |






<a name="opensaves.GetBlobResponse"></a>

### GetBlobResponse
GetBlobResponse is a server-streaming response to return metadata and
content of a blob object. The first message contains blob_ref and the
subsequent messages contain the rest of the binary blob in the content
field.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [BlobMetadata](#opensaves.BlobMetadata) |  | metadata is the metadata used to initialize the blob object with. |
| content | [bytes](#bytes) |  | content is the binary content of the blob object. |






<a name="opensaves.GetRecordRequest"></a>

### GetRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| key | [string](#string) |  | The key of the record to get. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints. |






<a name="opensaves.GetStoreRequest"></a>

### GetStoreRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the store to retrieve. |






<a name="opensaves.Hint"></a>

### Hint
Performance optimization hints for the server.
The server may silently ignore the hints when not feasible.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| do_not_cache | [bool](#bool) |  | If true, do not cache the record for future requests. |
| skip_cache | [bool](#bool) |  | If true, skip the cache check and always check the metadata server. If false, allow file size to determine cache checks. |
| force_blob_store | [bool](#bool) |  | If true, always store the blob in blob storage, rather than in the metadata server. If false, allow file size to determine where to store the blob. |
| force_inline_blob | [bool](#bool) |  | Tells the server to not use blob storage. Always store the blob into the metadata entity. The server will return an error if the blob is too large. The exact size limit depends on the backend implementation. |






<a name="opensaves.ListStoresRequest"></a>

### ListStoresRequest
ListStoresRequest contains conditions to filter stores.
Multiple conditions are AND&#39;ed together.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Store name. This is an exact match. |
| tags | [string](#string) | repeated | List of tags |
| owner_id | [string](#string) |  | owner_id is the owner of stores, represented as an external user ID. |






<a name="opensaves.ListStoresResponse"></a>

### ListStoresResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stores | [Store](#opensaves.Store) | repeated | List of stores that match the provided criteria. |






<a name="opensaves.PingRequest"></a>

### PingRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ping | [string](#string) |  | An optional string to send with the ping message. |






<a name="opensaves.PingResponse"></a>

### PingResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pong | [string](#string) |  | An optional string that is copied from PingRequest. |






<a name="opensaves.Property"></a>

### Property
Property represents typed data in Open Saves.
This is limited to one type per key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Property.Type](#opensaves.Property.Type) |  |  |
| integer_value | [int64](#int64) |  |  |
| string_value | [string](#string) |  |  |
| boolean_value | [bool](#bool) |  |  |






<a name="opensaves.QueryFilter"></a>

### QueryFilter
QueryFilter is a filtering condition when querying records.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property_name | [string](#string) |  | Propety name of the filter. |
| operator | [FilterOperator](#opensaves.FilterOperator) |  | Operator of the filter. |
| value | [Property](#opensaves.Property) |  | Value to compare the property with. |






<a name="opensaves.QueryRecordsRequest"></a>

### QueryRecordsRequest
QueryRecordsRequest contains conditions to search particular records.
Multiple conditions are AND&#39;ed together.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | store_key is the primary key of the store. Optional and the method queries all stores when omitted. |
| filters | [QueryFilter](#opensaves.QueryFilter) | repeated | List of filters |
| tags | [string](#string) | repeated | List of tags |
| owner_id | [string](#string) |  | owner_id is the owner of records, represented as an external user ID. |






<a name="opensaves.QueryRecordsResponse"></a>

### QueryRecordsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [Record](#opensaves.Record) | repeated | List of records that match the criteria. |
| store_keys | [string](#string) | repeated | List of store keys that each of the records belongs to. The order of keys is the same as the records field, e.g. store_keys[0] is the store for records[0], and so on. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints. Query caching is not supported at the moment |






<a name="opensaves.Record"></a>

### Record
Record represents each entity in the Open Saves database.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | key is the user defined primary key. It is recommended to use uniformally distributed key (e.g. UUID) rather than monotonically increasing values for performance reasons. See https://cloud.google.com/datastore/docs/best-practices#keys for details. |
| blob_size | [int64](#int64) |  | Byte length of the blob (read-only) |
| properties | [Record.PropertiesEntry](#opensaves.Record.PropertiesEntry) | repeated | Typed values that are indexed and searchable. The number of properties allowed in a record is backend dependent. |
| owner_id | [string](#string) |  | owner_id is the owner of the record, represented as an external user ID. Open Saves doesn&#39;t maintain list of valid users and it is the responsibility of the client to keep track of user IDs. |
| tags | [string](#string) | repeated | tags are queryable strings to categorize records Examples: &#34;player:1&#34;, &#34;system&#34;, &#34;inventory:xxx&#34; |
| created_at | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | created_at is the point in time in UTC when the Record is created on the Open Saves server. It is managed and set by the server. |
| updated_at | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | updated_at is the point in time in UTC when the Record is updated on the Open Saves server. It is managed by the server and updated every time the Record is updated. |






<a name="opensaves.Record.PropertiesEntry"></a>

### Record.PropertiesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Property](#opensaves.Property) |  |  |






<a name="opensaves.Store"></a>

### Store
Store represents an internal bucket to contain records.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | key is the user defined primary key. It is recommended to use uniformally distributed key (e.g. UUID) rather than monotonically increasing values for performance reasons. See https://cloud.google.com/datastore/docs/best-practices#keys for details. |
| name | [string](#string) |  | name is a user defined name of the store. It is indexed and queriable with ListStore, but is otherwise opaque to the server. |
| tags | [string](#string) | repeated | tags are queryable strings to categorize stores Examples: &#34;player:1&#34;, &#34;system&#34;, &#34;inventory:xxx&#34; |
| owner_id | [string](#string) |  | owner_id is the owner of the store, represented as an external user ID. Open Saves doesn&#39;t maintain list of valid users and it is the responsibility of the client to keep track of user IDs. |
| created_at | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | created_at is the point in time in UTC when the Store is created on the Open Saves server. It is managed and set by the server. |
| updated_at | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | updated_at is the point in time in UTC when the Store is updated on the Open Saves server. It is managed by the server and updated every time the Store is updated. |






<a name="opensaves.UpdateRecordRequest"></a>

### UpdateRecordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_key | [string](#string) |  | The key of the store that the record belongs to. |
| record | [Record](#opensaves.Record) |  | The content to update the record with. |
| hint | [Hint](#opensaves.Hint) |  | Performance hints. |





 


<a name="opensaves.FilterOperator"></a>

### FilterOperator
FilterOperator has a list of comperators.

| Name | Number | Description |
| ---- | ------ | ----------- |
| EQUAL | 0 | = |
| GREATER | 1 | &gt; |
| LESS | 2 | &lt; |
| GREATER_OR_EQUAL | 3 | &gt;= |
| LESS_OR_EQUAL | 4 | &lt;= |



<a name="opensaves.Property.Type"></a>

### Property.Type
Supported structured data types

| Name | Number | Description |
| ---- | ------ | ----------- |
| DATATYPE_UNDEFINED | 0 |  |
| INTEGER | 1 |  |
| STRING | 2 |  |
| BOOLEAN | 3 |  |


 

 


<a name="opensaves.OpenSaves"></a>

### OpenSaves
Public interface of the Open Saves service.
TODO(#69): Add streaming endpoints to transfer large blobs.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateStore | [CreateStoreRequest](#opensaves.CreateStoreRequest) | [Store](#opensaves.Store) | CreateStore creates and returns a new store. |
| GetStore | [GetStoreRequest](#opensaves.GetStoreRequest) | [Store](#opensaves.Store) | GetStore fetches store with the specified key. |
| ListStores | [ListStoresRequest](#opensaves.ListStoresRequest) | [ListStoresResponse](#opensaves.ListStoresResponse) | ListStore returns stores matching the provided criteria. |
| DeleteStore | [DeleteStoreRequest](#opensaves.DeleteStoreRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) | DeleteStore deletes a single store with the specified key. |
| CreateRecord | [CreateRecordRequest](#opensaves.CreateRecordRequest) | [Record](#opensaves.Record) | CreateRecord creates a new record. This returns an error if the specified key already exists. |
| GetRecord | [GetRecordRequest](#opensaves.GetRecordRequest) | [Record](#opensaves.Record) | GetRecord returns a record with the specified key. |
| QueryRecords | [QueryRecordsRequest](#opensaves.QueryRecordsRequest) | [QueryRecordsResponse](#opensaves.QueryRecordsResponse) | QueryRecords performs a query and returns matching records. |
| UpdateRecord | [UpdateRecordRequest](#opensaves.UpdateRecordRequest) | [Record](#opensaves.Record) | UpdateRecord updates an existing record. This returns an error and does not create a new record if the key doesn&#39;t exist. |
| DeleteRecord | [DeleteRecordRequest](#opensaves.DeleteRecordRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) | DeleteRecord deletes a single record with the specified key. |
| CreateBlob | [CreateBlobRequest](#opensaves.CreateBlobRequest) stream | [BlobMetadata](#opensaves.BlobMetadata) | CreateBlob adds a new blob to a record. |
| GetBlob | [GetBlobRequest](#opensaves.GetBlobRequest) | [GetBlobResponse](#opensaves.GetBlobResponse) stream | GetBlob retrieves a blob object in a record. |
| DeleteBlob | [DeleteBlobRequest](#opensaves.DeleteBlobRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) | DeleteBlob removes an blob object from a record. |
| Ping | [PingRequest](#opensaves.PingRequest) | [PingResponse](#opensaves.PingResponse) | Ping returns the same string provided by the client. The string is optional and the server returns an empty string if omitted. |

 



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

