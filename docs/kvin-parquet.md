# KVIN Parquet upload and download

`POST /linkedfactory/**/values` accepts `Content-Type: application/vnd.apache.parquet`.
`GET` on the same endpoint returns that format with `Accept: application/vnd.apache.parquet`
or `?type=application/vnd.apache.parquet`. Existing item, property, model, time-range,
limit, and aggregation parameters apply to downloads.

Both directions use one row per KVIN tuple. The schema follows the internal Parquet
archive, with URI strings replacing internal IDs and without the archive's `first`
marker or separate mapping files. Archive shards and generic SPARQL result files are
not this exchange format. SPARQL export is unchanged.

| Column | Physical type | Meaning |
| --- | --- | --- |
| `item` | BINARY (STRING) | Item URI |
| `context` | BINARY (STRING) | Context URI |
| `property` | BINARY (STRING) | Property URI |
| `time` | INT64 | Epoch milliseconds |
| `seqNr` | INT32 | Sequence number |
| `valueInt` | INT32 | Java integer |
| `valueLong` | INT64 | Java long |
| `valueFloat` | FLOAT | Java float |
| `valueDouble` | DOUBLE | Java double |
| `valueString` | BINARY | UTF-8 string, as in the archive |
| `valueBool` | BOOLEAN | Boolean |
| `valueObject` | BINARY | Archive `Records` binary encoding |

Exactly one value column must be non-null in each row. Types can vary between rows,
including within the same item/property series. A single generic `value` column is
not accepted. Empty result sets produce valid Parquet files with the same schema.

Downloads include all columns. Uploads can omit unused value columns, `context`
(defaults to the request's model context), and `seqNr` (defaults to zero). Column
order does not matter; unrelated columns are ignored. Relative item/property URIs
resolve against the request path; supplied context URIs must be absolute.
Uploads also accept INT64 `seqNr` within the INT32 range and timestamp annotations
on `time` in milliseconds, microseconds or nanoseconds (floored to milliseconds).
Decimal and unsigned integer annotations are not accepted. `valueString` may carry
the STRING annotation when produced by another tool.

`valueObject` reuses the existing KVIN codec for records, arrays, URI values and
additional scalar types. It is not JSON or Parquet VARIANT. Other Parquet tools see
opaque bytes here; decoding requires the KVIN codec. Existing codec conventions
convert BigInteger to long and BigDecimal to double, potentially losing range or
precision; this format does not add arbitrary-precision support.

Uploads up to 32 MiB are buffered in memory; larger files are spooled to a temporary
file and deleted after processing. Parsing proceeds by row group. Downloads write
tuples directly to the response with bounded row-group buffering. Ingestion uses
the normal `store.put(...)` path and is not transactional: a late invalid row can
leave earlier tuples stored. Schema and available footer statistics are checked
before ingestion, and each row is validated while reading.

Example with pyarrow (two different datatypes in one file):

```python
import pyarrow as pa
import pyarrow.parquet as pq

pq.write_table(pa.table({
    'item': ['urn:machine1', 'urn:machine1'],
    'property': ['urn:temperature', 'urn:running'],
    'time': pa.array([1710000000000, 1710000000000], type=pa.int64()),
    'valueDouble': pa.array([21.5, None], type=pa.float64()),
    'valueBool': pa.array([None, True], type=pa.bool_()),
}), 'values.parquet')
```

```sh
curl -X POST -H 'Content-Type: application/vnd.apache.parquet' \
  --data-binary @values.parquet http://localhost:8080/linkedfactory/values

curl -H 'Accept: application/vnd.apache.parquet' \
  'http://localhost:8080/linkedfactory/values?item=urn:machine1' \
  -o downloaded.parquet
```

See [ingestion and codec benchmarks](benchmarks/kvin-ingestion.md) for performance measurements.
