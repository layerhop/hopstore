# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-03

Initial release.

- `Store`: single-writer store over an S3-compatible prefix, with atomic batch commits, a local fsynced WAL,
  group-committed WAL objects on S3 (`durable: "local"` and `durable: "s3"`), Parquet snapshots, recovery and
  `If-None-Match` manifest fencing.
- Table kinds: state tables, ranked indexes over state tables, and log tables with Parquet segments (`sealLogs`,
  `scan`).
- Object stores: `S3ObjectStore` (SigV4, path-style, bounded concurrency, retries) and `MemoryObjectStore` for tests.
- Table views (`StateTable`, `RankedIndex`, `LogTable`) are read-only interfaces; the package exports no table
  classes, so the internal mutators cannot be reached through the public types. `Store.describe()` reports the
  manifest seq, epoch, sequence numbers and per-table counts.
- Values the store holds are deeply frozen; mutating a value read from a table throws instead of diverging from the WAL.
- `RankedSpec.minScore` (default `1n`: only positive scores are ranked; `null` ranks every score).
- Ranked indexes keep each group as sorted blocks: an update is O(log n + block) instead of a splice of the whole
  group, and a batch applies its last change per member only.
- Opening a store reads state snapshots one row group at a time (1M rows: +272 MB peak RSS for a 167 MB table, against
  +487 MB decoding the whole file); `bench/snapshot-load.ts` measures it.
- `LogTable.scanEntries` (scan with each row's partition); `SealOptions.where`/`evict`
  also get the row's partition; `SealOptions.withOps` commits ops built from exactly the sealed rows, published with
  the seal. A scan copies only the in-range slice of memory. The periodic snapshot timer starts over after every
  snapshot or seal that writes one, so a seal counts as the periodic snapshot (a seal with nothing to seal does not).
- `LogSpec.codec` (`RowCodec`): a log table holds its rows in memory packed and decodes them on every read; stored
  data is unchanged. `SealOptions.packed` gives seal callbacks the packed strings, `LogTable.packedRowsInMemory` reads
  them. Rows loaded from a snapshot or the WAL get the same round-trip check as commits; one that fails it is held
  unpacked. Rows of a partition share one partition string, and a log snapshot is loaded one row group at a time.
- `SealOptions.tables`: a seal of only some log tables (tables with different seal rules seal on their own); a table
  named twice is rejected. `where`, `evict` and `withOps` also get each row's table.
- `Store.compactLogs` (`CompactOptions`): merges segments of a log table by a caller's grouping into fewer, larger
  ones, published atomically in place of the inputs (same scan results, equal keys in the same order); inputs are
  deleted after the publish, and not while a scan that may read them runs (scans lease their segments). `close()` waits
  for a compaction in flight, which stops after its current run.
- `Store.sealsInDoubt()`: failed seals that may still land (until adopted, or until their manifest slot is taken).
- `Store.open` validates `prefix` (`""` or safe segments ending in `/`) and `localDir` (non-empty).
- `flush()`, `snapshot()` and `sealLogs()` reject after `close()` ("store is closed") and with `FencedError` once fenced.
- The default `onError` always logs with `console.error`; pass `onError` to route or silence background failures.
- `S3Config.forcePathStyle` (default `true`) with virtual-hosted-style URLs when `false`; keys are URI-encoded as SigV4
  canonicalises them; `ListObjectsV2` responses are parsed with a minimal XML parser (entities, CDATA, namespaces,
  truncation without a continuation token is an error).
