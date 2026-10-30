# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

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
- `Store.open` validates `prefix` (`""` or safe segments ending in `/`) and `localDir` (non-empty).
- `flush()`, `snapshot()` and `sealLogs()` reject after `close()` ("store is closed") and with `FencedError` once fenced.
- The default `onError` always logs with `console.error`; pass `onError` to route or silence background failures.
- `S3Config.forcePathStyle` (default `true`) with virtual-hosted-style URLs when `false`; keys are URI-encoded as SigV4
  canonicalises them; `ListObjectsV2` responses are parsed with a minimal XML parser (entities, CDATA, namespaces,
  truncation without a continuation token is an error).
