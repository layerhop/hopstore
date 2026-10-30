# Hopstore

An embedded storage engine for Node.js. Your tables live in the memory of your own process, so reads are plain
function calls that answer in microseconds, like reading from Redis without the network hop. Every commit is fsynced to
a local write-ahead log first, and the permanent copy lives on cheap S3-compatible object storage as gzipped WAL
objects and open Apache Parquet files. There is no database server to run: one Node process, one local directory, one
bucket prefix. It was built for the market indexer of [LayerHop](https://layerhop.com) and has run on its production server
since 2026-10-30, next to the Postgres-backed indexer it is meant to replace (shadow run).

```
npm install hopstore
```

Apache-2.0. Requires Node.js 20 or newer.

## When to use it

- Your live working set fits in RAM (see [Limits](#limits)) and you read it far more often than you write it.
- You want reads with no network round trip: leaderboards, top-N, latest-N, counters, per-key state.
- You want durable, inspectable storage without operating Postgres or Redis: an S3 bucket you already pay for, with
  snapshots in a format DuckDB, Polars, Spark or pandas can read directly.
- One process writes. Typical fits: indexers, stream processors, bots, game or market state, materialised views fed
  from a queue or a chain.

## When not to use it

- Several processes need to write the same data. Hopstore allows exactly one writer per prefix and fences the others.
- Your live data does not fit in memory, or you need ad-hoc SQL over it. Use a database (or query the Parquet
  snapshots with DuckDB).
- You need read replicas or other processes reading live data. Readers live inside the writer process.
- You need a mature, widely deployed system. Hopstore is young (see [Limits](#limits)).

## Quick start

This runs as is, with no credentials: `MemoryObjectStore` is an in-memory stand-in for S3. The same code is in
[`examples/quickstart.ts`](examples/quickstart.ts) (`npx tsx examples/quickstart.ts`).

```ts
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryObjectStore, Store, type ObjectStore, type StoreOptions } from "hopstore";

type Player = { name: string; points: number };

// Tables are declared up front, the same way on every open.
const options = (objects: ObjectStore, localDir: string): StoreOptions => ({
  objects,
  prefix: "examples/leaderboard/",
  localDir,
  tables: {
    state: ["players"],
    // a ranked index orders the rows of a state table by a bigint score, per group
    ranked: [{ name: "by_points", table: "players", group: () => "all", member: (key) => key, score: (v) => BigInt((v as Player).points) }],
  },
});

const objects = new MemoryObjectStore();
const store = await Store.open(options(objects, await mkdtemp(join(tmpdir(), "hop-"))));

// one atomic batch; resolves once it is fsynced to the local WAL
await store.commit([
  { t: "put", table: "players", key: "ada", value: { name: "Ada", points: 120 } },
  { t: "put", table: "players", key: "bob", value: { name: "Bob", points: 80 } },
  { t: "put", table: "players", key: "cy", value: { name: "Cy", points: 95 } },
]);

// reads are synchronous and come from memory
const bob = store.stateTable<Player>("players").get("bob")!;
// `durable: "s3"` resolves once the batch is published to the object store
await store.commit([{ t: "put", table: "players", key: "bob", value: { ...bob, points: bob.points + 50 } }], { durable: "s3" });

console.log(store.ranked("by_points").top("all", 2)); // [{ member: "bob", score: 130n }, { member: "ada", score: 120n }]

await store.close(); // final flush and a Parquet snapshot of every table

// reopen with an EMPTY local directory: everything comes back from the object store
const again = await Store.open(options(objects, await mkdtemp(join(tmpdir(), "hop-"))));
console.log(again.stateTable("players").size, again.ranked("by_points").top("all", 1)); // 3 [{ member: "bob", score: 130n }]
await again.close();
```

For real storage, swap the object store ([`examples/s3.ts`](examples/s3.ts) is a complete script):

```ts
import { S3ObjectStore, Store } from "hopstore";

const objects = new S3ObjectStore({
  endpoint: process.env.S3_ENDPOINT!, // e.g. https://eu2.contabostorage.com, http://localhost:9000
  region: process.env.S3_REGION!, // e.g. "default" (Contabo), "us-east-1", "auto" (R2)
  bucket: process.env.S3_BUCKET!,
  accessKeyId: process.env.S3_ACCESS_KEY_ID!,
  secretAccessKey: process.env.S3_SECRET_ACCESS_KEY!,
  maxConcurrency: 16, // optional, the default
});
const store = await Store.open({ objects, prefix: "myapp/prod/", localDir: "/var/lib/myapp/hopstore", tables: { state: ["players"] } });
```

Give `localDir` a persistent disk if you can: reopening with the same directory recovers batches that were committed
locally but not yet published to S3, and reuses cached snapshots instead of downloading them.

## Concepts

### Tables

All tables are declared in `StoreOptions.tables` and live entirely in memory. State and log table names share one
namespace; names match `[A-Za-z0-9_-]+`. Opening a store whose stored data names a table you did not declare fails
rather than silently dropping it.

- **State tables** map a string key to a JSON value (`Json`: null, boolean, number, string, arrays, plain objects; no
  bigints, store them as decimal strings). `get`, `has`, `size`, iteration and prefix `scan`.
- **Ranked indexes** are maintained from a state table on every put and delete: a spec maps each row to a
  `(group, member, score)` with a `bigint` score. Per group, members are kept ordered by score (descending, ties by
  member ascending) with a running sum, so `top(group, n)`, `count(group)` and `sum(group)` are cheap. By default only
  positive scores are ranked (a zero balance is not a holder); `minScore` changes that. Indexes are not stored; they
  are rebuilt on open.
- **Log tables** hold append-only rows per partition, ordered by a sort key (a non-empty array of strings and finite
  numbers, compared element by element). The newest `keepPerPartition` rows of each partition stay in memory for
  `latest(partition, n)`; older rows wait in memory until `sealLogs()` moves them into sorted Parquet segments on S3.
  `scan({ partition, from, to })` merges memory with those segments, reading segments by range or from a local cache.
  `sealLogs` selects rows by sort key (`olderThanSort`) or content (`where`), and `evict` also takes rows from the
  newest ones (age-based eviction).

### Commits and durability

`commit(ops)` applies a batch of `put`, `del` and `append` ops atomically and returns its batch number (a dense
sequence starting at 1). Ops are validated, including the ranked and sort-key callbacks, before anything is written.
There are two durability levels:

- `durable: "local"` (the default) resolves once the batch is fsynced to the local WAL and visible to reads. Commits in
  the same tick share one fsync.
- `durable: "s3"` resolves once the batch is published to S3. A background loop (every `flushMs`, default 1 s) uploads
  everything not yet published as one gzipped WAL object and publishes a manifest naming it; durable commits trigger a
  flush on the next tick, so concurrent ones share it. Failed flushes are retried.

`seq` is the last committed batch and `durableSeq()` the last one published to S3. A common pattern is to commit your
data and an input cursor in the same batch and resume from the cursor after a restart.

### Snapshots

`snapshot()` (also on a timer, every `snapshotEveryMs`, default 10 minutes, and on `close()`) writes every table as
Parquet at one batch number, publishes a manifest naming it, then deletes the WAL objects, snapshots and segments that
manifest no longer needs. The capture is synchronous and cheap; encoding runs one 10,000-row group at a time with the
event loop free in between, so commits and reads go on during a snapshot. State snapshots have the columns `key` and
`value` (JSON text); log snapshots and segments have `partition`, `sort` and `row` (JSON text).

### Recovery

`Store.open` reads the newest manifest, loads its snapshots, replays the WAL objects it names, then the tail of the
local WAL if it was written by the same writer. WAL objects whose batches are still in the local WAL are replayed from
disk instead of being downloaded. Log segments are never read back into memory.

### Fencing

Each `Store.open` claims a new epoch by creating the next manifest with `If-None-Match: *`. A writer that loses a race
for a manifest, or finds its predecessor gone, is fenced: its commits reject with `FencedError` and `failed()` reports
why. Treat that as fatal: exit and let a supervisor restart the process.

## API

Everything below is exported from the package root.

### `Store`

```ts
class Store {
  static open(o: StoreOptions): Promise<Store>;

  commit(ops: Op[], opts?: { durable?: "local" | "s3" }): Promise<number>;
  waitDurable(seq: number): Promise<void>; // resolves once batch `seq` is on S3
  flush(): Promise<void>; // publish everything committed so far now
  snapshot(): Promise<void>;
  sealLogs(opts?: SealOptions): Promise<number>; // rows sealed
  close(): Promise<void>; // flush + snapshot (unless fenced), then stop timers and close the WAL
  describe(): StoreInfo; // manifest seq, epoch, sequence numbers and per-table counts, from memory

  stateTable<V extends Json = Json>(name: string): StateTable<V>;
  ranked(name: string): RankedIndex;
  logTable<R extends Json = Json>(name: string): LogTable<R>;

  readonly epoch: number; // this writer's epoch
  readonly seq: number; // last committed (visible) batch
  durableSeq(): number; // last batch published to S3
  failed(): string | null; // why the store stopped for good (fenced, or the local WAL failed), else null
}

interface StoreOptions {
  objects: ObjectStore;
  prefix: string; // "" or ending with "/", e.g. "myapp/prod/"; everything lives under it (see below)
  localDir: string; // local WAL and snapshot/segment cache
  flushMs?: number; // group-commit interval, default 1000
  snapshotEveryMs?: number; // default 600000 (10 minutes)
  tables?: StoreTables;
  segmentWholeMaxBytes?: number; // segments up to this size are cached whole locally, default 64 MiB
  onError?: (e: unknown) => void; // background flush/snapshot failures (retried); default console.error("hopstore:", e)
}

interface StoreInfo {
  prefix: string;
  epoch: number;
  manifestSeq: number; // newest manifest this writer created or adopted
  seq: number; // last committed batch
  durableSeq: number; // last batch on S3
  snapshotSeq: number; // batch of the current snapshot (0: none yet)
  walObjects: number; // WAL objects after the snapshot
  state: Record<string, { rows: number }>;
  log: Record<string, { memoryRows: number; segments: number; segmentRows: number; segmentBytes: number }>;
}

interface StoreTables {
  state?: string[];
  ranked?: RankedSpec[];
  log?: LogSpec[];
}

interface RankedSpec {
  name: string;
  table: string; // a declared state table
  group: (key: string, v: Json) => string;
  member: (key: string, v: Json) => string;
  score: (v: Json) => bigint;
  minScore?: bigint | null; // scores below it are not ranked; default 1n (positive only), null ranks every score
}

interface LogSpec {
  name: string;
  keepPerPartition: number; // integer >= 0
  sortKey: (row: Json) => SortKey;
}

interface SealOptions {
  olderThanSort?: SortKey; // waiting rows with a lower sort key
  where?: (row: Json) => boolean; // waiting rows it accepts
  evict?: (row: Json) => boolean; // any row in memory it accepts, the newest included
}

type Op =
  | { t: "put"; table: string; key: string; value: Json }
  | { t: "del"; table: string; key: string }
  | { t: "append"; table: string; partition: string; row: Json };

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
type SortKey = (string | number)[];
interface Batch { seq: number; ops: Op[] }
class FencedError extends Error {}
function compareSort(a: SortKey, b: SortKey): number; // the sort-key order (numbers before strings, a prefix first)
```

`prefix` is `""` or one or more segments each followed by `/`. Segments use letters, digits and `! - _ . * ' ( ) =`
only, and cannot be empty, `.` or `..` (so no leading `/` and no `//`). `localDir` must be a non-empty path.
`Store.open` rejects anything else before touching S3 or the disk.

`flush()`, `snapshot()` and `sealLogs()` reject with `FencedError` on a fenced store and with "store is closed" after
`close()`.

Values you read are deeply frozen: the store keeps its own decoded copy of every committed value, so mutating the
objects you passed to `commit` afterwards is safe, and mutating a value you read throws a `TypeError` (in strict mode,
which ES modules always are). To change a value, commit a changed copy.

### Table views

`stateTable`, `ranked` and `logTable` return read-only interfaces; every write goes through `commit`. They are types
only: the package exports no table classes.

#### `StateTable<V>`

```ts
readonly name: string;
readonly size: number;
get(key: string): V | undefined;
has(key: string): boolean;
entries(): IterableIterator<[string, V]>; // no particular order
keys(): IterableIterator<string>;
values(): IterableIterator<V>;
scan(prefix?: string): IterableIterator<[string, V]>; // key order; sorts the matching keys on every call
```

Consume iterators synchronously; do not hold one across an `await`.

#### `RankedIndex`

```ts
top(group: string, n: number): { member: string; score: bigint }[]; // highest first, ties by member
count(group: string): number; // members with a ranked score (at least minScore)
sum(group: string): bigint; // sum of their scores
groups(): IterableIterator<string>;
```

#### `LogTable<R>`

```ts
readonly name: string;
readonly size: number; // rows in memory
latest(partition: string, n: number, keep?: (row: R) => boolean): R[]; // newest first, at most keepPerPartition
scan(range: { partition?: string; from: SortKey; to: SortKey }): AsyncIterable<R>; // from <= sort < to, memory + segments
partitions(): IterableIterator<string>; // partitions with rows in memory
memoryRows(partition: string): IterableIterator<R>; // rows in memory, ascending
rowsInMemory(partition: string): IterableIterator<R>; // the same, unordered and without copying
has(partition: string, sortKey: SortKey): boolean; // memory only
```

Scope scans to a partition and a narrow range where you can: without a partition, every segment that overlaps the
range is opened.

### Object stores

```ts
interface ObjectStore {
  put(key: string, body: Uint8Array, opts?: { ifNoneMatch?: "*"; ifMatch?: string; contentType?: string }): Promise<{ etag: string }>;
  get(key: string, range?: { start: number; end: number }): Promise<Uint8Array | null>; // range end inclusive; null = 404
  head(key: string): Promise<ObjectInfo | null>;
  list(prefix: string): Promise<ObjectInfo[]>; // sorted by key
  delete(key: string): Promise<void>;
}
interface ObjectInfo { key: string; size: number; etag: string }

class S3ObjectStore implements ObjectStore {
  constructor(cfg: S3Config, fetchFn?: typeof fetch);
}
interface S3Config {
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  maxConcurrency?: number; // default 16
  forcePathStyle?: boolean; // default true: <endpoint>/<bucket>/<key>; false: <scheme>://<bucket>.<endpoint host>/<key>
}

class MemoryObjectStore implements ObjectStore {
  readonly objects: Map<string, Uint8Array>;
  failNext(n: number, status?: number): void; // the next n calls throw S3Error(status, default 503)
}

class PreconditionFailed extends Error {} // a conditional write lost (HTTP 412 or 409)
class S3Error extends Error { readonly status: number }
```

You can implement `ObjectStore` yourself (for example over a local filesystem or another SDK). A conditional put must
reject with `PreconditionFailed` when it loses. The types `Manifest`, `SegmentRef` and `SnapshotRef` describe the
manifest JSON.

## Guarantees

- `commit(ops)` is atomic and ordered: all ops of a batch apply or none do (ops are validated before anything is written).
  The default `durable: "local"` resolves once the batch is fsynced to the local WAL; `durable: "s3"` resolves once it is
  published to S3 (group-committed, one WAL object and one manifest per flush).
- After a crash, reopening with the same `localDir` recovers every locally committed batch; reopening with an empty
  `localDir` recovers exactly the batches published to S3 (`durableSeq()`), no more, each applied once. The usual
  pattern is to commit your data and a cursor in the same batch, and resume from the cursor.
- A WAL object uploaded but never named by a manifest (crash between the two PUTs) is never replayed from S3.
- Single writer per prefix (see [Single-writer fencing](#single-writer-fencing)). A superseded writer fails with
  `FencedError` instead of publishing.

Not guaranteed: anything committed `durable: "local"` survives loss of the local disk only if a later `s3` commit or
snapshot covered it; readers are not isolated from the writer process (there is one process, one store).

## Single-writer fencing

`Store.open` claims a new epoch (`previous + 1`) by creating the next manifest with `If-None-Match: *`. Every
manifest carries its writer's epoch, and manifests are created only with `If-None-Match: *`, so exactly one writer wins
each manifest number. A writer that loses a race, or finds its predecessor manifest gone, is fenced: pending and later
commits reject with `FencedError`, and `failed()` returns the reason (also after a local disk failure), so the process
can report unhealthy and exit. WAL, snapshot and segment object names include the epoch and a per-attempt random
nonce, so a fenced writer can never overwrite an object a live manifest refers to. Run one writer per prefix.

## S3 layout

Everything lives under the store prefix; numbers are zero-padded to 20 digits.

```
manifest/<seq>.json                               the only mutable-by-creation pointer; highest seq wins
wal/<firstSeq>-<lastSeq>-<epoch>.jsonl.gz         batches (gzipped length-prefixed JSON) not yet covered by a snapshot
snap/<batchSeq>-<manifestSeq>-<nonce>-<epoch>/    one Parquet file per table: <table>.parquet (state),
                                                  <table>.log.parquet (unsealed log rows)
seg/<table>/<manifestSeq>-<nonce>-<epoch>.parquet sealed log segments, sorted by (partition, sort key)
```

A manifest lists the snapshots, the WAL objects after them, and the sealed segments per log table. Recovery reads the
newest manifest, loads the snapshots, replays the WAL objects it names, then the local WAL tail of the same writer.
When that writer's local WAL still holds the batches of a WAL object (it keeps everything since its last snapshot), they
are replayed from disk and the object is not downloaded.
`snapshot()` writes all tables, publishes a manifest, then deletes every `wal/`, `snap/` and `seg/` object that
manifest does not name (only objects of its own epoch or older, so a newer writer's uploads survive) and all but the
newest 10 manifests, and prunes the local cache.

A snapshot captures the tables at one batch seq synchronously and cheaply (state tables are frozen and later writes go to
an overlay that reads see and that is merged back afterwards; log tables leave out rows appended after the capture), then
encodes them one 10,000-row group at a time, giving the event loop a turn between groups. Commits and reads go on
meanwhile; commits made during encoding are not in that snapshot and reach S3 through the WAL as usual.

Do not put other objects under a store's prefix: cleanup deletes what the manifest does not name.

## S3 compatibility

`S3ObjectStore` signs requests with SigV4 ([aws4fetch](https://github.com/mhart/aws4fetch)) and uses path-style URLs
(`<endpoint>/<bucket>/<key>`) by default; set `forcePathStyle: false` for virtual-hosted-style URLs
(`<scheme>://<bucket>.<endpoint host>/<key>`), which AWS requires for new buckets. Keys are URI-encoded per path segment
as SigV4 canonicalises them; empty keys, a leading `/`, `.` or `..` segments and control characters are refused.
`ListObjectsV2` responses are read with a small built-in XML parser (entities, CDATA, namespaces; a truncated page
without a continuation token is an error). The engine needs: `PUT` with `If-None-Match: *` (conditional create), ranged
`GET`, `HEAD`, `DELETE` and `ListObjectsV2`.

| Store | Status |
| --- | --- |
| Contabo Object Storage (Ceph RGW) | Tested; used by LayerHop |
| `MemoryObjectStore` (in-memory fake) | Tested; the whole offline test suite runs on it |
| AWS S3 | Should work with `forcePathStyle: false` (supports conditional writes); not tested yet, please report issues |
| Other Ceph RGW deployments | Should work; not tested yet, please report issues |
| MinIO | Should work; not tested yet, please report issues |
| Cloudflare R2 | Should work (region `auto`); not tested yet, please report issues |

Measured against Contabo (`eu2.contabostorage.com`):

- `If-Match` must carry the ETag without quotes (the listing and PUT responses return it unquoted).
- A lost conditional write answers 412 or 409; both map to `PreconditionFailed`. `If-None-Match: *` works.
- Keep at most 16 concurrent requests (`S3Config.maxConcurrency` defaults to 16). 5xx and network errors are retried
  3 times with backoff; a put retried after an error may already have landed, so the engine re-reads before treating a
  conditional failure as lost.
- Path-style URLs, SigV4 region `default`.

`MemoryObjectStore` reproduces these behaviours for tests; `failNext(n, status)` makes the next `n` calls throw an `S3Error`.

## Benchmarks

Measured on LayerHop's production server on 2026-10-30, on a Berachain dataset of 500k token balances and 120k swaps,
comparing the market indexer's queries on Hopstore with its previous Postgres setup (p50 latency):

| Query | Hopstore | Postgres |
| --- | --- | --- |
| Holders, top 100 | 4 ms | 32 ms |
| Trades | 4 ms | 24 ms |
| Candles | 4 ms | 15 ms |

In-process benchmark:

- ranked top-N: p99 0.005 ms
- log table latest-N: p99 0.022 ms
- longest event-loop gap during a snapshot: 70 to 79 ms

## Limits

- **Live state must fit in RAM.** State tables, ranked indexes and the in-memory part of log tables are fully resident;
  size the process for them. Only sealed log segments stay on S3.
- **One writer per prefix**, and no read replicas.
- **Node.js 20 or newer only.** No browser, Deno or Bun support is claimed.
- **Young project.** Running at LayerHop since 2026-10-30, as a shadow of its Postgres-backed indexer; the on-disk and on-S3 formats are versioned
  but may still change before 1.0.
- `sealLogs()` writes a full snapshot as part of the seal, so its cost grows with total state, not just the sealed rows.
- There is no segment compaction yet: segments accumulate as seals happen.
- A `scan()` without a partition opens every segment whose sort range overlaps the query (lazily, a few at a time).

## FAQ

**How is this different from Redis?** Redis is a separate server you reach over the network, with its own persistence
(RDB/AOF) on its disk, shared by many clients. Hopstore is a library inside one Node process: no network hop, no
server to run, and the durable copy is on object storage rather than a local disk. In exchange, only that one process
can write and read the data, and you get far fewer data structures.

**How is this different from SQLite with Litestream?** SQLite is a full SQL database on disk, and Litestream streams its
WAL to S3 for backup and restore. Hopstore keeps everything in memory without SQL, makes S3 the source of truth that it
recovers from on every open, and stores snapshots as Parquet you can query with other tools. If you need SQL, indexes
on arbitrary columns or data larger than RAM, SQLite is the better fit.

**How is this different from DuckDB over Parquet?** DuckDB is an analytical query engine; it can read Hopstore's
snapshots and segments directly. Hopstore is the write path and the low-latency read path: transactional commits,
point reads, top-N and latest-N in microseconds. They combine well: Hopstore for the live service, DuckDB for ad-hoc
analysis of the Parquet files.

**How is this different from SlateDB?** SlateDB is an embedded LSM-tree key-value store (Rust) that writes directly to
object storage and can hold data much larger than memory. Hopstore is a TypeScript engine for Node with everything
resident in memory, typed table kinds (state, ranked, log), a local fsynced WAL for low-latency commits, and Parquet
rather than SSTables on S3. Pick SlateDB when your data outgrows RAM or you work in Rust.

**Can I read the data without Hopstore?** Yes. Snapshots and segments are plain Parquet; the manifest is JSON; WAL
objects are gzipped length-prefixed JSON records (`[u32 big-endian length][UTF-8 JSON]`).

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
