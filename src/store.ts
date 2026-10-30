/**
 * The store: one writer per prefix. Commits go to the local WAL (fsync, one per group of commits in the same tick),
 * then become visible; a group-commit loop uploads the WAL tail to S3 and publishes it with a fenced manifest.
 * `open()` recovers (snapshots, the WAL objects named by the manifest, read from the local WAL of the same writer
 * when it has them, else from S3, then that local WAL's tail) and claims a new epoch.
 */
import { randomUUID } from "node:crypto";
import { mkdir, open as openFile, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { promisify } from "node:util";
import { gunzip, gzip } from "node:zlib";
import { decodeRecords, encodeRecords, type Batch, type Json, type Op } from "./codec.js";
import { LogTableImpl, readLogParquet, segmentBounds, writeLogParquet, type Entry, type LogSpec, type LogTable, type SegmentSource, type SortKey } from "./log-table.js";
import { createManifest, emptyManifest, highestManifestSeq, predecessorExists, readLatestManifest, segmentKey, snapshotDir, walKey, walLastSeq, type Manifest, type SegmentRef, type SnapshotRef } from "./manifest.js";
import { bytesBuffer, fileBuffer, objectBuffer, type AsyncBuffer } from "./parquet.js";
import type { ObjectStore } from "./s3.js";
import { RankedIndexImpl, type RankedIndex } from "./ranked.js";
import { readStateSnapshot, StateTableImpl, writeStateSnapshot, type StateTable } from "./state-table.js";
import { LocalWal, readLocalWals, removeLocalWals } from "./wal.js";

export type { Batch, Json, Op } from "./codec.js";
export type { StateTable } from "./state-table.js";
export type { RankedIndex } from "./ranked.js";
export { compareSort, type LogSpec, type LogTable, type SortKey } from "./log-table.js";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

/** A ranked index over a state table: each value maps to a (group, member, score); maintained on every put/del and rebuilt on recovery. */
export interface RankedSpec {
  name: string;
  /** a declared state table */
  table: string;
  group: (key: string, v: Json) => string;
  member: (key: string, v: Json) => string;
  score: (v: Json) => bigint;
  /**
   * Members whose score is below it are not ranked (left out of `top`, `count` and `sum`). Default `1n`: zero and
   * negative scores are left out, e.g. zero balances are not holders. `null` ranks every score. Indexes are rebuilt on
   * open, so changing it needs no migration.
   */
  minScore?: bigint | null;
}

/** Tables are declared up front; state and log table names share one namespace. */
export interface StoreTables {
  state?: string[];
  ranked?: RankedSpec[];
  log?: LogSpec[];
}

export interface StoreOptions {
  objects: ObjectStore;
  /**
   * Key prefix for every object of this store: "" or ending with "/", e.g. "myapp/prod/". Letters, digits, "/" and
   * `! - _ . * ' ( ) =` only; no empty, "." or ".." segments. `Store.open` rejects anything else.
   */
  prefix: string;
  /** Local directory (ideally a persistent volume) for the WAL and the snapshot and segment cache. */
  localDir: string;
  /** Group-commit interval (default 1000 ms). */
  flushMs?: number;
  /** Snapshot interval (default 10 minutes). */
  snapshotEveryMs?: number;
  tables?: StoreTables;
  /** Segments up to this size are fetched whole and cached under `<localDir>/cache/`; larger ones are read by range (default 64 MiB). */
  segmentWholeMaxBytes?: number;
  /** Background flush/snapshot failures (they are retried). Default: `console.error("hopstore:", e)`; pass a no-op to silence it. */
  onError?: (e: unknown) => void;
}

/** Which log rows `Store.sealLogs` moves to segments. */
export interface SealOptions {
  /** waiting rows: only those whose sort key is lower */
  olderThanSort?: SortKey;
  /** waiting rows: only those it accepts */
  where?: (row: Json) => boolean;
  /** any row in memory it accepts, the tail included (age-based eviction) */
  evict?: (row: Json) => boolean;
}

/** What `Store.describe` reports: this writer's view of the current manifest and its tables (read-only, no S3 calls). */
export interface StoreInfo {
  prefix: string;
  /** this writer's epoch */
  epoch: number;
  /** seq of the newest manifest this writer created or adopted */
  manifestSeq: number;
  /** last committed (visible) batch */
  seq: number;
  /** last batch published to S3 */
  durableSeq: number;
  /** batch the current snapshot was taken at (0: none yet) */
  snapshotSeq: number;
  /** WAL objects the manifest names (batches after the snapshot) */
  walObjects: number;
  state: Record<string, { rows: number }>;
  /** `memoryRows`: rows in memory; the rest describes the sealed segments the manifest names */
  log: Record<string, { memoryRows: number; segments: number; segmentRows: number; segmentBytes: number }>;
}

/** Another writer claimed a newer epoch: this one must stop. */
export class FencedError extends Error {}

/** Stored data names a table the options do not declare: open() refuses rather than silently dropping it. */
class UndeclaredTable extends Error {
  constructor(table: string) {
    super(`hopstore: stored data uses undeclared table ${table}; declare it in StoreOptions.tables`);
  }
}

/** A recovery read raced a cleanup of the previous writer; open() starts over. */
class RetryOpen extends Error {}

/** Every table the store holds, and the one place where ops are validated and applied (live commits and replay). */
class Tables {
  readonly state = new Map<string, StateTableImpl>();
  readonly ranked = new Map<string, RankedIndexImpl>();
  readonly log = new Map<string, LogTableImpl>();
  private readonly specs = new Map<string, RankedSpec[]>();

  constructor(t: StoreTables = {}) {
    for (const name of t.state ?? []) {
      if (!/^[A-Za-z0-9_-]+$/.test(name)) throw new Error(`bad table name ${name}`);
      if (this.state.has(name)) throw new Error(`table ${name} declared twice`);
      this.state.set(name, new StateTableImpl(name));
    }
    for (const r of t.ranked ?? []) {
      if (!/^[A-Za-z0-9_-]+$/.test(r.name)) throw new Error(`bad ranked index name ${r.name}`);
      if (this.ranked.has(r.name)) throw new Error(`ranked index ${r.name} declared twice`);
      if (!this.state.has(r.table)) throw new Error(`ranked index ${r.name} uses undeclared table ${r.table}`);
      if (r.minScore !== undefined && r.minScore !== null && typeof r.minScore !== "bigint") throw new Error(`ranked index ${r.name}: minScore must be a bigint or null`);
      this.ranked.set(r.name, new RankedIndexImpl(r.minScore));
      this.specs.set(r.table, [...(this.specs.get(r.table) ?? []), r]);
    }
    for (const l of t.log ?? []) {
      if (!/^[A-Za-z0-9_-]+$/.test(l.name)) throw new Error(`bad table name ${l.name}`);
      if (this.state.has(l.name) || this.log.has(l.name)) throw new Error(`table ${l.name} declared twice`);
      if (!Number.isInteger(l.keepPerPartition) || l.keepPerPartition < 0) throw new Error(`log table ${l.name}: keepPerPartition must be an integer >= 0`);
      this.log.set(l.name, new LogTableImpl(l));
    }
  }

  /** Rebuilds every ranked index from the state tables (after a snapshot load): one sort per group. */
  rebuildRanked(): void {
    for (const [table, specs] of this.specs) {
      for (const s of specs) {
        const groups = new Map<string, { member: string; score: bigint }[]>();
        for (const [k, v] of this.state.get(table)!.entries()) {
          const g = s.group(k, v);
          const list = groups.get(g) ?? [];
          groups.set(g, list);
          list.push({ member: s.member(k, v), score: s.score(v) });
        }
        const idx = this.ranked.get(s.name)!;
        for (const [g, list] of groups) idx.load(g, list);
      }
    }
  }

  validate(op: Op): void {
    if (op.t === "append") {
      const l = this.log.get(op.table);
      if (!l) throw new Error(`unknown log table ${op.table}`);
      if (typeof op.partition !== "string") throw new Error(`partition must be a string (table ${op.table})`);
      if (op.row === undefined) throw new Error(`append without row (table ${op.table})`);
      l.sortKeyOf(op.row); // dry run, like ranked entries
      return;
    }
    if (op.t !== "put" && op.t !== "del") throw new Error(`unknown op ${(op as { t: unknown }).t}`);
    if (!this.state.has(op.table)) throw new Error(`unknown table ${op.table}`);
    if (typeof op.key !== "string") throw new Error(`key must be a string (table ${op.table})`);
    if (op.t === "put" && op.value === undefined) throw new Error(`put without value (table ${op.table}, key ${op.key})`);
    if (op.t === "put") this.entries(op.table, op.key, op.value); // dry run: a bad value must not reach the WAL
  }

  /** Applies one op. An undeclared table fails (recovery must not drop data that a later snapshot would then delete). */
  apply(op: Op): void {
    if (op.t === "append") {
      const l = this.log.get(op.table);
      if (!l) throw new UndeclaredTable(op.table);
      return l.apply(op);
    }
    const t = this.state.get(op.table);
    if (!t) throw new UndeclaredTable(op.table);
    const specs = this.specs.get(op.table);
    if (!specs) return t.apply(op);
    // compute everything that can throw before mutating anything
    const next = op.t === "put" ? this.entries(op.table, op.key, op.value) : [];
    const old = t.get(op.key);
    const prev = old === undefined ? [] : this.entries(op.table, op.key, old);
    for (const e of prev) this.ranked.get(e.name)!.apply(e.group, e.member, null);
    t.apply(op);
    for (const e of next) this.ranked.get(e.name)!.apply(e.group, e.member, e.score);
  }

  /** The (group, member, score) each ranked index of `table` derives from a value. Throws if a callback does. */
  private entries(table: string, key: string, v: Json) {
    return (this.specs.get(table) ?? []).map((s) => {
      const score = s.score(v);
      if (typeof score !== "bigint") throw new Error(`ranked index ${s.name}: score must be a bigint (key ${key})`);
      return { name: s.name, group: s.group(key, v), member: s.member(key, v), score };
    });
  }

  applyBatch(b: Batch): void {
    for (const op of b.ops) this.apply(op);
  }
}

interface Queued {
  batch: Batch;
  bytes: Uint8Array;
  resolve: () => void;
  reject: (e: unknown) => void;
}

/** A seal whose segments a manifest may name: its rows leave memory the moment one does (ours, or adopted). */
interface SealAttempt {
  keys: Set<string>;
  sealed: { t: LogTableImpl; entries: Entry[] }[];
  /** set when the publish failed: the manifest seq it was creating (it may have landed anyway) */
  triedSeq?: number;
}

interface Waiter {
  seq: number;
  resolve: () => void;
  reject: (e: unknown) => void;
}

const concat = (parts: Uint8Array[]) => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.byteLength;
  }
  return out;
};

const notDurable = () => new Error("hopstore: store closed before the batch reached S3");
const epochOfKey = (key: string) => Number(/-(\d{20})(?:\.jsonl\.gz|\.parquet|\/)/.exec(key)?.[1] ?? Number.NaN);
const SEGMENT_WHOLE_MAX_BYTES = 64 * 1024 * 1024;
const cachePath = (o: StoreOptions, key: string) => join(o.localDir, "cache", key.slice(o.prefix.length));
const MANIFESTS_KEPT = 10;

/** Snapshot bytes from the local cache when key and size match, else from S3 (then cached). */
async function loadSnapshot(o: StoreOptions, { key, size }: SnapshotRef): Promise<Uint8Array> {
  const cached = cachePath(o, key);
  try {
    const local = new Uint8Array(await readFile(cached));
    if (local.byteLength === size) return local;
  } catch {
    // not cached
  }
  const body = await o.objects.get(key);
  if (!body) throw new RetryOpen(`snapshot ${key} is missing`);
  if (body.byteLength !== size) throw new Error(`hopstore: snapshot ${key} has ${body.byteLength} bytes, the manifest says ${size}`);
  await cacheFile(cached, body);
  return body;
}

async function cacheFile(path: string, body: Uint8Array) {
  try {
    await mkdir(dirname(path), { recursive: true });
    const tmp = `${path}.${randomUUID()}.tmp`; // concurrent readers may cache the same segment
    await writeFile(tmp, body);
    await rename(tmp, path);
  } catch {
    // the cache is an optimisation only
  }
}

function* withoutSealed(rows: Iterable<Entry>, gone: Set<Entry>): Iterable<Entry> {
  for (const e of rows) if (!gone.has(e)) yield e;
}

/**
 * Checks `prefix` and `localDir` before anything touches S3 or the disk. A prefix is "" or segments ending in "/", each
 * segment non-empty, not "." or "..", made of the characters S3 keys carry safely everywhere (letters, digits and
 * `! - _ . * ' ( ) =`): no leading "/", no "//", no spaces, backslashes, control or non-ASCII characters.
 */
function checkOptions(o: StoreOptions): void {
  const p = o.prefix;
  const bad = (why: string) => new Error(`hopstore: invalid prefix ${JSON.stringify(p)}: ${why}`);
  if (typeof p !== "string") throw bad("must be a string");
  if (p !== "") {
    if (!p.endsWith("/")) throw bad('must be "" or end with "/"');
    for (const seg of p.slice(0, -1).split("/")) {
      if (!seg) throw bad('must not start with "/" or contain "//"');
      if (seg === "." || seg === "..") throw bad('must not contain "." or ".." segments');
      if (!/^[A-Za-z0-9!_.*'()=-]+$/.test(seg)) throw bad("may only contain letters, digits, \"/\" and ! - _ . * ' ( ) =");
    }
  }
  if (typeof o.localDir !== "string" || !o.localDir.trim()) throw new Error("hopstore: localDir must be a non-empty path");
}

/** Rebuilds state from the newest manifest plus the local WAL tail of the writer that created it. */
async function recover(o: StoreOptions) {
  const manifest = (await readLatestManifest(o.objects, o.prefix)) ?? emptyManifest();
  const tables = new Tables(o.tables);
  const logSnapshots = manifest.logSnapshots ?? {};
  for (const name of Object.keys(manifest.snapshots)) if (!tables.state.has(name)) throw new UndeclaredTable(name);
  for (const name of [...Object.keys(logSnapshots), ...Object.keys(manifest.segments)]) if (!tables.log.has(name)) throw new UndeclaredTable(name);
  for (const [name, ref] of Object.entries(manifest.snapshots)) {
    const table = tables.state.get(name)!;
    for (const [k, v] of (await readStateSnapshot(await loadSnapshot(o, ref))).rows) table.load(k, v);
  }
  // unsealed log rows only: segments are never read back into memory
  for (const [name, ref] of Object.entries(logSnapshots)) {
    const table = tables.log.get(name)!;
    for (const e of (await readLogParquet(bytesBuffer(await loadSnapshot(o, ref)))).entries) table.insert({ p: e.p, s: table.sortKeyOf(e.row), row: e.row });
  }
  tables.rebuildRanked();
  let seq = manifest.snapshotSeq;
  const replay = (b: Batch, where: string) => {
    if (b.seq <= seq) return false;
    if (b.seq !== seq + 1) throw new Error(`hopstore: gap in ${where}: expected batch ${seq + 1}, found ${b.seq}`);
    tables.applyBatch(b);
    seq = b.seq;
    return true;
  };
  // the local WAL of the writer that created the manifest usually holds the same batches as its S3 WAL objects
  // (everything since its last snapshot): an object whose seqs it covers is replayed from disk, without a GET
  const local = (await readLocalWals(o.localDir)).filter((w) => w.header.epoch === manifest.epoch && w.header.writer === manifest.writer);
  const onDisk = new Map<number, Batch>();
  for (const w of local) for (const b of w.batches) onDisk.set(b.seq, b);
  const tail: Batch[] = [];
  const replayTail = (b: Batch, where: string) => replay(b, where) && tail.push(b);
  for (const key of manifest.wal) {
    const last = walLastSeq(key);
    const from = seq + 1;
    let covered = true;
    for (let s = from; s <= last && covered; s++) covered = onDisk.has(s);
    if (covered) {
      for (let s = from; s <= last; s++) replayTail(onDisk.get(s)!, key);
      continue;
    }
    const body = await o.objects.get(key);
    if (!body) throw new RetryOpen(`wal object ${key} is missing`);
    const raw = new Uint8Array(await gunzipAsync(body));
    const { records, validBytes } = decodeRecords(raw);
    if (validBytes !== raw.byteLength) throw new Error(`hopstore: corrupt wal object ${key}`);
    for (const b of records) replayTail(b, key);
  }
  if (seq < manifest.batchSeq) throw new Error(`hopstore: manifest ${manifest.seq} covers batch ${manifest.batchSeq} but its objects end at ${seq}`);
  const pending: Batch[] = [];
  for (const w of local) for (const b of w.batches) if (replay(b, w.file)) pending.push(b), tail.push(b);
  // `tail`: every batch after the snapshot (the new local WAL starts with it, so the next open needs no GETs either);
  // `pending`: those S3 does not have yet
  return { manifest, tables, seq, pending, tail };
}

/** A single-writer store over an object-store prefix. See the module comment. */
export class Store {
  private nextSeq: number;
  private queue: Queued[] = [];
  private writeScheduled = false;
  private localLane: Promise<void> = Promise.resolve();
  private s3Lane: Promise<void> = Promise.resolve();
  private flushTimer: NodeJS.Timeout | null = null;
  private waiters: Waiter[] = [];
  private fenced: FencedError | null = null;
  private failure: Error | null = null;
  private closed = false;
  /** close() has finished: nothing will flush any more */
  private shut = false;
  private seals: SealAttempt[] = [];
  private readonly warming = new Map<string, Promise<void>>();
  /** segment files open for scans right now, and the most at once (tests) */
  private readonly segmentStats = { open: 0, maxOpen: 0 };
  private readonly timers: NodeJS.Timeout[] = [];
  private readonly flushMs: number;
  private readonly onError: (e: unknown) => void;

  private constructor(
    private readonly o: StoreOptions,
    private readonly wal: LocalWal,
    private manifest: Manifest,
    private readonly tables: Tables,
    private applied: number,
    private pending: Batch[],
  ) {
    this.nextSeq = applied;
    this.flushMs = o.flushMs ?? 1000;
    this.onError = o.onError ?? ((e) => console.error("hopstore:", e));
    const every = (ms: number, fn: () => Promise<unknown>) => {
      const t = setInterval(() => void fn().catch((e) => this.fenced || this.onError(e)), ms);
      t.unref();
      this.timers.push(t);
    };
    every(this.flushMs, () => this.flushNow());
    every(o.snapshotEveryMs ?? 600_000, () => this.snapshotNow());
    const source: SegmentSource = { segments: (t) => this.manifest.segments[t] ?? [], open: (ref) => this.openSegment(ref) };
    for (const l of tables.log.values()) l.source = source;
  }

  /** Recovers state and claims the next epoch by creating the next manifest. */
  static async open(o: StoreOptions): Promise<Store> {
    checkOptions(o);
    await mkdir(o.localDir, { recursive: true });
    for (let attempt = 1; ; attempt++) {
      let r: Awaited<ReturnType<typeof recover>>;
      try {
        r = await recover(o);
      } catch (e) {
        if (e instanceof RetryOpen && attempt < 5) continue;
        throw e;
      }
      const writer = randomUUID();
      const epoch = r.manifest.epoch + 1;
      // the replayed tail goes into the new epoch's file before the claim, so it survives a crash right after it
      const wal = await LocalWal.create(o.localDir, { epoch, writer }, r.tail);
      const claim: Manifest = { ...r.manifest, seq: r.manifest.seq + 1, epoch, writer, createdAt: new Date().toISOString() };
      let res: Awaited<ReturnType<typeof createManifest>>;
      try {
        res = await createManifest(o.objects, o.prefix, claim);
      } catch (e) {
        await wal.close(); // keep both files: the claim may have landed, the next open sorts it out
        throw e;
      }
      if (res.ours && !(await predecessorExists(o.objects, o.prefix, claim.seq))) res = { ours: false, manifest: res.manifest };
      if (res.ours) {
        await removeLocalWals(o.localDir, wal.path);
        return new Store(o, wal, res.manifest, r.tables, r.seq, r.pending);
      }
      await wal.close();
      await rm(wal.path, { force: true });
      if (attempt >= 5) throw new FencedError(`hopstore: lost the epoch claim ${attempt} times at ${o.prefix}`);
    }
  }

  get epoch(): number {
    return this.manifest.epoch;
  }

  /** Last committed (visible) batch seq. */
  get seq(): number {
    return this.applied;
  }

  /**
   * Why this store stopped for good (fenced by a newer writer, or the local WAL failed), else null. A stopped store
   * rejects every commit and never flushes again: the process should exit and reopen.
   */
  failed(): string | null {
    return (this.fenced ?? this.failure)?.message ?? null;
  }

  /** Last batch seq published to S3. */
  durableSeq(): number {
    return this.manifest.batchSeq;
  }

  /** The current manifest's numbers and per-table counts, from memory (no S3 calls). */
  describe(): StoreInfo {
    const m = this.manifest;
    const log: StoreInfo["log"] = {};
    for (const [name, t] of this.tables.log) {
      const refs = m.segments[name] ?? [];
      log[name] = { memoryRows: t.size, segments: refs.length, segmentRows: refs.reduce((n, r) => n + r.rows, 0), segmentBytes: refs.reduce((n, r) => n + r.size, 0) };
    }
    return {
      prefix: this.o.prefix,
      epoch: m.epoch,
      manifestSeq: m.seq,
      seq: this.applied,
      durableSeq: m.batchSeq,
      snapshotSeq: m.snapshotSeq,
      walObjects: m.wal.length,
      state: Object.fromEntries([...this.tables.state].map(([name, t]) => [name, { rows: t.size }])),
      log,
    };
  }

  stateTable<V extends Json = Json>(name: string): StateTable<V> {
    const t = this.tables.state.get(name);
    if (!t) throw new Error(`unknown state table ${name}`);
    return t as unknown as StateTable<V>;
  }

  ranked(name: string): RankedIndex {
    const r = this.tables.ranked.get(name);
    if (!r) throw new Error(`unknown ranked index ${name}`);
    return r;
  }

  logTable<R extends Json = Json>(name: string): LogTable<R> {
    const l = this.tables.log.get(name);
    if (!l) throw new Error(`unknown log table ${name}`);
    return l as unknown as LogTable<R>;
  }

  /**
   * A readable segment: the local cache when key and size match, else range reads from S3 (only the footer and the row
   * groups a scan needs). An uncached segment up to `segmentWholeMaxBytes` is also fetched whole in the background to
   * warm the cache (at most two at a time), never on the scan path.
   */
  private async openSegment(ref: SegmentRef) {
    const stats = this.segmentStats;
    const opened = (file: AsyncBuffer, close: () => Promise<void>) => {
      stats.maxOpen = Math.max(stats.maxOpen, ++stats.open);
      let done = false;
      return { file, close: () => (done ? Promise.resolve() : ((done = true), stats.open--, close())) };
    };
    const path = cachePath(this.o, ref.key);
    try {
      const fh = await openFile(path, "r");
      if ((await fh.stat()).size === ref.size) return opened(fileBuffer(fh, ref.size), () => fh.close());
      await fh.close();
    } catch {
      // not cached
    }
    if (ref.size <= (this.o.segmentWholeMaxBytes ?? SEGMENT_WHOLE_MAX_BYTES) && this.warming.size < 2 && !this.warming.has(ref.key) && !this.closed) {
      const warm = (async () => {
        const body = await this.o.objects.get(ref.key);
        if (body?.byteLength === ref.size) await cacheFile(path, body);
      })()
        .catch(() => {}) // an optimisation only
        .finally(() => this.warming.delete(ref.key));
      this.warming.set(ref.key, warm);
    }
    return opened(objectBuffer(this.o.objects, ref.key, ref.size), async () => {});
  }

  /** Resolves once background cache warming has finished (tests, close). */
  private async settled(): Promise<void> {
    while (this.warming.size) await Promise.all(this.warming.values());
  }

  /**
   * Applies `ops` atomically and returns the batch seq. Resolves once the batch is fsynced locally and visible, or with
   * `durable: "s3"` once it is published to S3. The store keeps a frozen decoded copy of each value, so the caller's
   * objects stay theirs to mutate, and values read back are frozen.
   */
  commit(ops: Op[], opts: { durable?: "local" | "s3" } = {}): Promise<number> {
    const bad = this.fenced ?? this.failure ?? (this.closed ? new Error("hopstore: store is closed") : null);
    if (bad) return Promise.reject(bad);
    let batch: Batch;
    let bytes: Uint8Array;
    try {
      for (const op of ops) this.tables.validate(op);
      bytes = encodeRecords([{ seq: this.nextSeq + 1, ops }]); // throws on bigints and other non-JSON before a seq is used
      // memory gets exactly what the WAL holds (a decoded copy), never the caller's objects
      batch = decodeRecords(bytes).records[0]!;
    } catch (e) {
      return Promise.reject(e);
    }
    this.nextSeq = batch.seq;
    const local = new Promise<void>((resolve, reject) => this.queue.push({ batch, bytes, resolve, reject }));
    if (!this.writeScheduled) {
      this.writeScheduled = true;
      setImmediate(() => {
        this.writeScheduled = false;
        this.localLane = this.localLane.then(() => this.writeLocal());
      });
    }
    return local.then(() => (opts.durable === "s3" ? this.waitDurable(batch.seq) : undefined)).then(() => batch.seq);
  }

  /** Writes every queued batch with one fsync, then applies them in order. */
  private async writeLocal(): Promise<void> {
    const group = this.queue.splice(0);
    if (!group.length) return;
    const bad = this.fenced ?? this.failure;
    if (bad) return group.forEach((g) => g.reject(bad));
    try {
      await this.wal.append(concat(group.map((g) => g.bytes)));
    } catch (e) {
      this.failure = e instanceof Error ? e : new Error(String(e));
      for (const g of group) g.reject(this.failure);
      this.stop(this.failure);
      return;
    }
    for (const g of group) {
      this.tables.applyBatch(g.batch);
      this.applied = g.batch.seq;
      this.pending.push(g.batch);
      g.resolve();
    }
  }

  /**
   * Resolves once batch `seq` is published to S3 (what `commit(..., { durable: "s3" })` waits for): failed flushes are
   * retried; rejects only when the store is fenced or failed.
   */
  waitDurable(seq: number): Promise<void> {
    if (this.durableSeq() >= seq) return Promise.resolve();
    const bad = this.fenced ?? this.failure ?? (this.shut ? notDurable() : null);
    if (bad) return Promise.reject(bad);
    const p = new Promise<void>((resolve, reject) => this.waiters.push({ seq, resolve, reject }));
    this.flushSoon();
    return p;
  }

  /**
   * Flush on the next tick (grouping durable commits of this tick); while waiters remain after a failure, retry. Never
   * once closed, fenced or failed: those reject the waiters instead.
   */
  private flushSoon(delay = 0): void {
    if (this.flushTimer || this.closed || this.fenced || this.failure) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      if (this.closed) return; // close() runs the final flush itself; a retry queued behind it would outlive it
      this.flushNow().then(
        () => this.waiters.length && this.flushSoon(),
        (e) => {
          if (this.fenced || this.failure || this.closed) return;
          this.onError(e);
          if (this.waiters.length) this.flushSoon(Math.min(this.flushMs, 1000));
        },
      );
    }, delay); // ref'd: only scheduled while durable waiters exist, so a script awaiting its ack cannot exit early
  }

  private runS3<T>(fn: () => Promise<T>): Promise<T> {
    const p = this.s3Lane.then(fn);
    this.s3Lane = p.then(
      () => {},
      () => {},
    );
    return p;
  }

  /** Why a public flush, snapshot or seal cannot run: the FencedError once fenced, else "store is closed" after `close()`. */
  private unusable(): Error | null {
    return this.fenced ?? (this.closed ? new Error("hopstore: store is closed") : null);
  }

  /** Uploads the not-yet-durable batches as one WAL object and publishes a manifest naming it. Rejects once closed or fenced. */
  flush(): Promise<void> {
    const bad = this.unusable();
    return bad ? Promise.reject(bad) : this.flushNow();
  }

  private flushNow(): Promise<void> {
    return this.runS3(async () => {
      if (this.fenced) throw this.fenced;
      if (!this.pending.length) return;
      const batches = this.pending.slice();
      const first = batches[0]!.seq;
      const last = batches[batches.length - 1]!.seq;
      const key = walKey(this.o.prefix, first, last, this.epoch);
      await this.o.objects.put(key, new Uint8Array(await gzipAsync(encodeRecords(batches))), { contentType: "application/gzip" });
      await this.publish((m) => (m.batchSeq >= last ? null : { ...m, batchSeq: last, wal: [...m.wal, key] }));
    });
  }

  /**
   * Creates manifests until `build` has nothing left to add. A manifest at the next seq that turns out to be ours (an
   * earlier attempt that reported a failure but landed) is adopted and built upon; one from another writer fences us.
   */
  private async publish(build: (cur: Manifest) => Manifest | null): Promise<void> {
    for (;;) {
      const next = build(this.manifest);
      if (!next) return;
      const m: Manifest = { ...next, seq: this.manifest.seq + 1, epoch: this.epoch, writer: this.manifest.writer, createdAt: new Date().toISOString() };
      const res = await createManifest(this.o.objects, this.o.prefix, m);
      if (!res.ours) this.fence(`epoch ${res.manifest.epoch} wrote manifest ${m.seq} (we are epoch ${this.epoch})`);
      if (!(await predecessorExists(this.o.objects, this.o.prefix, m.seq))) this.fence(`manifest ${m.seq - 1} is gone: a newer writer is far ahead`);
      this.manifest = res.manifest;
      this.reconcileSeals(); // synchronously with the swap: no reader sees a row both in memory and in a segment
      const durable = res.manifest.batchSeq;
      this.pending = this.pending.filter((b) => b.seq > durable);
      this.waiters = this.waiters.filter((w) => (w.seq <= durable ? (w.resolve(), false) : true));
    }
  }

  /**
   * Drops the rows of every seal the current manifest names (our publish, or one adopted by any later publish after a
   * reported failure that had landed) and forgets failed seals that can no longer land: we have since created their slot.
   */
  private reconcileSeals(): void {
    if (!this.seals.length) return;
    const named = new Set(Object.values(this.manifest.segments).flatMap((refs) => refs.map((r) => r.key)));
    this.seals = this.seals.filter((a) => {
      if ([...a.keys].some((k) => named.has(k))) {
        for (const { t, entries } of a.sealed) t.dropSealed(entries);
        return false;
      }
      return a.triedSeq === undefined || this.manifest.seq < a.triedSeq;
    });
  }

  private fence(why: string): never {
    const err = new FencedError(`hopstore: fenced at ${this.o.prefix}: ${why}`);
    this.fenced = err;
    this.stop(err);
    throw err;
  }

  /** Rejects everything waiting and stops the timers (fenced or the local disk failed). */
  private stop(err: Error): void {
    for (const t of this.timers) clearInterval(t);
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.flushTimer = null;
    for (const w of this.waiters.splice(0)) w.reject(err);
    for (const q of this.queue.splice(0)) q.reject(err);
  }

  /**
   * Writes every table (state, and the unsealed rows of log tables) as Parquet at the current seq, publishes it, then
   * drops older WAL objects and snapshots. Rejects once closed or fenced.
   */
  snapshot(): Promise<void> {
    const bad = this.unusable();
    return bad ? Promise.reject(bad) : this.snapshotNow();
  }

  private snapshotNow(): Promise<void> {
    return this.runS3(async () => void (await this.checkpoint(null)));
  }

  /**
   * Moves the rows waiting in memory (beyond each partition's `keepPerPartition`; with `olderThanSort`, only those
   * whose sort key is lower; with `where`, only those it accepts) into one sorted Parquet segment per log table. `evict`
   * also takes any row in memory it accepts, the tail included: age-based eviction (rows of a pruned day leave memory
   * even when they are among the newest of their partition). The segments are published together with a snapshot that
   * no longer holds those rows, so WAL replay can never bring them back; memory drops them only after the manifest is
   * published. Returns the number of rows sealed. The callbacks run synchronously when the seal starts. Rejects once
   * closed or fenced.
   */
  sealLogs(opts: SealOptions = {}): Promise<number> {
    const bad = this.unusable();
    if (bad) return Promise.reject(bad);
    return this.runS3(() => this.checkpoint(opts));
  }

  /** Runs a checkpoint; starts over when an adopted manifest turned out to hold an earlier seal (memory changed under it). */
  private async checkpoint(seal: SealOptions | null): Promise<number> {
    for (let round = 1; ; round++) {
      const n = await this.checkpointOnce(seal);
      if (n !== "superseded") return n;
      if (round >= 3) throw new Error("hopstore: checkpoint superseded three times");
    }
  }

  private async checkpointOnce(seal: SealOptions | null): Promise<number | "superseded"> {
    if (this.fenced) throw this.fenced;
    const { prefix } = this.o;
    const seq = this.applied;
    // captured synchronously: the tables are exactly the state at `seq`, and the earlier seals still in doubt
    const sealed = seal ? [...this.tables.log.values()].map((t) => ({ t, entries: t.sealable({ olderThan: seal.olderThanSort, where: seal.where, evict: seal.evict }) })).filter((x) => x.entries.length) : [];
    if (seal ? !sealed.length : seq <= this.manifest.snapshotSeq) return 0;
    const inDoubt = new Set(this.seals.flatMap((a) => [...a.keys]));
    const mseq = this.manifest.seq + 1;
    const nonce = randomUUID().slice(0, 8);
    const dir = snapshotDir(prefix, seq, mseq, nonce, this.epoch);
    const gone = new Set<Entry>(sealed.flatMap((x) => x.entries));
    // also captured now, encoded below a row group at a time while commits go on: state tables are frozen (writes go
    // to an overlay until thawed), log tables leave out rows appended after this point
    const frozen = [...this.tables.state].map(([name, t]) => ({ name, t, rows: t.freeze() }));
    const unsealed = [...this.tables.log].map(([name, t]) => ({ name, rows: t.unsealed(), n: t.size }));
    const state: { name: string; key: string; body: Uint8Array }[] = [];
    const logs: typeof state = [];
    const segs: (typeof state[number] & { ref: SegmentRef })[] = [];
    try {
      for (const { name, t, rows } of frozen) {
        state.push({ name, key: `${dir}${name}.parquet`, body: await writeStateSnapshot(name, rows, rows.size) });
        t.thaw();
      }
      for (const { name, rows, n } of unsealed) logs.push({ name, key: `${dir}${name}.log.parquet`, body: await writeLogParquet(name, withoutSealed(rows, gone), n - gone.size) });
      for (const { t, entries } of sealed) {
        const key = segmentKey(prefix, t.name, mseq, nonce, this.epoch);
        const body = await writeLogParquet(t.name, entries, entries.length);
        segs.push({ name: t.name, key, body, ref: { key, size: body.byteLength, ...segmentBounds(entries) } });
      }
    } finally {
      for (const { t } of frozen) t.thaw();
    }
    const all = [...state, ...logs, ...segs];
    await Promise.all(all.map((f) => this.o.objects.put(f.key, f.body, { contentType: "application/vnd.apache.parquet" })));
    await Promise.all(all.map((f) => cacheFile(cachePath(this.o, f.key), f.body)));
    const ref = (f: { key: string; body: Uint8Array }): SnapshotRef => ({ key: f.key, size: f.body.byteLength });
    const ours = new Set(all.map((f) => f.key));
    const attempt: SealAttempt = { keys: new Set(segs.map((f) => f.key)), sealed };
    if (segs.length) this.seals.push(attempt);
    let superseded = false;
    try {
      await this.publish((m) => {
        const named = [...Object.values(m.snapshots), ...Object.values(m.logSnapshots ?? {}), ...Object.values(m.segments).flat()];
        if (named.some((r) => ours.has(r.key))) return null; // published (by us, or adopted after a reported failure)
        // an adopted manifest holds a seal we were unsure of: our capture still counts those rows as unsealed
        if (named.some((r) => inDoubt.has(r.key))) return (superseded = true), null;
        if (!seal && m.snapshotSeq >= seq) return null;
        const segments = { ...m.segments };
        for (const s of segs) segments[s.name] = [...(segments[s.name] ?? []), s.ref];
        return {
          ...m,
          snapshotSeq: seq,
          batchSeq: Math.max(m.batchSeq, seq),
          snapshots: Object.fromEntries(state.map((f) => [f.name, ref(f)])),
          logSnapshots: Object.fromEntries(logs.map((f) => [f.name, ref(f)])),
          segments,
          wal: m.wal.filter((k) => walLastSeq(k) > seq),
        };
      });
    } catch (e) {
      if (this.seals.includes(attempt)) attempt.triedSeq = this.manifest.seq + 1;
      throw e;
    }
    if (superseded) {
      this.seals = this.seals.filter((a) => a !== attempt); // never published; cleanup removes its objects
      return "superseded";
    }
    // the local WAL only needs what S3 does not have yet
    const rewrite = this.localLane.then(() => this.wal.rewrite(this.pending));
    this.localLane = rewrite.catch(() => {});
    await rewrite;
    await this.cleanup();
    return gone.size;
  }

  /**
   * Deletes objects the current manifest no longer needs. Only objects of our epoch or older: a newer writer's fresh
   * uploads (not yet in any manifest) must survive.
   */
  private async cleanup(): Promise<void> {
    const { prefix } = this.o;
    const highest = await highestManifestSeq(this.o.objects, prefix);
    if (highest !== this.manifest.seq) this.fence(`manifest ${highest} is newer than ours (${this.manifest.seq})`);
    const snaps = [...Object.values(this.manifest.snapshots), ...Object.values(this.manifest.logSnapshots ?? {})];
    const segs = Object.values(this.manifest.segments).flat();
    const live = new Set([...this.manifest.wal, ...snaps.map((r) => r.key), ...segs.map((r) => r.key)]);
    const doomed: string[] = [];
    for (const sub of ["wal/", "snap/", "seg/"]) {
      for (const { key } of await this.o.objects.list(prefix + sub)) if (!live.has(key) && epochOfKey(key) <= this.epoch) doomed.push(key);
    }
    for (const { key } of await this.o.objects.list(`${prefix}manifest/`)) {
      const n = Number(/(\d{20})\.json$/.exec(key)?.[1]);
      if (n <= this.manifest.seq - MANIFESTS_KEPT) doomed.push(key);
    }
    for (let i = 0; i < doomed.length; i += 8) await Promise.all(doomed.slice(i, i + 8).map((k) => this.o.objects.delete(k)));
    // local snapshot cache: keep only the current snapshot directory
    const cacheSnap = join(this.o.localDir, "cache", "snap");
    const keep = new Set(snaps.map(({ key: k }) => k.slice(`${prefix}snap/`.length).split("/")[0]));
    for (const d of await readdir(cacheSnap).catch(() => [] as string[])) if (!keep.has(d)) await rm(join(cacheSnap, d), { recursive: true, force: true });
    // local segment cache: drop files of segments the manifest no longer names (and stray temp files)
    const cachedSegs = new Set(segs.map((r) => cachePath(this.o, r.key)));
    const cacheSeg = join(this.o.localDir, "cache", "seg");
    for (const t of await readdir(cacheSeg).catch(() => [] as string[])) {
      for (const f of await readdir(join(cacheSeg, t)).catch(() => [] as string[])) if (!cachedSegs.has(join(cacheSeg, t, f))) await rm(join(cacheSeg, t, f), { force: true });
    }
  }

  /** Flushes and snapshots (unless fenced), stops the timers and closes the local WAL. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const t of this.timers) clearInterval(t);
    this.localLane = this.localLane.then(() => this.writeLocal());
    await this.localLane;
    try {
      if (!this.fenced && !this.failure) {
        await this.flushNow();
        await this.snapshotNow();
      }
    } finally {
      if (this.flushTimer) clearTimeout(this.flushTimer);
      this.flushTimer = null;
      this.shut = true;
      for (const w of this.waiters.splice(0)) w.reject(notDurable()); // the final flush failed: nothing retries now
      await this.settled();
      await this.wal.close();
    }
  }
}
