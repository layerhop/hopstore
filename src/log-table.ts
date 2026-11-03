/**
 * Log tables: append-only rows per partition, ordered by a sort key. Memory holds the newest `keepPerPartition` rows of
 * each partition (the tail) plus older rows waiting to be sealed; `Store.sealLogs` moves those (and, with `evict`, tail
 * rows that have aged out, e.g. of a pruned day) into a sorted Parquet segment on S3. Snapshots carry every unsealed row, so recovery never reads segments.
 * A table with a `codec` holds each row in memory as one packed string and decodes it on every read; the WAL, snapshots
 * and segments always hold the rows themselves.
 */
import { deepFreeze, type Json, type Op } from "./codec.js";
import type { SegmentRef } from "./manifest.js";
import { kvMetadata, readMetadata, readRows, ROW_GROUP_SIZE, writeParquetRows, type AsyncBuffer, type FileMetaData } from "./parquet.js";

export type SortKey = (string | number)[];

/**
 * How a log table holds its rows in memory: `pack` turns a row into one string (e.g. a compact binary encoding, much
 * smaller than an array of strings and numbers), `unpack` gives the row back. `unpack(pack(row))` must equal `row` as
 * JSON for every row the table takes (a commit checks it and refuses the batch otherwise).
 */
export interface RowCodec {
  pack(row: Json): string;
  unpack(packed: string): Json;
}

/** A declared log table. */
export interface LogSpec {
  name: string;
  /** Rows per partition that stay in memory for `latest` (older rows wait in memory until sealed). */
  keepPerPartition: number;
  sortKey: (row: Json) => SortKey;
  /**
   * Holds rows in memory packed (see RowCodec): reads return a decoded copy. Only memory changes, so a codec can be
   * added, changed or removed on an existing store.
   */
  codec?: RowCodec;
}

/** A row with its partition and sort key (internal to the package). */
export interface Entry {
  p: string;
  s: SortKey;
  /** the row; in memory, for a table with a codec, its packed string (unless the codec could not give it back) */
  row: Json;
  /** insertion stamp (set by `LogTable.insert`): a snapshot leaves out rows stamped after its capture */
  n?: number;
}

/** Tuple order: element by element (numbers before strings), a prefix sorts first. */
export function compareSort(a: SortKey, b: SortKey): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i]!;
    const y = b[i]!;
    if (typeof x !== typeof y) return typeof x === "number" ? -1 : 1;
    if (x < y) return -1;
    if (x > y) return 1;
  }
  return a.length - b.length;
}

const cmpStr = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
/** (partition, sort) order. */
export const compareEntry = (a: { p: string; s: SortKey }, b: { p: string; s: SortKey }) => cmpStr(a.p, b.p) || compareSort(a.s, b.s);

/** Inserts after any equal keys (appends in commit order stay in order); O(1) for in-order appends. */
function insertSorted(list: Entry[], e: Entry): void {
  if (!list.length || compareSort(list[list.length - 1]!.s, e.s) <= 0) {
    list.push(e);
    return;
  }
  let lo = 0;
  let hi = list.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (compareSort(list[mid]!.s, e.s) <= 0) lo = mid + 1;
    else hi = mid;
  }
  list.splice(lo, 0, e);
}

/** Merges two sorted lists. */
function mergeSorted(a: Entry[], b: Entry[]): Entry[] {
  const out: Entry[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) out.push(compareSort(b[j]!.s, a[i]!.s) < 0 ? b[j++]! : a[i++]!);
  while (i < a.length) out.push(a[i++]!);
  while (j < b.length) out.push(b[j++]!);
  return out;
}

interface Partition {
  /** the partition: every row in memory shares this one string */
  name: string;
  /** newest rows, ascending, at most keepPerPartition */
  tail: Entry[];
  /** older rows waiting for sealLogs, ascending */
  pending: Entry[];
}

/** How a log table reaches its segments; the store wires it up. */
export interface SegmentSource {
  segments(table: string): SegmentRef[];
  /** Marks these segments in use by a scan until the returned function is called (cleanup keeps them meanwhile). */
  lease?(refs: SegmentRef[]): () => void;
  /** A readable file for a segment (cached locally or read by range) and a function that releases it. */
  open(ref: SegmentRef): Promise<{ file: AsyncBuffer; close: () => Promise<void> }>;
}

const noSegments: SegmentSource = {
  segments: () => [],
  open: () => Promise.reject(new Error("hopstore: no segment source")),
};

/**
 * Which rows a seal takes (see `LogTable.sealable`, `Store.sealLogs`); the callbacks also get the row's partition and
 * table, and with `packed` a table's packed strings instead of decoded rows.
 */
export interface SealSelect {
  olderThan?: SortKey;
  where?: (row: Json, partition: string, table: string) => boolean;
  evict?: (row: Json, partition: string, table: string) => boolean;
  packed?: boolean;
}

/** First index in `list` (ascending) whose sort key is >= `s`. */
function lowerBound(list: Entry[], s: SortKey): number {
  let lo = 0;
  let hi = list.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (compareSort(list[mid]!.s, s) < 0) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** Read-only view of a log table (what `Store.logTable` returns). Appends go through `Store.commit` (`{ t: "append", table, partition, row }`). */
export interface LogTable<R extends Json = Json> {
  readonly name: string;
  /** Rows held in memory (tails plus rows waiting to be sealed). */
  readonly size: number;
  /** The newest `n` rows of a partition (only those `keep` accepts, if given), newest first; memory only, so at most `keepPerPartition`. */
  latest(partition: string, n: number, keep?: (row: R) => boolean): R[];
  /** Partitions with rows in memory. */
  partitions(): IterableIterator<string>;
  /** Every row of a partition held in memory (waiting to be sealed, then the tail), ascending by sort key. */
  memoryRows(partition: string): IterableIterator<R>;
  /** Whether memory (tail or waiting rows, not sealed segments) holds a row of `partition` with exactly this sort key. */
  has(partition: string, sortKey: SortKey): boolean;
  /** Rows with `from <= sortKey < to`, of one partition or all, in (partition, sortKey) order: memory merged with the segments. */
  scan(range: { partition?: string; from: SortKey; to: SortKey }): AsyncIterable<R>;
  /** `scan`, each row with its partition. */
  scanEntries(range: { partition?: string; from: SortKey; to: SortKey }): AsyncIterable<[partition: string, row: R]>;
  /** Every row of a partition held in memory, in no particular order and without copying the list. Do not commit to the table while iterating. */
  rowsInMemory(partition: string): IterableIterator<R>;
  /**
   * `rowsInMemory` as held: each row's packed string for a table with a codec (no decoding), else the rows. A row the
   * codec could not give back when it was loaded is held, and yielded, as the row itself.
   */
  packedRowsInMemory(partition: string): IterableIterator<Json>;
}

/** The log table itself, internal to the package: the store appends, snapshots and seals through it. */
export class LogTableImpl<R extends Json = Json> implements LogTable<R> {
  readonly parts = new Map<string, Partition>();
  source: SegmentSource = noSegments;
  private rows = 0;
  private stamp = 0;
  private readonly codec: RowCodec | undefined;
  /** Entries of a codec table holding the row itself: the codec did not give it back (null until there is one). */
  private unpacked: WeakSet<Entry> | null = null;

  constructor(readonly spec: LogSpec) {
    this.codec = spec.codec;
  }

  /** The row of an entry held in memory (decoded for a table with a codec). */
  rowOf(e: Entry): Json {
    return this.codec && !this.unpacked?.has(e) ? this.codec.unpack(e.row as string) : e.row;
  }

  /** The JSON text of an entry held in memory: what snapshots and segments store. */
  jsonOf(e: Entry): string {
    return JSON.stringify(this.rowOf(e));
  }

  /** The row packed, or null when the codec does not give it back exactly. */
  private pack(row: Json): string | null {
    const packed = this.codec!.pack(row);
    return JSON.stringify(this.codec!.unpack(packed)) === JSON.stringify(row) ? packed : null;
  }

  /** Throws unless the codec gives back exactly this row (as the WAL holds it: decoded from its JSON). */
  checkCodec(row: Json): void {
    if (!this.codec) return;
    const json = JSON.stringify(row);
    if (JSON.stringify(this.codec.unpack(this.codec.pack(JSON.parse(json) as Json))) !== json) throw new Error(`log table ${this.name}: codec does not give back the row ${json}`);
  }

  get name(): string {
    return this.spec.name;
  }

  /** Rows held in memory (tails plus rows waiting to be sealed). */
  get size(): number {
    return this.rows;
  }

  /** The newest `n` rows of a partition (only those `keep` accepts, if given), newest first; memory only, so at most `keepPerPartition`. */
  latest(partition: string, n: number, keep?: (row: R) => boolean): R[] {
    const tail = this.parts.get(partition)?.tail ?? [];
    const out: R[] = [];
    for (let i = tail.length - 1; i >= 0 && out.length < n; i--) {
      const row = this.rowOf(tail[i]!) as R;
      if (!keep || keep(row)) out.push(row);
    }
    return out;
  }

  /** Partitions with rows in memory. */
  partitions(): IterableIterator<string> {
    return this.parts.keys();
  }

  /** Every row of a partition held in memory (waiting to be sealed, then the tail), ascending by sort key. */
  *memoryRows(partition: string): IterableIterator<R> {
    const part = this.parts.get(partition);
    if (!part) return;
    for (const e of mergeSorted(part.pending, part.tail)) yield this.rowOf(e) as R;
  }

  /** Whether memory (tail or waiting rows, not sealed segments) holds a row of `partition` with exactly this sort key. */
  has(partition: string, sortKey: SortKey): boolean {
    const part = this.parts.get(partition);
    if (!part) return false;
    for (const list of [part.tail, part.pending]) {
      const at = lowerBound(list, sortKey);
      if (at < list.length && compareSort(list[at]!.s, sortKey) === 0) return true;
    }
    return false;
  }

  /**
   * Rows with `from <= sortKey < to`, of one partition or all, in (partition, sortKey) order: memory merged with the
   * segments whose partition and sort ranges overlap. Memory is captured when iteration starts. Segments are opened
   * lazily in order of their lowest possible row: the manifest bound first, then (after a footer-only probe that
   * releases the file) the bound of the first matching row group, so only segments overlapping the current position
   * are open at once. Scope scans to a partition: without one, every segment spanning the range of partitions overlaps
   * and stays open (one decoded row group each) until its last matching row.
   * Equal keys come out oldest first: segments in seal order, then memory.
   */
  async *scan(range: { partition?: string; from: SortKey; to: SortKey }): AsyncIterable<R> {
    for await (const e of this.scanAll(range)) yield e.row as R;
  }

  /** `scan`, each row with its partition. */
  async *scanEntries(range: { partition?: string; from: SortKey; to: SortKey }): AsyncIterable<[partition: string, row: R]> {
    for await (const e of this.scanAll(range)) yield [e.p, e.row as R];
  }

  /** Captured memory entries, their rows decoded (one at a time, as the merge reaches them). */
  private async *memoryStream(list: Entry[]): AsyncGenerator<Entry> {
    if (!this.codec) return yield* list;
    for (const e of list) yield { p: e.p, s: e.s, row: this.rowOf(e) };
  }

  private async *scanAll(range: { partition?: string; from: SortKey; to: SortKey }): AsyncGenerator<Entry> {
    const { partition, from, to } = range;
    if (compareSort(from, to) >= 0) return;
    const names = partition === undefined ? [...this.parts.keys()].sort(cmpStr) : this.parts.has(partition) ? [partition] : [];
    const memory: Entry[] = [];
    // each list is ascending: only the slice inside the range is merged (a narrow range copies little of a large memory)
    const slice = (list: Entry[]) => list.slice(lowerBound(list, from), lowerBound(list, to));
    for (const name of names) {
      const part = this.parts.get(name)!;
      for (const e of mergeSorted(slice(part.pending), slice(part.tail))) memory.push(e);
    }
    // a segment's rows (of `partition`, if given) are >= (partition ?? partitionMin, sortMin)
    const todo = this.source
      .segments(this.name)
      .map((ref, rank) => ({ ref, rank, low: { p: partition ?? ref.partitionMin, s: JSON.parse(ref.sortMin) as SortKey }, high: JSON.parse(ref.sortMax) as SortKey, meta: undefined as FileMetaData | undefined }))
      .filter(({ ref, high, low }) => (partition === undefined || (partition >= ref.partitionMin && partition <= ref.partitionMax)) && compareSort(high, from) >= 0 && compareSort(low.s, to) < 0)
      .sort((a, b) => compareEntry(a.low, b.low) || a.rank - b.rank);
    interface Stream {
      it: AsyncIterator<Entry>;
      head: Entry;
      rank: number;
    }
    const live: Stream[] = [];
    // captured with `todo`: a compaction may retire these segments, but none is deleted while this scan runs
    const release = this.source.lease?.(todo.map((x) => x.ref)) ?? (() => {});
    const add = async (it: AsyncIterator<Entry>, rank: number) => {
      const h = await it.next();
      if (!h.done) live.push({ it, head: h.value, rank });
    };
    const min = () => live.reduce<Stream | undefined>((m, x) => (!m || (compareEntry(x.head, m.head) || x.rank - m.rank) < 0 ? x : m), undefined);
    try {
      await add(this.memoryStream(memory), Number.POSITIVE_INFINITY); // memory loses ties: its rows are the newest
      for (;;) {
        let best = min();
        // open every segment that may hold a row at or before the current minimum
        while (todo.length && (!best || compareEntry(todo[0]!.low, best.head) <= 0)) {
          const next = todo.shift()!;
          if (next.meta) {
            await add(readSegment(this.source, next.ref, next.meta, partition, from, to)[Symbol.asyncIterator](), next.rank);
            best = min();
            continue;
          }
          const probe = await probeSegment(this.source, next.ref, partition, from, to);
          if (!probe) continue; // no row group can match
          next.meta = probe.meta;
          if (probe.low && compareEntry(probe.low, next.low) > 0) next.low = probe.low;
          let at = 0;
          while (at < todo.length && (compareEntry(todo[at]!.low, next.low) || todo[at]!.rank - next.rank) < 0) at++;
          todo.splice(at, 0, next);
        }
        if (!best) return;
        yield best.head;
        const n = await best.it.next();
        if (n.done) live.splice(live.indexOf(best), 1);
        else best.head = n.value;
      }
    } finally {
      await Promise.all(live.map((x) => x.it.return?.()));
      release();
    }
  }

  /** The sort key of a row; throws unless it is a non-empty array of strings and finite numbers. */
  sortKeyOf(row: Json): SortKey {
    const s = this.spec.sortKey(row);
    if (!Array.isArray(s) || !s.length || !s.every((x) => typeof x === "string" || (typeof x === "number" && Number.isFinite(x)))) {
      throw new Error(`log table ${this.name}: sort key must be a non-empty array of strings and finite numbers`);
    }
    return s;
  }

  /** Applies an append (the store validated it). */
  apply(op: Op): void {
    if (op.t !== "append") throw new Error(`log table ${this.name} cannot take ${op.t}`);
    this.insert({ p: op.partition, s: this.sortKeyOf(op.row), row: op.row });
  }

  /**
   * Adds a row (stamped for `unsealed`): packed by the codec when it gives the row back exactly (checked here too, as
   * recovery loads rows no commit validated), else frozen and held as it is (see `deepFreeze`).
   */
  insert(x: Entry): void {
    let part = this.parts.get(x.p);
    if (!part) this.parts.set(x.p, (part = { name: x.p, tail: [], pending: [] }));
    // a new entry of one shape: every row of a partition shares its name (each decoded op brings its own copy)
    const packed = this.codec ? this.pack(x.row) : null;
    const e: Entry = { p: part.name, s: x.s, row: packed ?? deepFreeze(x.row), n: ++this.stamp };
    if (this.codec && packed === null) (this.unpacked ??= new WeakSet()).add(e);
    insertSorted(part.tail, e);
    if (part.tail.length > this.spec.keepPerPartition) insertSorted(part.pending, part.tail.shift()!);
    this.rows++;
  }

  /**
   * Every unsealed row as of this call, in (partition, sort) order: what a snapshot stores. Captured now but
   * produced lazily (one partition at a time), leaving out rows appended later; no rows may be dropped meanwhile (seals
   * and snapshots run one at a time).
   */
  unsealed(): Iterable<Entry> {
    const names = [...this.parts.keys()].sort(cmpStr);
    const upto = this.stamp;
    const parts = this.parts;
    return (function* () {
      for (const name of names) {
        const part = parts.get(name);
        if (part) for (const e of mergeSorted(part.pending, part.tail)) if (e.n! <= upto) yield e;
      }
    })();
  }

  /**
   * Every row of a partition held in memory, in no particular order and without copying the list (waiting rows, then
   * the tail). Do not commit to the table while iterating.
   */
  *rowsInMemory(partition: string): IterableIterator<R> {
    for (const e of this.entriesInMemory(partition)) yield this.rowOf(e) as R;
  }

  /** `rowsInMemory` as held: each row's packed string for a table with a codec (no decoding), else the rows. */
  *packedRowsInMemory(partition: string): IterableIterator<Json> {
    for (const e of this.entriesInMemory(partition)) yield e.row;
  }

  private *entriesInMemory(partition: string): IterableIterator<Entry> {
    const part = this.parts.get(partition);
    if (!part) return;
    yield* part.pending;
    yield* part.tail;
  }

  /**
   * What a seal takes, in (partition, sort) order: waiting rows with sortKey < `olderThan` that `where`
   * accepts (each test skipped when omitted), plus any row in memory, the tail included, that `evict` accepts. The
   * callbacks get decoded rows, or with `packed` the rows as held (no decoding).
   */
  sealable(o: SealSelect = {}): Entry[] {
    const { olderThan, where, evict } = o;
    const row = o.packed ? (e: Entry) => e.row : (e: Entry) => this.rowOf(e);
    const out: Entry[] = [];
    for (const name of [...this.parts.keys()].sort(cmpStr)) {
      const part = this.parts.get(name)!;
      const waiting: Entry[] = [];
      for (const e of part.pending) {
        const old = !olderThan || compareSort(e.s, olderThan) < 0;
        if (!old && !evict) break; // ascending: nothing further is older
        if ((old && (!where || where(row(e), name, this.name))) || evict?.(row(e), name, this.name)) waiting.push(e);
      }
      const tail = evict ? part.tail.filter((e) => evict(row(e), name, this.name)) : [];
      for (const e of tail.length ? mergeSorted(waiting, tail) : waiting) out.push(e);
    }
    return out;
  }

  /** Drops rows that a published segment now holds (waiting rows, or tail rows taken by `evict`). */
  dropSealed(sealed: Entry[]): void {
    const gone = new Set(sealed);
    for (const p of new Set(sealed.map((e) => e.p))) {
      const part = this.parts.get(p);
      if (!part) continue;
      const before = part.pending.length + part.tail.length;
      part.pending = part.pending.filter((e) => !gone.has(e));
      part.tail = part.tail.filter((e) => !gone.has(e));
      this.rows -= before - part.pending.length - part.tail.length;
      if (!part.pending.length && !part.tail.length) this.parts.delete(p);
    }
  }
}

/** Row-group bounds kept in `hopstore.rowGroups`: [firstPartition, firstSort, lastPartition, lastSort] per group. */
type GroupBounds = [string, SortKey, string, SortKey];

/**
 * Writes rows sorted by (partition, sort) as Parquet: `partition`, `sort` (JSON array), `row` (JSON), row groups of
 * 10,000, metadata `hopstore.table` and the row-group bounds. Used for segments and for log snapshots; encoded a row
 * group at a time (see `writeParquetRows`). `json` gives a row's JSON text (`LogTableImpl.jsonOf` for entries held in
 * memory).
 */
export function writeLogParquet(table: string, entries: Iterable<Entry>, expectedRows?: number, json: (e: Entry) => string = (e) => JSON.stringify(e.row)): Promise<Uint8Array> {
  const groups: GroupBounds[] = [];
  let i = 0;
  const rows = function* () {
    const row: unknown[] = [];
    for (const e of entries) {
      if (i++ % ROW_GROUP_SIZE === 0) groups.push([e.p, e.s, e.p, e.s]);
      const g = groups[groups.length - 1]!;
      g[2] = e.p;
      g[3] = e.s;
      row[0] = e.p;
      row[1] = JSON.stringify(e.s);
      row[2] = json(e);
      yield row;
    }
  };
  return writeParquetRows(
    [
      { name: "partition", type: "STRING", nullable: false },
      { name: "sort", type: "STRING", nullable: false, statistics: false },
      { name: "row", type: "STRING", nullable: false, statistics: false },
    ],
    rows(),
    () => ({ "hopstore.table": table, "hopstore.rowGroups": JSON.stringify(groups) }),
    expectedRows,
  );
}

/** Manifest bounds of a segment holding `entries` (sorted, non-empty). */
export function segmentBounds(entries: Entry[]): Omit<SegmentRef, "key" | "size"> {
  let sortMin = entries[0]!.s;
  let sortMax = entries[0]!.s;
  for (const e of entries) {
    if (compareSort(e.s, sortMin) < 0) sortMin = e.s;
    if (compareSort(e.s, sortMax) > 0) sortMax = e.s;
  }
  return { partitionMin: entries[0]!.p, partitionMax: entries[entries.length - 1]!.p, rows: entries.length, sortMin: JSON.stringify(sortMin), sortMax: JSON.stringify(sortMax) };
}

const toEntry = (r: Record<string, unknown>): Entry => ({ p: String(r.partition), s: JSON.parse(String(r.sort)) as SortKey, row: JSON.parse(String(r.row)) as Json });

/** Every row of a log snapshot (or segment) file. */
export async function readLogParquet(file: AsyncBuffer): Promise<{ table: string; entries: Entry[] }> {
  const { table, groups } = await readLogParquetGroups(file);
  const entries: Entry[] = [];
  for await (const g of groups) for (const e of g) entries.push(e);
  return { table, entries };
}

/** The rows of a log snapshot (or segment) file one row group at a time, so a large file is never decoded at once. */
export async function readLogParquetGroups(file: AsyncBuffer): Promise<{ table: string; groups: AsyncIterable<Entry[]> }> {
  const meta = await readMetadata(file);
  const table = kvMetadata(meta)["hopstore.table"];
  if (!table) throw new Error("log parquet without hopstore.table");
  const groups = async function* () {
    let start = 0;
    for (const g of meta.row_groups) {
      const n = Number(g.num_rows);
      yield (await readRows(file, meta, start, start + n)).map(toEntry);
      start += n;
    }
  };
  return { table, groups: groups() };
}

const groupBounds = (meta: FileMetaData) => JSON.parse(kvMetadata(meta)["hopstore.rowGroups"] ?? "null") as GroupBounds[] | null;

/** Whether row group bounds `b` may hold rows of `partition` (any when undefined) within [from, to). */
const groupMatches = (b: GroupBounds | undefined, partition: string | undefined, from: SortKey, to: SortKey) =>
  partition === undefined || !b || (compareEntry({ p: b[2], s: b[3] }, { p: partition, s: from }) >= 0 && compareEntry({ p: b[0], s: b[1] }, { p: partition, s: to }) < 0);

/**
 * Reads only a segment's footer and releases the file: its metadata and a lower bound for its first matching row
 * (from the first matching row group), or null when no row group can match.
 */
async function probeSegment(source: SegmentSource, ref: SegmentRef, partition: string | undefined, from: SortKey, to: SortKey): Promise<{ meta: FileMetaData; low: { p: string; s: SortKey } | null } | null> {
  const { file, close } = await source.open(ref);
  try {
    const meta = await readMetadata(file);
    const bounds = groupBounds(meta);
    for (let g = 0; g < meta.row_groups.length; g++) {
      const b = bounds?.[g];
      if (!groupMatches(b, partition, from, to)) continue;
      return { meta, low: b ? { p: b[0], s: b[1] } : null };
    }
    return null;
  } finally {
    await close();
  }
}

/** Rows of one segment inside the range, in order; reads only the row groups whose bounds overlap it. */
async function* readSegment(source: SegmentSource, ref: SegmentRef, meta: FileMetaData, partition: string | undefined, from: SortKey, to: SortKey): AsyncGenerator<Entry> {
  const { file, close } = await source.open(ref);
  try {
    const bounds = groupBounds(meta);
    let start = 0;
    for (let g = 0; g < meta.row_groups.length; g++) {
      const n = Number(meta.row_groups[g]!.num_rows);
      if (groupMatches(bounds?.[g], partition, from, to)) {
        for (const r of await readRows(file, meta, start, start + n)) {
          const e = toEntry(r);
          if (partition !== undefined && compareEntry(e, { p: partition, s: to }) >= 0) return; // sorted: nothing further matches
          if ((partition === undefined || e.p === partition) && compareSort(e.s, from) >= 0 && compareSort(e.s, to) < 0) yield e;
        }
      }
      start += n;
    }
  } finally {
    await close();
  }
}
