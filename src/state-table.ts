/** State tables: latest JSON value per key, fully in memory, snapshotted as Parquet (`key`, `value` JSON text). */
import { deepFreeze, type Json, type Op } from "./codec.js";
import { readParquet, writeParquetRows } from "./parquet.js";

const DELETED = Symbol("deleted");

/** Read-only view of a state table (what `Store.stateTable` returns). Writes go through `Store.commit`. */
export interface StateTable<V extends Json = Json> {
  readonly name: string;
  readonly size: number;
  get(key: string): V | undefined;
  has(key: string): boolean;
  /** Every entry, in no particular order. Consume it synchronously (a commit may land between awaits). */
  entries(): IterableIterator<[string, V]>;
  keys(): IterableIterator<string>;
  values(): IterableIterator<V>;
  /** Entries whose key starts with `prefix`, in key order (sorts the matches on every call). */
  scan(prefix?: string): IterableIterator<[string, V]>;
}

/**
 * The state table itself, internal to the package: the store applies ops and loads snapshots through it. While a
 * snapshot encodes the rows (`freeze`), writes land in an overlay that every read sees and `thaw` merges back, so the
 * snapshot reads a fixed point in time.
 */
export class StateTableImpl<V extends Json = Json> implements StateTable<V> {
  private readonly base = new Map<string, V>();
  private overlay: Map<string, V | typeof DELETED> | null = null;
  /** keys added minus keys removed through the overlay */
  private added = 0;

  constructor(readonly name: string) {}

  get(key: string): V | undefined {
    const o = this.overlay?.get(key);
    return o === undefined ? this.base.get(key) : o === DELETED ? undefined : o;
  }

  has(key: string): boolean {
    return this.get(key) !== undefined;
  }

  get size(): number {
    return this.base.size + this.added;
  }

  /** Every entry, in no particular order. Consume it synchronously (a commit may land between awaits). */
  *entries(): IterableIterator<[string, V]> {
    const o = this.overlay;
    if (!o) {
      yield* this.base;
      return;
    }
    for (const [k, v] of this.base) {
      const w = o.get(k);
      if (w === undefined) yield [k, v];
      else if (w !== DELETED) yield [k, w];
    }
    for (const [k, w] of o) if (w !== DELETED && !this.base.has(k)) yield [k, w];
  }

  *keys(): IterableIterator<string> {
    for (const [k] of this.entries()) yield k;
  }

  *values(): IterableIterator<V> {
    for (const [, v] of this.entries()) yield v;
  }

  /** Entries whose key starts with `prefix`, in key order (sorts the matches on every call). */
  *scan(prefix = ""): IterableIterator<[string, V]> {
    const keys = [...this.keys()].filter((k) => k.startsWith(prefix)).sort();
    for (const k of keys) yield [k, this.get(k) as V];
  }

  /** Applies a put or del (the store validated the op); a put's value is frozen (see `deepFreeze`). */
  apply(op: Op): void {
    if (op.t !== "put" && op.t !== "del") throw new Error(`state table ${this.name} cannot take ${op.t}`);
    if (op.t === "put") deepFreeze(op.value);
    if (!this.overlay) {
      if (op.t === "put") this.base.set(op.key, op.value as V);
      else this.base.delete(op.key);
      return;
    }
    const had = this.has(op.key);
    if (op.t === "put") this.overlay.set(op.key, op.value as V);
    else if (had || this.base.has(op.key)) this.overlay.set(op.key, DELETED);
    this.added += (op.t === "put" ? 1 : 0) - (had ? 1 : 0);
  }

  /** Sets a row while loading a snapshot (frozen, like every value the table holds). */
  load(key: string, value: V): void {
    this.base.set(key, deepFreeze(value));
  }

  /** The rows as of now, unchanged until `thaw` (later writes go to the overlay). */
  freeze(): ReadonlyMap<string, V> {
    if (this.overlay) throw new Error(`state table ${this.name} is already frozen`);
    this.overlay = new Map();
    return this.base;
  }

  /** Merges the overlay into the rows; a no-op unless frozen. */
  thaw(): void {
    const o = this.overlay;
    if (!o) return;
    for (const [k, w] of o) {
      if (w === DELETED) this.base.delete(k);
      else this.base.set(k, w);
    }
    this.overlay = null;
    this.added = 0;
  }
}

/** Snapshot of one state table as Parquet, encoded a row group at a time (see `writeParquetRows`). */
export function writeStateSnapshot(table: string, rows: Iterable<[string, Json]>, expectedRows?: number): Promise<Uint8Array> {
  return writeParquetRows(
    [
      { name: "key", type: "STRING", nullable: false },
      { name: "value", type: "STRING", nullable: false },
    ],
    (function* () {
      const row: unknown[] = [];
      for (const [k, v] of rows) {
        row[0] = k;
        row[1] = JSON.stringify(v);
        yield row;
      }
    })(),
    () => ({ "hopstore.table": table }),
    expectedRows,
  );
}

export async function readStateSnapshot(buf: Uint8Array): Promise<{ table: string; rows: [string, Json][] }> {
  const { rows, metadata } = await readParquet(buf);
  const table = metadata["hopstore.table"];
  if (!table) throw new Error("state snapshot without hopstore.table");
  return { table, rows: rows.map((r) => [String(r.key), JSON.parse(String(r.value)) as Json]) };
}
