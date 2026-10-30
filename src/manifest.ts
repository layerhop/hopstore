/**
 * Manifests: `<prefix>manifest/<seq 20 digits>.json`, each created with If-None-Match: *, so exactly one writer can
 * create each number. `seq` is the manifest's own dense number; `batchSeq` is the last batch it makes durable.
 */
import { PreconditionFailed, type ObjectStore } from "./s3.js";

/** A sealed log segment: rows sorted by (partition, sort key); `sortMin`/`sortMax` are JSON sort keys over all its rows. */
export interface SegmentRef {
  key: string;
  partitionMin: string;
  partitionMax: string;
  rows: number;
  sortMin: string;
  sortMax: string;
  /** bytes, so a local cached copy is trusted only when it matches */
  size: number;
}

export interface SnapshotRef {
  key: string;
  size: number;
}

export interface Manifest {
  version: 1;
  /** Manifest number (matches the object key). */
  seq: number;
  /** Epoch of the writer that created it; every open claims epoch + 1. */
  epoch: number;
  /** Random id of that writer, so a local WAL file is replayed only by the chain that wrote it. */
  writer: string;
  /** Last batch seq covered by snapshots + WAL objects. */
  batchSeq: number;
  snapshotSeq: number;
  /** table -> snapshot object (size lets recovery trust a local cached copy only when it matches) */
  snapshots: Record<string, SnapshotRef>;
  /** log table -> snapshot of its unsealed rows (absent in manifests written before log tables existed) */
  logSnapshots?: Record<string, SnapshotRef>;
  /** WAL object keys in order; they may overlap the snapshot or each other (replay skips seqs already applied). */
  wal: string[];
  /** log table -> sealed segments, oldest first */
  segments: Record<string, SegmentRef[]>;
  createdAt: string;
}

const pad = (n: number) => String(n).padStart(20, "0");

export const manifestKey = (prefix: string, seq: number) => `${prefix}manifest/${pad(seq)}.json`;

/**
 * WAL object key. The writer's epoch is part of the name (beyond `<firstSeq>-<lastSeq>`) so that a fenced writer that
 * has not noticed yet can never overwrite an object the current writer's manifest points at.
 */
export const walKey = (prefix: string, first: number, last: number, epoch: number) => `${prefix}wal/${pad(first)}-${pad(last)}-${pad(epoch)}.jsonl.gz`;

/**
 * Snapshot directory key for state tables (`<table>.parquet`) and log tables (`<table>.log.parquet`): batch seq, the
 * manifest seq meant to publish it, a per-attempt nonce, and the epoch (as in `walKey`). The nonce makes every attempt
 * write fresh objects: a retry after a failed publish that in fact landed must never overwrite what that manifest names.
 */
export const snapshotDir = (prefix: string, seq: number, manifestSeq: number, nonce: string, epoch: number) => `${prefix}snap/${pad(seq)}-${pad(manifestSeq)}-${nonce}-${pad(epoch)}/`;

/** Segment key; same parts and reasons as `snapshotDir`. */
export const segmentKey = (prefix: string, table: string, manifestSeq: number, nonce: string, epoch: number) => `${prefix}seg/${table}/${pad(manifestSeq)}-${nonce}-${pad(epoch)}.parquet`;

/** Last batch seq inside a WAL object key. */
export function walLastSeq(key: string): number {
  const m = /\/wal\/\d{20}-(\d{20})-\d{20}\.jsonl\.gz$/.exec(key);
  if (!m?.[1]) throw new Error(`bad wal key ${key}`);
  return Number(m[1]);
}

export const emptyManifest = (): Manifest => ({
  version: 1,
  seq: 0,
  epoch: 0,
  writer: "",
  batchSeq: 0,
  snapshotSeq: 0,
  snapshots: {},
  logSnapshots: {},
  wal: [],
  segments: {},
  createdAt: new Date(0).toISOString(),
});

const parse = (b: Uint8Array): Manifest => {
  const m = JSON.parse(new TextDecoder().decode(b)) as Manifest;
  if (m.version !== 1) throw new Error(`unsupported manifest version ${m.version}`);
  return m;
};

/** The newest manifest, or null for a new store. A manifest deleted between LIST and GET means a re-LIST, never an older one. */
export async function readLatestManifest(objects: ObjectStore, prefix: string): Promise<Manifest | null> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const keys = (await objects.list(`${prefix}manifest/`)).map((o) => o.key).filter((k) => /\/manifest\/\d{20}\.json$/.test(k));
    const newest = keys[keys.length - 1];
    if (!newest) return null;
    const body = await objects.get(newest);
    if (body) return parse(body);
  }
  throw new Error(`hopstore: the newest manifest under ${prefix} keeps disappearing`);
}

/** Highest manifest number under the prefix (0 when there is none). */
export async function highestManifestSeq(objects: ObjectStore, prefix: string): Promise<number> {
  const keys = (await objects.list(`${prefix}manifest/`)).map((o) => /\/manifest\/(\d{20})\.json$/.exec(o.key)?.[1]).filter((k) => k !== undefined);
  return keys.length ? Number(keys[keys.length - 1]) : 0;
}

export async function readManifest(objects: ObjectStore, prefix: string, seq: number): Promise<Manifest | null> {
  const body = await objects.get(manifestKey(prefix, seq));
  return body ? parse(body) : null;
}

/**
 * Creates manifest `m.seq` and returns the manifest now stored at that seq. `ours` is true when it names our epoch and
 * writer: either this put won, or an earlier attempt that reported a 5xx had in fact succeeded (then `manifest` is that
 * earlier content, which the caller adopts before trying the next seq). `ours: false` means another writer won: fenced.
 */
export async function createManifest(objects: ObjectStore, prefix: string, m: Manifest): Promise<{ ours: boolean; manifest: Manifest }> {
  try {
    await objects.put(manifestKey(prefix, m.seq), new TextEncoder().encode(JSON.stringify(m)), { ifNoneMatch: "*", contentType: "application/json" });
    return { ours: true, manifest: m };
  } catch (e) {
    if (!(e instanceof PreconditionFailed)) throw e;
    const there = await readManifest(objects, prefix, m.seq);
    if (!there) throw e; // deleted again since: transient, the caller retries
    return { ours: there.epoch === m.epoch && there.writer === m.writer, manifest: there };
  }
}

/**
 * True when manifest `seq - 1` still exists. Cleanup keeps the newest manifests, so a missing predecessor means another
 * writer has moved far ahead and deleted old slots: the create at `seq` only "won" because the slot was free again.
 */
export async function predecessorExists(objects: ObjectStore, prefix: string, seq: number): Promise<boolean> {
  return seq <= 1 || (await objects.head(manifestKey(prefix, seq - 1))) !== null;
}
