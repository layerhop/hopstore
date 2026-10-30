/** Parquet helpers for snapshots and segments: `hyparquet-writer` to write, `hyparquet` to read, `hopstore.*` key-value metadata. */
import type { FileHandle } from "node:fs/promises";
import { cachedAsyncBuffer, parquetMetadata, parquetMetadataAsync, parquetReadObjects, type AsyncBuffer, type FileMetaData } from "hyparquet";
import { ByteWriter, ParquetWriter, schemaFromColumnData, type ColumnSource } from "hyparquet-writer";
import type { ObjectStore } from "./s3.js";

export type { AsyncBuffer, FileMetaData } from "hyparquet";

/** Format version written as `hopstore.version`. */
export const PARQUET_VERSION = "1";
/** Rows per row group in snapshots and segments. */
export const ROW_GROUP_SIZE = 10_000;

/** A column of `writeParquetRows`: its name, type and options, without data. */
export type ColumnSpec = Omit<ColumnSource, "data">;

const nextTick = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * One buffer for the whole file, sized from the first row group and the expected row count: a large file is not
 * copied (and its old copies left for the GC) every time it doubles. Big short-lived buffers are what push V8 into a
 * full collection, a pause of its own on a large heap.
 */
class FileWriter extends ByteWriter {
  /** Grows the buffer to `bytes` at once (a no-op when it is already that large). */
  reserve(bytes: number): void {
    if (bytes <= this.buffer.byteLength) return;
    const next = new ArrayBuffer(bytes);
    new Uint8Array(next).set(new Uint8Array(this.buffer, 0, this.index));
    this.buffer = next;
    this.view = new DataView(next);
  }
}

/**
 * Writes rows (one value per column each, consumed lazily; a producer may reuse one array for every row) plus
 * key-value metadata (`hopstore.version` is always added, `metadata` is called after the last row) in row groups of
 * `ROW_GROUP_SIZE`. One row group is built and encoded at a time and the event loop gets a turn between them, so large
 * snapshots do not stall the process. The bytes are those of a single `parquetWriteBuffer` call over the same columns;
 * `expectedRows` (a hint) sizes the buffer. The result may be a view of a slightly larger buffer.
 */
export async function writeParquetRows(columns: ColumnSpec[], rows: Iterable<unknown[]>, metadata: () => Record<string, string>, expectedRows = 0): Promise<Uint8Array> {
  const writer = new FileWriter();
  const pq = new ParquetWriter({ writer, schema: schemaFromColumnData({ columnData: columns.map((c) => ({ ...c, data: [] })) }) });
  let data = columns.map((): unknown[] => []);
  let groups = 0;
  const writeGroup = () => {
    pq.write({ columnData: columns.map((c, i) => ({ ...c, data: data[i]! })), rowGroupSize: ROW_GROUP_SIZE });
    data = columns.map(() => []);
    if (++groups === 1 && expectedRows > ROW_GROUP_SIZE) writer.reserve(Math.ceil((writer.index / ROW_GROUP_SIZE) * expectedRows * 1.05) + 65_536);
  };
  for (const row of rows) {
    for (let i = 0; i < data.length; i++) data[i]!.push(row[i]);
    if (data[0]!.length === ROW_GROUP_SIZE) {
      writeGroup();
      await nextTick();
    }
  }
  if (data[0]!.length) writeGroup();
  pq.kvMetadata = Object.entries({ ...metadata(), "hopstore.version": PARQUET_VERSION }).map(([key, value]) => ({ key, value }));
  pq.finish();
  return writer.getBytes();
}

/** The `hopstore.*` key-value metadata of a file; throws on another format version. */
export function kvMetadata(meta: FileMetaData): Record<string, string> {
  const out: Record<string, string> = {};
  for (const kv of meta.key_value_metadata ?? []) if (kv.value !== undefined) out[kv.key] = kv.value;
  if (out["hopstore.version"] !== PARQUET_VERSION) throw new Error(`unsupported hopstore parquet version ${out["hopstore.version"]}`);
  return out;
}

/** Reads every row as an object plus the file's key-value metadata. */
export async function readParquet(buf: Uint8Array): Promise<{ rows: Record<string, unknown>[]; metadata: Record<string, string> }> {
  const file = toArrayBuffer(buf);
  const meta = parquetMetadata(file);
  const metadata = kvMetadata(meta);
  return { rows: await parquetReadObjects({ file, metadata: meta }), metadata };
}

/** Footer metadata of a (possibly remote) file. */
export const readMetadata = (file: AsyncBuffer): Promise<FileMetaData> => parquetMetadataAsync(file);

/** Rows `[rowStart, rowEnd)` as objects; only the row groups they touch are read. */
export const readRows = (file: AsyncBuffer, metadata: FileMetaData, rowStart: number, rowEnd: number): Promise<Record<string, unknown>[]> =>
  parquetReadObjects({ file, metadata, rowStart, rowEnd });

const toArrayBuffer = (b: Uint8Array) => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;

/** An in-memory file. */
export const bytesBuffer = (b: Uint8Array): AsyncBuffer => toArrayBuffer(b);

/** A local file read by range through an open handle (the caller closes it). */
export const fileBuffer = (fh: FileHandle, byteLength: number): AsyncBuffer => ({
  byteLength,
  async slice(start, end = byteLength) {
    const out = new Uint8Array(end - start);
    let at = 0;
    while (at < out.byteLength) {
      const { bytesRead } = await fh.read(out, at, out.byteLength - at, start + at);
      if (!bytesRead) throw new Error("hopstore: cached file is shorter than expected");
      at += bytesRead;
    }
    return out.buffer;
  },
});

/** An object read by HTTP range (`ObjectStore.get(key, range)`), with hyparquet's footer/slice cache in front. */
export const objectBuffer = (objects: ObjectStore, key: string, byteLength: number): AsyncBuffer =>
  cachedAsyncBuffer({
    byteLength,
    async slice(start, end = byteLength) {
      if (end <= start) return new ArrayBuffer(0);
      const body = await objects.get(key, { start, end: end - 1 });
      if (!body) throw new Error(`hopstore: object ${key} is missing`);
      if (body.byteLength !== end - start) throw new Error(`hopstore: short range read of ${key}`);
      return toArrayBuffer(body);
    },
  });
