/** Record codec shared by the local WAL file and the gzipped WAL objects on S3: `[u32 BE length][utf8 JSON]` records. */

/** JSON-compatible values. Bigints never appear here: callers store them as decimal strings. */
export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** One change inside a commit batch. */
export type Op =
  | { t: "put"; table: string; key: string; value: Json }
  | { t: "del"; table: string; key: string }
  | { t: "append"; table: string; partition: string; row: Json };

/** A committed batch: applied atomically, numbered by a dense sequence starting at 1. */
export interface Batch {
  seq: number;
  ops: Op[];
}

/**
 * Freezes a JSON value and everything inside it, in place, and returns it. The store freezes every value it holds (a
 * decoded copy, never the caller's object), so a reader that mutates one gets a TypeError in strict mode instead of
 * silently diverging from the WAL.
 */
export function deepFreeze<T extends Json>(v: T): T {
  if (typeof v !== "object" || v === null) return v;
  Object.freeze(v);
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) deepFreeze(v[i]!);
  } else {
    for (const k in v) deepFreeze(v[k]!);
  }
  return v;
}

const enc = new TextEncoder();
const dec = new TextDecoder("utf-8", { fatal: true });

/** Encodes values as length-prefixed JSON records. */
export function encodeRecords(values: unknown[]): Uint8Array {
  const parts = values.map((v) => enc.encode(JSON.stringify(v)));
  const out = new Uint8Array(parts.reduce((n, p) => n + 4 + p.byteLength, 0));
  const view = new DataView(out.buffer);
  let at = 0;
  for (const p of parts) {
    view.setUint32(at, p.byteLength);
    out.set(p, at + 4);
    at += 4 + p.byteLength;
  }
  return out;
}

/**
 * Decodes records up to the first incomplete or unparsable one (a torn write at the tail of a crashed file).
 * `validBytes` is the length of the clean prefix.
 */
export function decodeRecords<T = Batch>(buf: Uint8Array): { records: T[]; validBytes: number } {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const records: T[] = [];
  let at = 0;
  while (at + 4 <= buf.byteLength) {
    const len = view.getUint32(at);
    if (at + 4 + len > buf.byteLength) break;
    try {
      records.push(JSON.parse(dec.decode(buf.subarray(at + 4, at + 4 + len))) as T);
    } catch {
      break;
    }
    at += 4 + len;
  }
  return { records, validBytes: at };
}
