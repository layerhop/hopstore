import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { Json } from "./codec.js";
import { MemoryObjectStore } from "./memory-store.js";
import { Store, type StoreOptions } from "./store.js";

const dirs: string[] = [];
const tmp = async () => {
  const d = await mkdtemp(join(tmpdir(), "hopstore-log-"));
  dirs.push(d);
  return d;
};
const open: Store[] = [];
afterEach(async () => {
  for (const s of open.splice(0)) await s.close().catch(() => {});
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

/** Counts GETs (and ranged GETs) so tests can see which segments a scan touched. */
class CountingStore extends MemoryObjectStore {
  gets: string[] = [];
  ranged = 0;
  override async get(key: string, range?: { start: number; end: number }) {
    this.gets.push(key);
    if (range) this.ranged++;
    return super.get(key, range);
  }
}

type Trade = { b: number; i: number; v: string };
const sortKey = (r: Json) => [(r as Trade).b, (r as Trade).i];
const opts = (objects: MemoryObjectStore, localDir: string, extra: Partial<StoreOptions> = {}): StoreOptions => ({
  objects,
  prefix: "hop/test/",
  localDir,
  flushMs: 20,
  snapshotEveryMs: 3_600_000,
  tables: { state: ["meta"], log: [{ name: "trades", keepPerPartition: 3, sortKey }] },
  onError: () => {}, // background failures are expected in fault-injection tests
  ...extra,
});
const openStore = async (o: StoreOptions) => {
  const s = await Store.open(o);
  open.push(s);
  return s;
};
const append = (partition: string, b: number, i = 0) => ({ t: "append" as const, table: "trades", partition, row: { b, i, v: `${partition}${b}.${i}` } });
const all = async <R>(it: AsyncIterable<R>) => {
  const out: R[] = [];
  for await (const r of it) out.push(r);
  return out;
};
const segKeys = (m: MemoryObjectStore) => [...m.objects.keys()].filter((k) => k.startsWith("hop/test/seg/"));
const blocks = (rows: Trade[]) => rows.map((r) => r.b);

describe("LogTable", () => {
  it("latest keeps only keepPerPartition in memory but scan still returns sealed rows", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    const log = s.logTable<Trade>("trades");
    // out of order on purpose: memory keeps sortKey order
    await s.commit([3, 1, 2, 5, 4, 6, 7, 10, 9, 8].map((b) => append("A", b)));
    await s.commit([append("B", 1), append("B", 2)]);
    expect(blocks(log.latest("A", 2))).toEqual([10, 9]);
    expect(blocks(log.latest("A", 50))).toEqual([10, 9, 8]);
    expect(blocks(log.latest("nope", 5))).toEqual([]);
    expect(log.size).toBe(12);

    expect(await s.sealLogs({ olderThanSort: [5] })).toBe(4); // A1..A4 (pending holds A1..A7)
    expect(await s.sealLogs()).toBe(3); // A5..A7
    expect(await s.sealLogs()).toBe(0);
    expect(log.size).toBe(5);
    expect(segKeys(objects).length).toBe(2);

    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(blocks(await all(log.scan({ partition: "A", from: [3], to: [9] })))).toEqual([3, 4, 5, 6, 7, 8]);
    const every = await all(log.scan({ from: [0], to: [100] }));
    expect(every.map((r) => r.v)).toEqual(["A1.0", "A2.0", "A3.0", "A4.0", "A5.0", "A6.0", "A7.0", "A8.0", "A9.0", "A10.0", "B1.0", "B2.0"]);
  });

  it("memory accessors: has, memoryRows and partitions see the tail and the waiting rows, not sealed ones", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    const log = s.logTable<Trade>("trades");
    await s.commit([3, 1, 2, 5, 4, 6].map((b) => append("A", b)));
    await s.commit([append("B", 1)]);
    expect(blocks([...log.memoryRows("A")])).toEqual([1, 2, 3, 4, 5, 6]);
    expect([...log.memoryRows("nope")]).toEqual([]);
    expect([...log.partitions()].sort()).toEqual(["A", "B"]);
    expect([log.has("A", [2, 0]), log.has("A", [6, 0]), log.has("A", [2, 1]), log.has("A", [2]), log.has("C", [1, 0])]).toEqual([true, true, false, false, false]);
    await s.sealLogs({ olderThanSort: [3] });
    expect(blocks([...log.memoryRows("A")])).toEqual([3, 4, 5, 6]);
    expect(log.has("A", [2, 0])).toBe(false);
  });

  it("evict seals tail rows it accepts (age-based eviction); where narrows the waiting rows", async () => {
    const objects = new MemoryObjectStore();
    const dir = await tmp();
    const s = await openStore(opts(objects, dir));
    const log = s.logTable<Trade>("trades");
    await s.commit([1, 2, 3, 4, 5, 6].map((b) => append("A", b))); // tail 4, 5, 6; waiting 1, 2, 3
    await s.commit([append("B", 1), append("B", 7)]); // tail only
    expect(await s.sealLogs({ where: (r) => (r as Trade).b !== 2 })).toBe(2); // A1, A3
    expect(blocks([...log.memoryRows("A")])).toEqual([2, 4, 5, 6]);
    // rows with b <= 5 are "old": the waiting A2 and the tail rows A4, A5, B1 leave memory
    expect(await s.sealLogs({ olderThanSort: [0], evict: (r) => (r as Trade).b <= 5 })).toBe(4);
    expect(blocks([...log.memoryRows("A")])).toEqual([6]);
    expect(blocks([...log.memoryRows("B")])).toEqual([7]);
    expect(blocks(log.latest("A", 10))).toEqual([6]);
    expect(log.size).toBe(2);
    expect(blocks([...log.rowsInMemory("A")])).toEqual([6]);
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([1, 2, 3, 4, 5, 6]);
    expect(blocks(await all(log.scan({ partition: "B", from: [0], to: [100] })))).toEqual([1, 7]);
    // new rows refill the tail; the evicted rows never come back from a snapshot or the WAL
    await s.commit([append("A", 8)]);
    expect(blocks(log.latest("A", 10))).toEqual([8, 6]);
    await s.close();
    const again = await openStore(opts(objects, await tmp()));
    const l2 = again.logTable<Trade>("trades");
    expect([blocks([...l2.memoryRows("A")]), blocks([...l2.memoryRows("B")])]).toEqual([[6, 8], [7]]);
    expect(blocks(await all(l2.scan({ partition: "A", from: [0], to: [100] })))).toEqual([1, 2, 3, 4, 5, 6, 8]);
  });

  it("sealed segment survives reopen from a fresh localDir", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp()));
    await s1.commit(Array.from({ length: 8 }, (_, b) => append("A", b)));
    expect(await s1.sealLogs()).toBe(5);
    await s1.close();

    const s2 = await openStore(opts(objects, await tmp()));
    const log = s2.logTable<Trade>("trades");
    expect(log.size).toBe(3); // memory is rebuilt from the snapshot only, never from segments
    expect(blocks(log.latest("A", 10))).toEqual([7, 6, 5]);
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });

  it("a crash after sealing replays the wal without duplicating sealed rows", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await Store.open(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    await s1.commit(Array.from({ length: 6 }, (_, b) => append("A", b)), { durable: "s3" });
    expect(await s1.sealLogs()).toBe(3);
    await s1.commit([append("A", 6), append("A", 7)], { durable: "s3" }); // A3, A4 overflow into pending
    // no close: s1 "crashes"; the next writer replays snapshot + wal from a fresh disk
    const s2 = await openStore(opts(objects, await tmp()));
    const log = s2.logTable<Trade>("trades");
    expect(log.size).toBe(5);
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(await s2.sealLogs()).toBe(2);
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });

  it("a failed manifest publish keeps the rows in memory and the retry seals them once", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    const log = s.logTable<Trade>("trades");
    await s.commit(Array.from({ length: 6 }, (_, b) => append("A", b)));
    const realPut = objects.put.bind(objects);
    let failManifest = true;
    objects.put = async (key, body, o) => {
      if (failManifest && key.includes("/manifest/")) throw new (await import("./s3.js")).S3Error("down", 503);
      return realPut(key, body, o);
    };
    await expect(s.sealLogs()).rejects.toThrow(/down/);
    expect(segKeys(objects).length).toBe(1); // the orphan segment
    expect(log.size).toBe(6);
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([0, 1, 2, 3, 4, 5]);
    failManifest = false;
    expect(await s.sealLogs()).toBe(3);
    expect(segKeys(objects).length).toBe(1); // the retry reused the orphan's name
    expect(log.size).toBe(3);
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual([0, 1, 2, 3, 4, 5]);
    const s2 = await openStore(opts(objects, await tmp()));
    expect(blocks(await all(s2.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [100] })))).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it("scan reads only overlapping segments", async () => {
    const objects = new CountingStore();
    const s1 = await openStore(opts(objects, await tmp()));
    // segment 1: partitions m..n, blocks 0..9; segment 2: partitions o..p, blocks 100..109
    await s1.commit(["m", "n"].flatMap((p) => Array.from({ length: 13 }, (_, b) => append(p, b))));
    await s1.sealLogs();
    await s1.commit(["o", "p"].flatMap((p) => Array.from({ length: 13 }, (_, b) => append(p, 100 + b))));
    await s1.sealLogs();
    await s1.close();
    expect(segKeys(objects).length).toBe(2);

    const s2 = await openStore(opts(objects, await tmp(), { segmentWholeMaxBytes: 0 })); // force range reads
    const log = s2.logTable<Trade>("trades");
    objects.gets = [];
    expect(await all(log.scan({ partition: "a", from: [0], to: [1000] }))).toEqual([]);
    expect(await all(log.scan({ partition: "z", from: [0], to: [1000] }))).toEqual([]);
    expect(blocks(await all(log.scan({ partition: "m", from: [10], to: [1000] })))).toEqual([10, 11, 12]); // sort range misses segment 1
    expect(objects.gets).toEqual([]);

    expect(blocks(await all(log.scan({ partition: "n", from: [2], to: [5] })))).toEqual([2, 3, 4]);
    expect(objects.gets.length).toBeGreaterThan(0);
    expect(new Set(objects.gets).size).toBe(1); // only segment 1
    expect(objects.ranged).toBe(objects.gets.length);
    expect(blocks(await all(log.scan({ partition: "p", from: [0], to: [1000] })))).toEqual([100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112]);
  });

  it("an uncached segment is read by range and warmed into the local cache in the background", async () => {
    const objects = new CountingStore();
    const s1 = await openStore(opts(objects, await tmp()));
    await s1.commit(Array.from({ length: 13 }, (_, b) => append("A", b)));
    await s1.sealLogs();
    await s1.close();
    const s2 = await openStore(opts(objects, await tmp()));
    const log = s2.logTable<Trade>("trades");
    objects.gets = [];
    objects.ranged = 0;
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [5] })))).toEqual([0, 1, 2, 3, 4]);
    await s2["settled"]();
    expect(objects.ranged).toBeGreaterThan(0); // the scan itself only range-reads
    expect(objects.gets.length - objects.ranged).toBe(1); // one whole GET, off the scan path
    const before = objects.gets.length;
    expect(blocks(await all(log.scan({ partition: "A", from: [5], to: [100] })))).toEqual([5, 6, 7, 8, 9, 10, 11, 12]);
    expect(objects.gets.length).toBe(before); // served from the cache
  });

  it("scan opens segments lazily: never more than overlap at once", async () => {
    const objects = new CountingStore();
    const s1 = await openStore(opts(objects, await tmp(), { tables: { log: [{ name: "trades", keepPerPartition: 0, sortKey }] } }));
    for (let i = 0; i < 20; i++) {
      await s1.commit([...Array.from({ length: 5 }, (_, j) => append("A", i * 5 + j)), append("B", i)]);
      expect(await s1.sealLogs()).toBe(6);
    }
    await s1.close();
    expect(segKeys(objects).length).toBe(20);
    const s2 = await openStore(opts(objects, await tmp(), { segmentWholeMaxBytes: 0, tables: { log: [{ name: "trades", keepPerPartition: 0, sortKey }] } }));
    const log = s2.logTable<Trade>("trades");
    objects.gets = [];
    objects.ranged = 0;
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [1000] })))).toEqual(Array.from({ length: 100 }, (_, i) => i));
    expect(s2["segmentStats"].maxOpen).toBeLessThanOrEqual(2);
    expect(s2["segmentStats"].open).toBe(0);
    expect(objects.ranged).toBe(objects.gets.length);
    // across partitions every segment here spans A..B, so all 20 overlap the first row and must be open together
    expect((await all(log.scan({ from: [0], to: [1000] }))).length).toBe(120);
    expect(s2["segmentStats"].maxOpen).toBe(20);
    // an early break closes what is open
    for await (const _ of log.scan({ partition: "A", from: [0], to: [1000] })) break;
    expect(s2["segmentStats"].open).toBe(0);
  });

  it("equal sort keys come out in commit order across segments and memory", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp(), { tables: { log: [{ name: "trades", keepPerPartition: 1, sortKey }] } }));
    const log = s.logTable<Trade>("trades");
    const same = (v: string) => ({ t: "append" as const, table: "trades", partition: "A", row: { b: 5, i: 0, v } });
    await s.commit([same("1"), same("2"), same("3")]);
    await s.sealLogs(); // 1, 2
    await s.commit([same("4")]);
    await s.sealLogs(); // 3
    await s.commit([same("5"), same("6")]); // memory: 5 pending, 6 tail
    expect((await all(log.scan({ partition: "A", from: [0], to: [10] }))).map((r) => r.v)).toEqual(["1", "2", "3", "4", "5", "6"]);
    expect(log.latest("A", 5).map((r) => r.v)).toEqual(["6"]);
  });

  it("a seal retry after a lost manifest response overwrites nothing and seals every row once", async () => {
    const objects = new MemoryObjectStore();
    const realPut = objects.put.bind(objects);
    const written: string[] = [];
    let lose = false;
    objects.put = async (key, body, o) => {
      const r = await realPut(key, body, o);
      written.push(key);
      if (lose && key.includes("/manifest/")) {
        lose = false;
        throw new (await import("./s3.js")).S3Error("lost response", 503);
      }
      return r;
    };
    const s = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    const log = s.logTable<Trade>("trades");
    await s.commit(Array.from({ length: 6 }, (_, b) => append("A", b))); // pending A0..A2
    lose = true;
    await expect(s.sealLogs()).rejects.toThrow(/lost response/); // ...but the manifest landed
    await s.commit([append("A", 6), append("A", 7)]); // pending A0..A4
    expect(await s.sealLogs()).toBe(2); // adopts the landed seal, then seals only A3, A4
    expect(written.length).toBe(new Set(written).size); // no object written twice
    expect(log.size).toBe(3);
    const want = [0, 1, 2, 3, 4, 5, 6, 7];
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
    expect(segKeys(objects).length).toBe(2); // the superseded attempt's segment is cleaned up
    const s2 = await openStore(opts(objects, await tmp()));
    expect(s2.logTable("trades").size).toBe(3);
    expect(blocks(await all(s2.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
  });

  it("a flush that adopts a landed seal drops the sealed rows from memory", async () => {
    const objects = new MemoryObjectStore();
    const realPut = objects.put.bind(objects);
    let lose = false;
    objects.put = async (key, body, o) => {
      const r = await realPut(key, body, o);
      if (lose && key.includes("/manifest/")) {
        lose = false;
        throw new (await import("./s3.js")).S3Error("lost response", 503);
      }
      return r;
    };
    const s = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    const log = s.logTable<Trade>("trades");
    await s.commit(Array.from({ length: 6 }, (_, b) => append("A", b)));
    lose = true;
    await expect(s.sealLogs()).rejects.toThrow(/lost response/);
    await s.commit([append("A", 6)]);
    await s.flush(); // creates the slot the seal used: adopts it
    expect(log.size).toBe(4);
    const want = [0, 1, 2, 3, 4, 5, 6];
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
    expect(await s.sealLogs()).toBe(1); // A3 only
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
    const s2 = await openStore(opts(objects, await tmp()));
    expect(blocks(await all(s2.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
  });

  it("scan spans many row groups and merges interleaved segments", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp(), { tables: { log: [{ name: "trades", keepPerPartition: 0, sortKey }] } }));
    const log = s.logTable<Trade>("trades");
    await s.commit(Array.from({ length: 25_000 }, (_, i) => append("A", 2 * i)));
    await s.sealLogs();
    await s.commit(Array.from({ length: 25_000 }, (_, i) => append("A", 2 * i + 1)));
    await s.sealLogs();
    await s.commit([append("A", 7, 1)]); // stays in memory
    const rows = await all(log.scan({ partition: "A", from: [5], to: [30_005] }));
    expect(rows.length).toBe(30_001);
    expect(rows.slice(0, 5).map((r) => [r.b, r.i])).toEqual([[5, 0], [6, 0], [7, 0], [7, 1], [8, 0]]);
    for (let i = 1; i < rows.length; i++) expect(rows[i]!.b >= rows[i - 1]!.b).toBe(true);
  });

  it("validates appends and refuses undeclared log tables on open", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    await expect(s.commit([{ t: "append", table: "meta", partition: "p", row: 1 }])).rejects.toThrow(/unknown log table/);
    await expect(s.commit([{ t: "append", table: "trades", partition: "p", row: { b: "x" } as Json }])).rejects.toThrow(/sort key/);
    await expect(s.commit([{ t: "put", table: "trades", key: "k", value: 1 }])).rejects.toThrow(/unknown table/);
    expect(() => s.logTable("nope")).toThrow(/unknown log table/);
    expect(s.seq).toBe(0);
    await expect(Store.open(opts(objects, await tmp(), { tables: { state: ["trades"], log: [{ name: "trades", keepPerPartition: 1, sortKey }] } }))).rejects.toThrow(/declared twice/);

    await s.close(); // empty log snapshot
    const s2 = await openStore(opts(objects, await tmp()));
    expect(s2.logTable("trades").size).toBe(0);
    await s2.commit([append("A", 1)]);
    await s2.close();
    await expect(s2.sealLogs()).rejects.toThrow(/closed/);
    await expect(Store.open(opts(objects, await tmp(), { tables: { state: ["meta"] } }))).rejects.toThrow(/undeclared table trades/);
  });
});
