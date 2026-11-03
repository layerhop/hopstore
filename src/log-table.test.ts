import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { Json } from "./codec.js";
import { readLogParquetGroups, type RowCodec } from "./log-table.js";
import { MemoryObjectStore } from "./memory-store.js";
import { bytesBuffer } from "./parquet.js";
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

  it("a flush that adopts a landed seal drops the sealed rows from memory; sealsInDoubt counts the seal until then", async () => {
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
    expect(s.sealsInDoubt()).toBe(0);
    lose = true;
    await expect(s.sealLogs()).rejects.toThrow(/lost response/);
    expect(s.sealsInDoubt()).toBe(1);
    expect(await s.sealLogs({ where: () => false })).toBe(0); // an empty seal settles nothing
    expect(s.sealsInDoubt()).toBe(1);
    await s.commit([append("A", 6)]);
    await s.flush(); // creates the slot the seal used: adopts it
    expect(s.sealsInDoubt()).toBe(0);
    expect(log.size).toBe(4);
    const want = [0, 1, 2, 3, 4, 5, 6];
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
    expect(await s.sealLogs()).toBe(1); // A3 only
    expect(blocks(await all(log.scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
    const s2 = await openStore(opts(objects, await tmp()));
    expect(blocks(await all(s2.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [100] })))).toEqual(want);
  });

  it("a failed seal whose manifest never landed stops counting in sealsInDoubt once a later publish takes its slot", async () => {
    const objects = new MemoryObjectStore();
    const realPut = objects.put.bind(objects);
    let fail = false;
    objects.put = async (key, body, o) => {
      if (fail && key.includes("/manifest/")) throw new Error("manifest PUT failed");
      return realPut(key, body, o);
    };
    const s = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    await s.commit(Array.from({ length: 6 }, (_, b) => append("A", b)));
    fail = true;
    await expect(s.sealLogs()).rejects.toThrow(/manifest PUT failed/);
    expect(s.sealsInDoubt()).toBe(1);
    fail = false;
    await s.commit([append("B", 1)]);
    await s.flush(); // its manifest takes the slot: the failed seal can no longer land
    expect(s.sealsInDoubt()).toBe(0);
    expect(s.logTable("trades").size).toBe(7); // nothing left memory
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

  it("seal callbacks see the partition", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    const log = s.logTable<Trade>("trades");
    await s.commit([6, 2, 4, 1, 5, 3].map((b) => append("A", b))); // tail 4, 5, 6; waiting 1, 2, 3
    await s.commit([1, 2, 3, 4].map((b) => append("B", b))); // tail 2, 3, 4; waiting 1
    const seen: string[] = [];
    // only partition B: its waiting row, and its tail row 4 by evict
    const n = await s.sealLogs({ where: (_, p) => (seen.push(`w${p}`), p === "B"), evict: (r, p) => (seen.push(`e${p}`), p === "B" && (r as Trade).b === 4) });
    expect(n).toBe(2);
    expect(seen).toContain("wA");
    expect(seen).toContain("eB");
    expect([blocks([...log.memoryRows("A")]), blocks([...log.memoryRows("B")])]).toEqual([[1, 2, 3, 4, 5, 6], [2, 3]]);
  });

  it("tables limits a seal to those log tables: the others keep their waiting rows and see no callback", async () => {
    const extra = { tables: { state: ["meta"], log: [{ name: "trades", keepPerPartition: 3, sortKey }, { name: "fills", keepPerPartition: 0, sortKey }] } };
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp(), extra));
    const fill = (b: number) => ({ ...append("F", b), table: "fills" });
    await s.commit([...[1, 2, 3, 4, 5].map((b) => append("A", b)), fill(1), fill(2)]); // waiting A1, A2; F1, F2
    const seen: string[] = [];
    const where = (r: Json, p: string, table: string) => (seen.push(`${table}:${p}${(r as Trade).b}`), true);
    expect(await s.sealLogs({ tables: ["fills"], where, withOps: (rows) => (seen.push(`ops${rows.map(([, , t]) => t).join()}`), []) })).toBe(2);
    expect(seen).toEqual(["fills:F1", "fills:F2", "opsfills,fills"]);
    // a withOps callback typed for [partition, row] pairs (before the table was added) still fits
    const pairs = (rows: [string, Json][]) => (seen.push(`pairs${rows.length}`), []);
    await s.commit([fill(3)]);
    expect(await s.sealLogs({ tables: ["fills"], withOps: pairs })).toBe(1);
    expect(seen.at(-1)).toBe("pairs1");
    expect([s.logTable("trades").size, s.logTable("fills").size]).toEqual([5, 0]);
    const filled = [1, 2, 3];
    expect(await s.sealLogs({ tables: [] })).toBe(0);
    expect(s.logTable("trades").size).toBe(5);
    await expect(s.sealLogs({ tables: ["nope"] })).rejects.toThrow(/nope/);
    expect(await s.sealLogs({ tables: ["trades"] })).toBe(2);
    expect(blocks(await all(s.logTable<Trade>("fills").scan({ partition: "F", from: [0], to: [9] })))).toEqual(filled);
    await s.close();
    const again = await openStore(opts(objects, await tmp(), extra));
    expect(blocks(await all(again.logTable<Trade>("fills").scan({ partition: "F", from: [0], to: [9] })))).toEqual(filled);
    expect(blocks(await all(again.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [9] })))).toEqual([1, 2, 3, 4, 5]);
  });

  it("withOps commits ops built from exactly the rows a seal takes, in the snapshot it publishes", async () => {
    let hold: (() => void) | null = null;
    const objects = new (class extends MemoryObjectStore {
      override async put(...args: Parameters<MemoryObjectStore["put"]>) {
        if (hold) await new Promise<void>((r) => (hold = r));
        return super.put(...args);
      }
    })();
    const s = await openStore(opts(objects, await tmp()));
    await s.commit([1, 2, 3, 4].map((b) => append("A", b)), { durable: "s3" }); // waiting A1
    hold = () => {};
    const flushing = s.commit([append("B", 1)], { durable: "s3" }); // its upload holds the S3 lane
    await new Promise((r) => setTimeout(r, 50));
    let seen: string[] = [];
    const sealing = s.sealLogs({ withOps: (rows) => ((seen = rows.map(([p, r]) => `${p}${(r as Trade).b}`)), [{ t: "put", table: "meta", key: "sealed", value: seen.length }]) });
    await s.commit([5, 6].map((b) => append("A", b))); // A2, A3 start waiting before the seal starts
    expect(seen).toEqual([]);
    while (hold) {
      const h: () => void = hold;
      hold = null;
      h();
    }
    await flushing;
    expect(await sealing).toBe(3);
    expect(seen).toEqual(["A1", "A2", "A3"]);
    expect(s.stateTable("meta").get("sealed")).toBe(3);
    await s.close();
    const again = await openStore(opts(objects, await tmp())); // from S3 alone
    expect(again.stateTable("meta").get("sealed")).toBe(3);
    expect(blocks(await all(again.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [100] })))).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("scanEntries yields each row with its partition, memory merged with segments", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    const log = s.logTable<Trade>("trades");
    await s.commit([1, 2, 3, 4, 5].map((b) => append("A", b)));
    await s.commit([2, 9].map((b) => append("B", b)));
    await s.sealLogs(); // A1, A2 sealed
    const got = await all(log.scanEntries({ from: [2], to: [9] }));
    expect(got.map(([p, r]) => `${p}${r.b}`)).toEqual(["A2", "A3", "A4", "A5", "B2"]);
    expect((await all(log.scanEntries({ partition: "B", from: [0], to: [100] }))).map(([p, r]) => `${p}${r.b}`)).toEqual(["B2", "B9"]);
    // the same rows as scan
    expect((await all(log.scanEntries({ from: [0], to: [100] }))).map(([, r]) => r)).toEqual(await all(log.scan({ from: [0], to: [100] })));
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

  describe("codec (rows packed in memory)", () => {
    /** Packs a trade as "b|i|v"; counts its calls. */
    const counted = () => {
      const n = { pack: 0, unpack: 0 };
      const codec: RowCodec = {
        pack: (r) => {
          n.pack++;
          const t = r as Trade;
          return `${t.b}|${t.i}|${t.v}`;
        },
        unpack: (p) => {
          n.unpack++;
          const [b, i, v] = p.split("|");
          return { b: Number(b), i: Number(i), v: v! };
        },
      };
      return { codec, n };
    };
    const withCodec = (codec: RowCodec | undefined) => ({ tables: { state: ["meta"], log: [{ name: "trades", keepPerPartition: 3, sortKey, codec }] } });
    /** Every read of the table, as one comparable value. */
    const reads = async (s: Store) => {
      const log = s.logTable<Trade>("trades");
      return {
        size: log.size,
        latest: [log.latest("A", 10), log.latest("A", 2, (r) => r.b !== 6), log.latest("B", 10)],
        memory: [[...log.memoryRows("A")], [...log.rowsInMemory("A")].sort((x, y) => x.b - y.b), [...log.memoryRows("B")]],
        scan: await all(log.scan({ from: [0], to: [100] })),
        entries: await all(log.scanEntries({ partition: "A", from: [2], to: [9] })),
        has: [log.has("A", [6, 0]), log.has("A", [1, 0]), log.has("B", [7, 0])],
      };
    };
    const fill = async (s: Store) => {
      await s.commit([6, 2, 4, 1, 5, 3].map((b) => append("A", b)));
      await s.commit([7, 8].map((b) => append("B", b)));
      await s.sealLogs({ where: (r) => (r as Trade).b < 2 }); // A1 into a segment
      await s.commit([append("A", 9), append("B", 7, 1)]);
    };

    it("holds rows packed, reads them decoded exactly like a table without one; stored data is the same either way", async () => {
      const { codec, n } = counted();
      const plain = await openStore(opts(new MemoryObjectStore(), await tmp()));
      const objects = new MemoryObjectStore();
      const packed = await openStore(opts(objects, await tmp(), withCodec(codec)));
      await fill(plain);
      await fill(packed);
      expect(n.pack).toBeGreaterThan(0);
      expect([...packed.logTable("trades").packedRowsInMemory("B")].sort()).toEqual(["7|0|B7.0", "7|1|B7.1", "8|0|B8.0"]);
      expect([...plain.logTable("trades").packedRowsInMemory("B")]).toEqual([...plain.logTable("trades").rowsInMemory("B")]);
      const want = await reads(plain);
      expect(await reads(packed)).toEqual(want);
      // a row read is a decoded copy: changing it changes nothing held
      (packed.logTable<Trade>("trades").latest("A", 1)[0] as { v: string }).v = "changed";
      expect(await reads(packed)).toEqual(want);
      // snapshot, segments and WAL hold the rows themselves: a store without the codec reads them, and back
      await packed.snapshot();
      await packed.commit([append("B", 10)]);
      await packed.flush();
      const bare = await openStore(opts(objects, await tmp()));
      const snap = (await readLogParquetGroups(bytesBuffer(objects.objects.get([...objects.objects.keys()].find((k) => k.endsWith("trades.log.parquet"))!)!))).groups;
      for await (const g of snap) for (const e of g) expect(typeof e.row).toBe("object");
      await plain.commit([append("B", 10)]);
      const want2 = await reads(plain);
      expect(await reads(bare)).toEqual(want2);
      await bare.close();
      const again = await openStore(opts(objects, await tmp(), withCodec(counted().codec)));
      expect(await reads(again)).toEqual(want2);
    });

    it("a commit whose row the codec does not give back is refused before anything is written", async () => {
      const lossy: RowCodec = { pack: (r) => `${(r as Trade).b}|${(r as Trade).i}|x`, unpack: (p) => ({ b: Number(p.split("|")[0]), i: Number(p.split("|")[1]), v: "x" }) };
      const s = await openStore(opts(new MemoryObjectStore(), await tmp(), withCodec(lossy)));
      await s.commit([{ t: "append", table: "trades", partition: "A", row: { b: 1, i: 0, v: "x" } }]);
      await expect(s.commit([{ t: "put", table: "meta", key: "k", value: 1 }, append("A", 2)])).rejects.toThrow(/codec does not give back/);
      expect([s.seq, s.stateTable("meta").has("k"), s.logTable("trades").size]).toEqual([1, false, 1]);
    });

    it("seal callbacks get decoded rows, or with `packed` the packed strings", async () => {
      const { codec, n } = counted();
      const s = await openStore(opts(new MemoryObjectStore(), await tmp(), withCodec(codec)));
      await s.commit([1, 2, 3, 4, 5].map((b) => append("A", b))); // waiting: 1, 2
      const seen: Json[] = [];
      expect(await s.sealLogs({ where: (r) => (seen.push(r), (r as Trade).b === 1), withOps: (rows) => (seen.push(...rows.map(([, r]) => r)), []) })).toBe(1);
      expect(seen).toEqual([{ b: 1, i: 0, v: "A1.0" }, { b: 2, i: 0, v: "A2.0" }, { b: 1, i: 0, v: "A1.0" }]);
      seen.length = 0;
      const before = n.unpack;
      expect(await s.sealLogs({ packed: true, where: (r) => (seen.push(r), true), evict: (r) => (r as string).startsWith("5|"), withOps: (rows) => (seen.push(...rows.map(([, r]) => r)), []) })).toBe(2);
      expect(seen).toEqual(["2|0|A2.0", "2|0|A2.0", "5|0|A5.0"]);
      expect(n.unpack - before).toBe(4); // only to write the files: the 2 sealed rows, the 2 left in the snapshot
      expect(blocks([...s.logTable<Trade>("trades").memoryRows("A")])).toEqual([3, 4]);
      expect(blocks(await all(s.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [9] })))).toEqual([1, 2, 3, 4, 5]);
    });

    it("rows loaded on open (snapshot and WAL) that the codec does not give back are held as they are, read exactly", async () => {
      // a codec that loses any "|" in v: no commit through it would take such a row, but stored data may hold one
      const lossy: RowCodec = {
        pack: (r) => `${(r as Trade).b}|${(r as Trade).i}|${(r as Trade).v}`,
        unpack: (p) => {
          const [b, i, v] = p.split("|");
          return { b: Number(b), i: Number(i), v: v! };
        },
      };
      const objects = new MemoryObjectStore();
      const raw = await openStore(opts(objects, await tmp()));
      const row = (b: number, v: string) => ({ t: "append" as const, table: "trades", partition: "A", row: { b, i: 0, v } });
      await raw.commit([row(1, "ok"), row(2, "x|y"), row(3, "ok"), row(4, "ok")]);
      await raw.snapshot();
      await raw.commit([row(5, "p|q"), row(6, "ok")]); // in the WAL only
      await raw.flush();
      const want = await all(raw.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [9] }));
      const s = await openStore(opts(objects, await tmp(), withCodec(lossy))); // takes the prefix over
      const log = s.logTable<Trade>("trades");
      expect(await all(log.scan({ partition: "A", from: [0], to: [9] }))).toEqual(want);
      expect([...log.memoryRows("A")]).toEqual(want);
      expect([...log.packedRowsInMemory("A")].map((r) => typeof r).sort()).toEqual(["object", "object", "string", "string", "string", "string"]);
      // a packed seal gets those rows as they are; the seal and its snapshot keep them exact
      const seen: Json[] = [];
      expect(await s.sealLogs({ packed: true, where: (r) => (seen.push(r), true) })).toBe(3); // waiting: 1, 2, 3
      expect(seen).toEqual(["1|0|ok", { b: 2, i: 0, v: "x|y" }, "3|0|ok"]);
      expect(await all(log.scan({ partition: "A", from: [0], to: [9] }))).toEqual(want);
      const plain = await openStore(opts(objects, await tmp())); // no codec: the stored rows are the rows
      expect(await all(plain.logTable<Trade>("trades").scan({ partition: "A", from: [0], to: [9] }))).toEqual(want);
    });

    it("a snapshot is read back one row group at a time", async () => {
      const objects = new MemoryObjectStore();
      const s = await openStore(opts(objects, await tmp(), { tables: { state: ["meta"], log: [{ name: "trades", keepPerPartition: 30_000, sortKey }] } }));
      await s.commit(Array.from({ length: 25_000 }, (_, i) => append("A", i)));
      await s.snapshot();
      const key = [...objects.objects.keys()].find((k) => k.endsWith("trades.log.parquet"))!;
      const sizes: number[] = [];
      for await (const g of (await readLogParquetGroups(bytesBuffer(objects.objects.get(key)!))).groups) sizes.push(g.length);
      expect(sizes).toEqual([10_000, 10_000, 5_000]);
      const again = await openStore(opts(objects, await tmp(), { tables: { state: ["meta"], log: [{ name: "trades", keepPerPartition: 30_000, sortKey }] } }));
      expect(again.logTable("trades").size).toBe(25_000);
    });
  });
});
