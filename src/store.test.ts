import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryObjectStore } from "./memory-store.js";
import { readLogParquet } from "./log-table.js";
import { readLatestManifest } from "./manifest.js";
import { bytesBuffer } from "./parquet.js";
import { readStateSnapshot, StateTableImpl as StateTable, writeStateSnapshot } from "./state-table.js";
import { FencedError, Store, type StoreOptions } from "./store.js";

const dirs: string[] = [];
const tmp = async () => {
  const d = await mkdtemp(join(tmpdir(), "hopstore-store-"));
  dirs.push(d);
  return d;
};
const open: Store[] = [];
afterEach(async () => {
  for (const s of open.splice(0)) await s.close().catch(() => {});
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

const opts = (objects: MemoryObjectStore, localDir: string, extra: Partial<StoreOptions> = {}): StoreOptions => ({
  objects,
  prefix: "hop/test/",
  localDir,
  flushMs: 20,
  snapshotEveryMs: 3_600_000,
  tables: { state: ["balances", "meta"] },
  onError: () => {}, // background failures are expected in fault-injection tests
  ...extra,
});
const openStore = async (o: StoreOptions) => {
  const s = await Store.open(o);
  open.push(s);
  return s;
};
const keys = (m: MemoryObjectStore, sub: string) => [...m.objects.keys()].filter((k) => k.startsWith(`hop/test/${sub}`));

describe("Store", () => {
  it("waitDurable after a local commit retries failed flushes until the batch is on S3", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp(), { flushMs: 5 }));
    const seq = await s.commit([{ t: "put", table: "meta", key: "k", value: 1 }]);
    objects.failNext(8, 503);
    await s.waitDurable(seq);
    expect(s.durableSeq()).toBeGreaterThanOrEqual(seq);
    const again = await openStore(opts(objects, await tmp()));
    expect(again.stateTable("meta").get("k")).toBe(1);
  });

  it("commit then reopen restores state", async () => {
    const objects = new MemoryObjectStore();
    const dir = await tmp();
    const s1 = await openStore(opts(objects, dir));
    const bal = s1.stateTable<{ amount: string }>("balances");
    await s1.commit([
      { t: "put", table: "balances", key: "a", value: { amount: "1" } },
      { t: "put", table: "balances", key: "b", value: { amount: "2" } },
      { t: "put", table: "meta", key: "cursor", value: 42 },
    ]);
    expect(bal.get("a")).toEqual({ amount: "1" });
    await s1.commit([{ t: "del", table: "balances", key: "a" }]);
    expect(bal.has("a")).toBe(false);
    expect(s1.seq).toBe(2);
    await s1.close();

    const s2 = await openStore(opts(objects, await tmp()));
    const bal2 = s2.stateTable<{ amount: string }>("balances");
    expect(bal2.size).toBe(1);
    expect(bal2.get("b")).toEqual({ amount: "2" });
    expect(s2.stateTable("meta").get("cursor")).toBe(42);
    expect(s2.seq).toBe(2);
    expect(s2.epoch).toBe(s1.epoch + 1);
  });

  it("ranked index survives reopen", async () => {
    const objects = new MemoryObjectStore();
    const ranked = [{ name: "holders", table: "balances", group: (k: string) => k.split("|")[0]!, member: (k: string) => k.split("|")[1]!, score: (v: unknown) => BigInt((v as { amount: string }).amount) }];
    const o = (dir: string) => opts(objects, dir, { tables: { state: ["balances", "meta"], ranked } });
    const s1 = await openStore(o(await tmp()));
    await s1.commit([
      { t: "put", table: "balances", key: "t|a", value: { amount: "5" } },
      { t: "put", table: "balances", key: "t|b", value: { amount: "9" } },
      { t: "put", table: "balances", key: "t|c", value: { amount: "2" } },
    ]);
    await s1.commit([{ t: "put", table: "balances", key: "t|a", value: { amount: "0" } }, { t: "del", table: "balances", key: "t|c" }]);
    const want = { top: s1.ranked("holders").top("t", 10), count: 1, sum: 9n };
    expect(want.top).toEqual([{ member: "b", score: 9n }]);
    await s1.close();
    const s2 = await openStore(o(await tmp()));
    const r = s2.ranked("holders");
    expect({ top: r.top("t", 10), count: r.count("t"), sum: r.sum("t") }).toEqual(want);
    expect(() => s2.ranked("nope")).toThrow();
    await expect(Store.open({ ...o(await tmp()), tables: { state: ["meta"], ranked } })).rejects.toThrow(/undeclared table/);
  });

  it("ranked: a put that moves a key between groups, and a throwing score is rejected atomically", async () => {
    const objects = new MemoryObjectStore();
    const ranked = [{ name: "by", table: "balances", group: (_k: string, v: unknown) => (v as { g: string }).g, member: (k: string) => k, score: (v: unknown) => BigInt((v as { amount: string }).amount) }];
    const s = await openStore(opts(objects, await tmp(), { tables: { state: ["balances"], ranked } }));
    const r = s.ranked("by");
    await s.commit([{ t: "put", table: "balances", key: "k", value: { g: "A", amount: "7" } }]);
    expect(r.count("A")).toBe(1);
    await s.commit([{ t: "put", table: "balances", key: "k", value: { g: "B", amount: "7" } }]);
    expect([r.count("A"), r.count("B"), r.sum("A"), r.sum("B")]).toEqual([0, 1, 0n, 7n]);
    const seq = s.seq;
    await expect(s.commit([{ t: "put", table: "balances", key: "k", value: { g: "A", amount: "x" } }])).rejects.toThrow();
    expect(s.seq).toBe(seq);
    expect(s.stateTable("balances").get("k")).toEqual({ g: "B", amount: "7" });
    expect([r.count("A"), r.count("B")]).toEqual([0, 1]);
    await s.commit([{ t: "put", table: "balances", key: "k", value: { g: "A", amount: "3" } }]);
    expect([r.count("A"), r.count("B"), r.sum("A")]).toEqual([1, 0, 3n]);
  });

  it("ranked minScore: the default leaves out zero and negative scores; null ranks them, also after reopen", async () => {
    const objects = new MemoryObjectStore();
    const spec = (name: string, minScore?: bigint | null) => ({ name, table: "balances", group: () => "g", member: (k: string) => k, score: (v: unknown) => BigInt(v as string), ...(minScore === undefined ? {} : { minScore }) });
    const tables = { state: ["balances", "meta"], ranked: [spec("dflt"), spec("zero", 0n), spec("every", null)] };
    const s1 = await openStore(opts(objects, await tmp(), { tables }));
    await s1.commit(["3", "0", "-2"].map((v, i) => ({ t: "put" as const, table: "balances", key: `k${i}`, value: v })));
    const counts = (s: Store) => ["dflt", "zero", "every"].map((n) => [s.ranked(n).count("g"), s.ranked(n).sum("g")]);
    expect(counts(s1)).toEqual([[1, 3n], [2, 3n], [3, 1n]]);
    await s1.close();
    const s2 = await openStore(opts(objects, await tmp(), { tables }));
    expect(counts(s2)).toEqual([[1, 3n], [2, 3n], [3, 1n]]);
    expect(s2.ranked("every").top("g", 3).map((e) => e.member)).toEqual(["k0", "k1", "k2"]);
    await expect(Store.open(opts(objects, await tmp(), { tables: { state: ["balances", "meta"], ranked: [{ ...spec("bad"), minScore: 1 as unknown as bigint }] } }))).rejects.toThrow(/minScore must be a bigint or null/);
  });

  it("reopen without close replays local wal", async () => {
    const objects = new MemoryObjectStore();
    const dir = await tmp();
    const s1 = await Store.open(opts(objects, dir, { flushMs: 1_000_000 }));
    await s1.commit([{ t: "put", table: "balances", key: "x", value: { amount: "9" } }]);
    expect(keys(objects, "wal/")).toEqual([]); // never reached S3

    const s2 = await openStore(opts(objects, dir));
    expect(s2.stateTable("balances").get("x")).toEqual({ amount: "9" });
    expect(s2.seq).toBe(1);
    // the replayed batch is uploaded by the new writer and survives losing the local disk
    await s2.commit([], { durable: "s3" });
    const s3 = await openStore(opts(objects, await tmp()));
    expect(s3.stateTable("balances").get("x")).toEqual({ amount: "9" });
  });

  it("durable s3 waits for upload", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    const seq = await s.commit([{ t: "put", table: "balances", key: "a", value: { amount: "1" } }], { durable: "s3" });
    expect(keys(objects, "wal/").length).toBe(1);
    expect(s.durableSeq()).toBe(seq);
  });

  it("lost local disk recovers what reached s3", async () => {
    const objects = new MemoryObjectStore();
    const dir = await tmp();
    const s1 = await Store.open(opts(objects, dir, { flushMs: 1_000_000 }));
    const a = await s1.commit([{ t: "put", table: "balances", key: "A", value: { amount: "1" } }], { durable: "s3" });
    await s1.commit([{ t: "put", table: "balances", key: "B", value: { amount: "2" } }]);
    await rm(dir, { recursive: true, force: true });

    const s2 = await openStore(opts(objects, await tmp()));
    const bal = s2.stateTable("balances");
    expect(bal.get("A")).toEqual({ amount: "1" });
    expect(bal.has("B")).toBe(false);
    expect(s2.seq).toBe(a);
  });

  it("older writer is fenced after a newer one opens", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    await s1.commit([{ t: "put", table: "balances", key: "old", value: { amount: "1" } }], { durable: "s3" });
    const s2 = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    expect(s2.epoch).toBe(s1.epoch + 1);

    await expect(s1.commit([{ t: "put", table: "balances", key: "stale", value: { amount: "2" } }], { durable: "s3" })).rejects.toBeInstanceOf(FencedError);
    await expect(s1.commit([])).rejects.toBeInstanceOf(FencedError);

    expect(s2.stateTable("balances").get("old")).toEqual({ amount: "1" });
    expect(s2.stateTable("balances").has("stale")).toBe(false);
    await s2.commit([{ t: "put", table: "balances", key: "new", value: { amount: "3" } }], { durable: "s3" });
    const s3 = await openStore(opts(objects, await tmp()));
    expect([...s3.stateTable("balances").scan()].map(([k]) => k)).toEqual(["new", "old"]);
  });

  it("s3 outage delays durable commits without losing them", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    objects.failNext(3, 503);
    const seq = await s.commit([{ t: "put", table: "balances", key: "a", value: { amount: "5" } }], { durable: "s3" });
    expect(s.durableSeq()).toBe(seq);
    const s2 = await openStore(opts(objects, await tmp()));
    expect(s2.stateTable("balances").get("a")).toEqual({ amount: "5" });
  });

  it("snapshot truncates wal", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    for (let i = 0; i < 3; i++) await s.commit([{ t: "put", table: "balances", key: `k${i}`, value: { amount: String(i) } }], { durable: "s3" });
    await s.commit([{ t: "del", table: "balances", key: "k0" }], { durable: "s3" });
    expect(keys(objects, "wal/").length).toBeGreaterThanOrEqual(1);
    await s.snapshot();
    expect(keys(objects, "wal/")).toEqual([]);
    expect(keys(objects, "snap/").some((k) => k.endsWith("/balances.parquet"))).toBe(true);
    await s.commit([{ t: "put", table: "balances", key: "k9", value: { amount: "9" } }], { durable: "s3" });

    const s2 = await openStore(opts(objects, await tmp()));
    expect([...s2.stateTable("balances").scan("k")]).toEqual([
      ["k1", { amount: "1" }],
      ["k2", { amount: "2" }],
      ["k9", { amount: "9" }],
    ]);
    expect(s2.seq).toBe(5);
  });

  it("adopts its own manifest when a retried put already succeeded", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    // make the next manifest put store the object but report a 503, as a lost response would
    const realPut = objects.put.bind(objects);
    let tricked = false;
    objects.put = async (key, body, o) => {
      const r = await realPut(key, body, o);
      if (!tricked && key.includes("/manifest/")) {
        tricked = true;
        throw new (await import("./s3.js")).S3Error("lost response", 503);
      }
      return r;
    };
    const seq = await s.commit([{ t: "put", table: "balances", key: "a", value: { amount: "1" } }], { durable: "s3" });
    expect(s.durableSeq()).toBe(seq);
    await s.commit([{ t: "put", table: "balances", key: "b", value: { amount: "2" } }], { durable: "s3" });
    const s2 = await openStore(opts(objects, await tmp()));
    expect(s2.stateTable("balances").size).toBe(2);
  });

  it("a writer stalled past manifest cleanup is fenced instead of writing into a deleted slot", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    await s1.commit([{ t: "put", table: "balances", key: "z", value: { amount: "1" } }], { durable: "s3" });
    const s2 = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    for (let i = 0; i < 15; i++) await s2.commit([{ t: "put", table: "meta", key: "i", value: i }], { durable: "s3" });
    await s2.snapshot(); // deletes old manifests, including the slot s1 would write next
    await expect(s1.commit([{ t: "put", table: "balances", key: "zombie", value: { amount: "1" } }], { durable: "s3" })).rejects.toBeInstanceOf(FencedError);
    const s3 = await openStore(opts(objects, await tmp()));
    expect(s3.stateTable("meta").get("i")).toBe(14);
    expect(s3.stateTable("balances").has("zombie")).toBe(false);
    expect(s3.stateTable("balances").get("z")).toEqual({ amount: "1" });
  });

  it("refuses to open when stored data names an undeclared table, leaving S3 untouched", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp()));
    await s1.commit([{ t: "put", table: "meta", key: "m", value: 1 }], { durable: "s3" });
    await s1.close(); // snapshot holds meta
    const s2 = await openStore(opts(objects, await tmp()));
    await s2.commit([{ t: "put", table: "meta", key: "m", value: 2 }], { durable: "s3" }); // and a WAL op on meta
    await s2.close().catch(() => {});
    const before = new Map(objects.objects);
    await expect(Store.open(opts(objects, await tmp(), { tables: { state: ["balances"] } }))).rejects.toThrow(/undeclared table meta/);
    expect(new Map(objects.objects)).toEqual(before);
    const s3 = await openStore(opts(objects, await tmp()));
    expect(s3.stateTable("meta").get("m")).toBe(2);
  });

  it("refuses to open when only the wal names an undeclared table", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await Store.open(opts(objects, await tmp(), { flushMs: 1_000_000 }));
    await s1.commit([{ t: "put", table: "meta", key: "m", value: 1 }], { durable: "s3" });
    await expect(Store.open(opts(objects, await tmp(), { tables: { state: ["balances"] } }))).rejects.toThrow(/undeclared table meta/);
  });

  it("uses the cached snapshot only when key and size match", async () => {
    const objects = new MemoryObjectStore();
    const dir = await tmp();
    const s1 = await openStore(opts(objects, dir));
    await s1.commit([{ t: "put", table: "balances", key: "a", value: { amount: "1" } }]);
    await s1.close();
    const snapKey = keys(objects, "snap/").find((k) => k.endsWith("balances.parquet"))!;
    const cached = join(dir, "cache", snapKey.slice("hop/test/".length));
    const { readFile, writeFile } = await import("node:fs/promises");
    const good = await readFile(cached);
    await writeFile(cached, good.subarray(0, good.byteLength - 5)); // truncated cache file
    const s2 = await openStore(opts(objects, dir));
    expect(s2.stateTable("balances").get("a")).toEqual({ amount: "1" });
    expect((await readFile(cached)).byteLength).toBe(good.byteLength); // re-cached from S3
  });

  it("refuses a snapshot whose S3 size differs from the manifest", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp()));
    await s1.commit([{ t: "put", table: "balances", key: "a", value: { amount: "1" } }]);
    await s1.close();
    const snapKey = keys(objects, "snap/").find((k) => k.endsWith("balances.parquet"))!;
    objects.objects.set(snapKey, objects.objects.get(snapKey)!.subarray(0, 10));
    await expect(Store.open(opts(objects, await tmp()))).rejects.toThrow(/the manifest says/);
  });

  it("memory does not follow caller mutation after commit", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    const value = { amount: "1" };
    await s.commit([{ t: "put", table: "balances", key: "a", value }]);
    value.amount = "999";
    expect(s.stateTable("balances").get("a")).toEqual({ amount: "1" });
  });

  it("values read are deeply frozen: mutating one throws instead of diverging from the WAL (live, snapshot, WAL replay)", async () => {
    const objects = new MemoryObjectStore();
    const tables = { state: ["balances", "meta"], log: [{ name: "ev", keepPerPartition: 5, sortKey: (r: unknown) => [(r as { n: number }).n] }] };
    const s1 = await openStore(opts(objects, await tmp(), { tables }));
    await s1.commit([{ t: "put", table: "balances", key: "a", value: { amount: "1", tags: ["x"], deep: { n: 1 } } }, { t: "append", table: "ev", partition: "p", row: { n: 1, list: [1] } }]);
    const check = (s: Store) => {
      const v = s.stateTable<{ amount: string; tags: string[]; deep: { n: number } }>("balances").get("a")!;
      expect(() => (v.amount = "2")).toThrow(TypeError);
      expect(() => v.tags.push("y")).toThrow(TypeError);
      expect(() => (v.deep.n = 2)).toThrow(TypeError);
      const [row] = s.logTable<{ n: number; list: number[] }>("ev").latest("p", 1);
      expect(() => row!.list.push(2)).toThrow(TypeError);
      expect(s.stateTable("balances").get("a")).toEqual({ amount: "1", tags: ["x"], deep: { n: 1 } });
    };
    check(s1);
    await s1.snapshot();
    await s1.commit([{ t: "put", table: "meta", key: "m", value: [1, { a: 1 }] }], { durable: "s3" });
    const s2 = await openStore(opts(objects, await tmp(), { tables })); // snapshot + WAL objects from S3
    check(s2);
    expect(Object.isFrozen((s2.stateTable("meta").get("m") as unknown[])[1])).toBe(true);
  });

  it("rejects ops on undeclared tables and appends to state tables", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    await expect(s.commit([{ t: "put", table: "nope", key: "a", value: 1 }])).rejects.toThrow(/unknown table/);
    await expect(s.commit([{ t: "append", table: "balances", partition: "p", row: 1 }])).rejects.toThrow(/unknown log table/);
    expect(() => s.stateTable("nope")).toThrow(/unknown state table/);
    expect(s.seq).toBe(0);
  });

  it("a commit made while a snapshot encodes is neither in that snapshot nor lost", async () => {
    const objects = new MemoryObjectStore();
    const tables = { state: ["balances", "meta"], log: [{ name: "swaps", keepPerPartition: 2, sortKey: (r: unknown) => [(r as { b: number }).b] }] };
    const s = await openStore(opts(objects, await tmp(), { tables, flushMs: 3_600_000 }));
    for (let i = 0; i < 200_000; i += 20_000) await s.commit(Array.from({ length: 20_000 }, (_, j) => ({ t: "put" as const, table: "balances", key: `k${i + j}`, value: { n: i + j } })));
    await s.commit([1, 2, 3].map((b) => ({ t: "append" as const, table: "swaps", partition: "P", row: { b } })));
    await s.flush(); // the S3 lane is idle: the snapshot captures its state right away
    const before = s.seq;
    let seqAtUpload = -1;
    const put = objects.put.bind(objects);
    objects.put = (key, body, o) => {
      if (key.includes("/snap/") && seqAtUpload < 0) seqAtUpload = s.seq;
      return put(key, body, o);
    };
    const snap = s.snapshot();
    await new Promise((r) => setImmediate(r)); // the snapshot has captured its state and is encoding
    const late = [
      { t: "put" as const, table: "balances", key: "k0", value: { n: -1 } },
      { t: "del" as const, table: "balances", key: "k1" },
      { t: "put" as const, table: "balances", key: "new", value: { n: 7 } },
      { t: "put" as const, table: "meta", key: "late", value: true },
      { t: "append" as const, table: "swaps", partition: "P", row: { b: 0 } },
      { t: "append" as const, table: "swaps", partition: "Q", row: { b: 9 } },
    ];
    expect(await s.commit(late)).toBe(before + 1);
    // visible at once, while the snapshot still encodes
    const bal = s.stateTable("balances");
    expect([bal.get("k0"), bal.get("k1"), bal.has("k1"), bal.get("new"), bal.size]).toEqual([{ n: -1 }, undefined, false, { n: 7 }, 200_000]);
    expect([...bal.scan("k19999")].length).toBe(11);
    await snap;
    expect(seqAtUpload).toBe(before + 1); // the commit was applied before the snapshot finished encoding
    expect([bal.get("k0"), bal.has("k1"), bal.size]).toEqual([{ n: -1 }, false, 200_000]);

    // the snapshot holds exactly the state at `before`
    const m = (await readLatestManifest(objects, "hop/test/"))!;
    expect(m.snapshotSeq).toBe(before);
    const snapBal = new Map((await readStateSnapshot(objects.objects.get(m.snapshots.balances!.key)!)).rows);
    expect([snapBal.size, snapBal.get("k0"), snapBal.get("k1"), snapBal.has("new")]).toEqual([200_000, { n: 0 }, { n: 1 }, false]);
    expect((await readStateSnapshot(objects.objects.get(m.snapshots.meta!.key)!)).rows).toEqual([]);
    const snapLog = await readLogParquet(bytesBuffer(objects.objects.get(m.logSnapshots!.swaps!.key)!));
    expect(snapLog.entries.map((e) => [e.p, e.row])).toEqual([1, 2, 3].map((b) => ["P", { b }]));

    // and the late commit is not lost: a fresh disk recovers it from the WAL on S3
    await s.flush();
    const again = await openStore(opts(objects, await tmp(), { tables }));
    const b2 = again.stateTable("balances");
    expect([b2.get("k0"), b2.has("k1"), b2.get("new"), b2.size, again.stateTable("meta").get("late")]).toEqual([{ n: -1 }, false, { n: 7 }, 200_000, true]);
    expect([...again.logTable("swaps").memoryRows("P")]).toEqual([0, 1, 2, 3].map((b) => ({ b })));
    expect([...again.logTable("swaps").memoryRows("Q")]).toEqual([{ b: 9 }]);
  });

  it("a durable commit made just before close resolves through close's final flush", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp(), { flushMs: 3_600_000 }));
    const durable = s.commit([{ t: "put", table: "meta", key: "k", value: 1 }], { durable: "s3" });
    await s.close();
    expect(await durable).toBe(1);
    expect(s.durableSeq()).toBe(1);
  });

  it("close during an S3 outage rejects durable waiters and stops retrying", async () => {
    // fake timeouts and intervals (not setImmediate or I/O): retries fire only when the test advances the clock, so
    // the race that once queued one more flush behind close's own (a PUT after close) is staged on every run
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const until = async (cond: () => boolean) => {
      for (let i = 0; !cond(); i++) {
        if (i > 10_000) throw new Error("condition never met");
        await new Promise((r) => setImmediate(r));
      }
    };
    try {
      const objects = new MemoryObjectStore();
      let failures = 0;
      const s = await openStore(opts(objects, await tmp(), { flushMs: 5, onError: () => failures++ }));
      let calls = 0;
      let gate: Promise<void> | null = null;
      let release = () => {};
      const put = objects.put.bind(objects);
      objects.put = async () => {
        calls++;
        if (gate) await gate;
        throw new Error("S3 is down");
      };
      const durable = s.commit([{ t: "put", table: "meta", key: "k", value: 1 }], { durable: "s3" });
      durable.catch(() => {});
      await until(() => vi.getTimerCount() === 3); // two intervals plus the flush scheduled for the durable waiter
      await vi.advanceTimersByTimeAsync(0);
      await until(() => failures === 1); // the flush failed and a retry is scheduled
      expect(calls).toBe(1);
      gate = new Promise<void>((r) => (release = r));
      const closing = s.close();
      await until(() => calls === 2); // close's own flush is parked inside its PUT
      await vi.advanceTimersByTimeAsync(10); // the retry comes due while close is flushing
      release();
      await expect(closing).rejects.toThrow(/S3 is down/);
      await expect(durable).rejects.toThrow(/closed/);
      await expect(s.waitDurable(s.seq)).rejects.toThrow(/closed/);
      expect(vi.getTimerCount()).toBe(0); // nothing left to fire
      vi.useRealTimers();
      await new Promise((r) => setTimeout(r, 50)); // a flush queued behind close's would PUT by now
      expect(calls).toBe(2);
      objects.put = put;
    } finally {
      vi.useRealTimers();
    }
  });

  it("failed() is null while healthy and names the reason once fenced; commits are refused", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp(), { onError: () => {} }));
    await s1.commit([{ t: "put", table: "meta", key: "k", value: 1 }], { durable: "s3" });
    expect(s1.failed()).toBeNull();
    await openStore(opts(objects, await tmp())); // a newer writer claims the prefix
    await s1.commit([{ t: "put", table: "meta", key: "k", value: 2 }]);
    await expect(s1.flush()).rejects.toThrow(FencedError);
    expect(s1.failed()).toMatch(/fenced at hop\/test\//);
    await expect(s1.commit([{ t: "put", table: "meta", key: "k", value: 3 }])).rejects.toThrow(FencedError);
  });

  it("recovery replays WAL objects from the local WAL of the same writer instead of downloading them", async () => {
    class Counting extends MemoryObjectStore {
      walGets = 0;
      override async get(key: string, range?: { start: number; end: number }) {
        if (key.includes("/wal/")) this.walGets++;
        return super.get(key, range);
      }
    }
    const objects = new Counting();
    const dir = await tmp();
    const s1 = await Store.open(opts(objects, dir, { flushMs: 3_600_000 }));
    for (let i = 1; i <= 5; i++) await s1.commit([{ t: "put", table: "meta", key: `k${i}`, value: i }], { durable: "s3" });
    await s1.commit([{ t: "put", table: "meta", key: "local", value: true }]); // only in the local WAL
    const walObjects = keys(objects, "wal/").length;
    expect(walObjects).toBe(5);
    // a crash: s1 is abandoned without close (its timers are unref'd and hour-long)
    const s2 = await openStore(opts(objects, dir, { flushMs: 3_600_000 }));
    expect(objects.walGets).toBe(0);
    expect([1, 2, 3, 4, 5].map((i) => s2.stateTable("meta").get(`k${i}`))).toEqual([1, 2, 3, 4, 5]);
    expect(s2.stateTable("meta").get("local")).toBe(true);
    // the new epoch's local WAL starts with every batch since the snapshot, so the next restart needs no GETs either
    await s2.commit([{ t: "put", table: "meta", key: "k6", value: 6 }], { durable: "s3" });
    const s3 = await openStore(opts(objects, dir, { flushMs: 3_600_000 }));
    expect(objects.walGets).toBe(0);
    expect([s3.stateTable("meta").get("k6"), s3.stateTable("meta").get("local"), s3.seq]).toEqual([6, true, 7]);
    // a fresh disk still downloads them
    const s4 = await openStore(opts(objects, await tmp(), { flushMs: 3_600_000 }));
    expect(objects.walGets).toBeGreaterThan(0);
    expect(s4.stateTable("meta").get("k6")).toBe(6);
  });

  it("commits in the same tick share one fsync and keep order", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    const seqs = await Promise.all([1, 2, 3].map((i) => s.commit([{ t: "put", table: "meta", key: "c", value: i }])));
    expect(seqs).toEqual([1, 2, 3]);
    expect(s.stateTable("meta").get("c")).toBe(3);
  });
});

describe("state snapshot parquet", () => {
  it("round-trips rows and carries table metadata", async () => {
    const buf = await writeStateSnapshot("balances", [
      ["a", { amount: "1", tags: ["x"] }],
      ["b", null],
    ]);
    expect(await readStateSnapshot(buf)).toEqual({
      table: "balances",
      rows: [
        ["a", { amount: "1", tags: ["x"] }],
        ["b", null],
      ],
    });
    expect((await readStateSnapshot(await writeStateSnapshot("empty", []))).rows).toEqual([]);
  });
});

describe("StateTable freeze", () => {
  it("keeps the frozen rows as they were while reads and writes see the overlay, then merges it", () => {
    const t = new StateTable("t");
    for (const [k, v] of [["a", 1], ["b", 2], ["c", null]] as const) t.apply({ t: "put", table: "t", key: k, value: v });
    const frozen = t.freeze();
    t.apply({ t: "put", table: "t", key: "a", value: 10 });
    t.apply({ t: "del", table: "t", key: "b" });
    t.apply({ t: "put", table: "t", key: "d", value: 4 });
    t.apply({ t: "del", table: "t", key: "nope" });
    t.apply({ t: "put", table: "t", key: "b", value: 20 });
    t.apply({ t: "del", table: "t", key: "c" });
    expect([...frozen]).toEqual([["a", 1], ["b", 2], ["c", null]]);
    expect([t.get("a"), t.get("b"), t.has("c"), t.get("d"), t.size]).toEqual([10, 20, false, 4, 3]);
    expect([...t.scan()]).toEqual([["a", 10], ["b", 20], ["d", 4]]);
    expect(new Map(t.entries())).toEqual(new Map([["a", 10], ["b", 20], ["d", 4]]));
    expect(() => t.freeze()).toThrow(/frozen/);
    t.thaw();
    expect(new Map(t.entries())).toEqual(new Map([["a", 10], ["b", 20], ["d", 4]]));
    expect([[...t.keys()].sort(), [...t.values()].sort(), t.size]).toEqual([["a", "b", "d"], [10, 20, 4], 3]);
  });
});

describe("Store after close or fencing", () => {
  it("snapshot, flush and sealLogs reject with 'store is closed' after close", async () => {
    const objects = new MemoryObjectStore();
    const s = await Store.open(opts(objects, await tmp(), { tables: s1Tables() }));
    await s.commit([{ t: "put", table: "meta", key: "k", value: 1 }]);
    await s.close();
    const manifests = keys(objects, "manifest/").length;
    await expect(s.snapshot()).rejects.toThrow(/store is closed/);
    await expect(s.flush()).rejects.toThrow(/store is closed/);
    await expect(s.sealLogs()).rejects.toThrow(/store is closed/);
    expect(keys(objects, "manifest/").length).toBe(manifests); // nothing was published
  });

  it("snapshot, flush and sealLogs reject with the FencedError on a fenced store", async () => {
    const objects = new MemoryObjectStore();
    const s1 = await openStore(opts(objects, await tmp(), { flushMs: 1_000_000, tables: s1Tables() }));
    await s1.commit([{ t: "put", table: "meta", key: "k", value: 1 }, { t: "append", table: "l", partition: "p", row: 1 }]);
    await openStore(opts(objects, await tmp(), { flushMs: 1_000_000, tables: s1Tables() }));
    await expect(s1.flush()).rejects.toBeInstanceOf(FencedError);
    await expect(s1.snapshot()).rejects.toBeInstanceOf(FencedError);
    await expect(s1.sealLogs()).rejects.toBeInstanceOf(FencedError);
    await s1.close();
    await expect(s1.snapshot()).rejects.toBeInstanceOf(FencedError); // the reason it stopped, also after close
  });
});

const s1Tables = () => ({ state: ["meta"], log: [{ name: "l", keepPerPartition: 0, sortKey: (r: unknown) => [r as number] }] });

describe("Store.open option checks", () => {
  it("accepts an empty prefix and prefixes ending with '/'", async () => {
    for (const prefix of ["", "a/", "my-app/prod_1/", "x.y/z=1/", "A(1)!*'/"]) {
      const s = await openStore(opts(new MemoryObjectStore(), await tmp(), { prefix }));
      expect(s.seq).toBe(0);
    }
  });

  it.each([
    ["missing trailing slash", "app"],
    ["leading slash", "/app/"],
    ["empty segment", "app//x/"],
    ["dot segment", "app/./x/"],
    ["dot-dot segment", "app/../x/"],
    ["backslash", "app\\x/"],
    ["control character", "app\nx/"],
    ["space", "my app/"],
    ["non-ASCII", "café/"],
    ["ampersand", "a&b/"],
    ["only a slash", "/"],
  ])("rejects a prefix with a %s", async (_why, prefix) => {
    const objects = new MemoryObjectStore();
    await expect(Store.open(opts(objects, await tmp(), { prefix }))).rejects.toThrow(/hopstore: invalid prefix/);
    expect(objects.objects.size).toBe(0);
  });

  it("rejects an empty localDir", async () => {
    for (const localDir of ["", "  "]) await expect(Store.open(opts(new MemoryObjectStore(), localDir))).rejects.toThrow(/hopstore: localDir must be a non-empty path/);
  });
});

describe("Store onError", () => {
  it("defaults to console.error, whatever LOG_SILENT says; onError replaces it", async () => {
    const before = process.env.LOG_SILENT;
    process.env.LOG_SILENT = "1";
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const objects = new MemoryObjectStore();
      const s = await openStore(opts(objects, await tmp(), { flushMs: 5, onError: undefined }));
      await s.commit([{ t: "put", table: "meta", key: "k", value: 1 }]);
      objects.failNext(1);
      await vi.waitFor(() => expect(spy).toHaveBeenCalledWith("hopstore:", expect.any(Error)));
      const seen: unknown[] = [];
      const quiet = await openStore(opts(objects, await tmp(), { flushMs: 5, onError: (e) => seen.push(e) }));
      spy.mockClear();
      await quiet.commit([{ t: "put", table: "meta", key: "k", value: 2 }]);
      objects.failNext(1);
      await vi.waitFor(() => expect(seen.length).toBeGreaterThan(0));
      expect(spy).not.toHaveBeenCalled();
    } finally {
      spy.mockRestore();
      if (before === undefined) delete process.env.LOG_SILENT;
      else process.env.LOG_SILENT = before;
    }
  });
});

describe("Store.describe", () => {
  it("reports the manifest, sequence numbers and per-table counts without touching S3", async () => {
    const objects = new MemoryObjectStore();
    const tables = { state: ["meta"], log: [{ name: "ev", keepPerPartition: 1, sortKey: (r: unknown) => [r as number] }] };
    const s = await openStore(opts(objects, await tmp(), { tables, flushMs: 3_600_000 }));
    expect(s.describe()).toEqual({ prefix: "hop/test/", epoch: 1, manifestSeq: 1, seq: 0, durableSeq: 0, snapshotSeq: 0, walObjects: 0, state: { meta: { rows: 0 } }, log: { ev: { memoryRows: 0, segments: 0, segmentRows: 0, segmentBytes: 0 } } });
    await s.commit([{ t: "put", table: "meta", key: "a", value: 1 }, ...[1, 2, 3].map((n) => ({ t: "append" as const, table: "ev", partition: "p", row: n }))]);
    await s.commit([{ t: "put", table: "meta", key: "b", value: 2 }], { durable: "s3" });
    expect(s.describe()).toMatchObject({ manifestSeq: 2, seq: 2, durableSeq: 2, snapshotSeq: 0, walObjects: 1, state: { meta: { rows: 2 } }, log: { ev: { memoryRows: 3 } } });
    expect(await s.sealLogs()).toBe(2);
    const d = s.describe();
    expect(d).toMatchObject({ manifestSeq: 3, seq: 2, durableSeq: 2, snapshotSeq: 2, walObjects: 0, log: { ev: { memoryRows: 1, segments: 1, segmentRows: 2 } } });
    expect(d.log.ev!.segmentBytes).toBeGreaterThan(0);
    const gets = vi.spyOn(objects, "get");
    const lists = vi.spyOn(objects, "list");
    s.describe();
    expect([gets.mock.calls.length, lists.mock.calls.length]).toEqual([0, 0]);
  });
});
