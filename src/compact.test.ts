import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { Json } from "./codec.js";
import { MemoryObjectStore } from "./memory-store.js";
import { S3Error } from "./s3.js";
import { Store, type StoreOptions } from "./store.js";

const dirs: string[] = [];
const tmp = async () => {
  const d = await mkdtemp(join(tmpdir(), "hopstore-compact-"));
  dirs.push(d);
  return d;
};
const open: Store[] = [];
afterEach(async () => {
  for (const s of open.splice(0)) await s.close().catch(() => {});
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

/** Fault injection at the next manifest PUT: "kill" (the process dies before it), "lose" (it lands, the caller sees an error). */
class FaultStore extends MemoryObjectStore {
  atManifest: "kill" | "lose" | null = null;
  dead = false;
  /** A second process over the same bucket. */
  view(): FaultStore {
    const v = new FaultStore();
    Object.defineProperty(v, "objects", { value: this.objects });
    return v;
  }
  private check() {
    if (this.dead) throw new S3Error("process killed", 500);
  }
  override async put(key: string, body: Uint8Array, o: { ifNoneMatch?: "*"; ifMatch?: string; contentType?: string } = {}) {
    this.check();
    if (this.atManifest && key.includes("/manifest/")) {
      const f = this.atManifest;
      this.atManifest = null;
      if (f === "kill") {
        this.dead = true;
        this.check();
      }
      await super.put(key, body, o);
      throw new S3Error("response lost", 500);
    }
    return super.put(key, body, o);
  }
  override async get(key: string, r?: { start: number; end: number }) {
    this.check();
    return super.get(key, r);
  }
  override async head(key: string) {
    this.check();
    return super.head(key);
  }
  override async list(prefix: string) {
    this.check();
    return super.list(prefix);
  }
  override async delete(key: string) {
    this.check();
    return super.delete(key);
  }
}

type Ev = { n: number; v: string };
const opts = (objects: MemoryObjectStore, localDir: string): StoreOptions => ({
  objects,
  prefix: "hop/test/",
  localDir,
  flushMs: 3_600_000,
  snapshotEveryMs: 3_600_000,
  tables: { state: ["meta"], log: [{ name: "ev", keepPerPartition: 0, sortKey: (r: Json) => [(r as Ev).n] }, { name: "other", keepPerPartition: 0, sortKey: (r: Json) => [(r as Ev).n] }] },
  onError: () => {},
});
const openStore = async (o: StoreOptions) => {
  const s = await Store.open(o);
  open.push(s);
  return s;
};
const ev = (partition: string, n: number, v = `${partition}${n}`) => ({ t: "append" as const, table: "ev", partition, row: { n, v } });
const all = async (s: Store, partition?: string) => {
  const out: string[] = [];
  for await (const [p, r] of s.logTable<Ev>("ev").scanEntries({ partition, from: [0], to: [1e9] })) out.push(`${p}:${r.n}:${r.v}`);
  return out;
};
const segs = (s: Store, table = "ev") => (s as unknown as { manifest: { segments: Record<string, { key: string; rows: number }[]> } }).manifest.segments[table] ?? [];
const segObjects = (o: MemoryObjectStore, table = "ev") => [...o.objects.keys()].filter((k) => k.startsWith(`hop/test/seg/${table}/`)).sort();
/** Every scan an answer can depend on: all partitions, each partition, and two inner ranges. */
const answers = async (s: Store) => ({ all: await all(s), a: await all(s, "a"), b: await all(s, "b"), mid: (await all(s)).filter((x) => Number(x.split(":")[1]) >= 15 && Number(x.split(":")[1]) < 42) });

/** Six seals ("hours" 0..5), each a segment with rows 10h..10h+3 of a and b; key a:5 gets a newer version in hour 2 and 4, and one in memory. */
async function sixHours(s: Store) {
  for (let h = 0; h < 6; h++) {
    const ops = [0, 1, 2, 3].flatMap((i) => [ev("a", h * 10 + i), ev("b", h * 10 + i)]);
    if (h === 2) ops.push(ev("a", 5, "v2"));
    if (h === 4) ops.push(ev("a", 5, "v3"));
    await s.commit(ops);
    expect(await s.sealLogs({ tables: ["ev"] })).toBe(ops.length);
  }
  await s.commit([ev("a", 5, "v4")]); // in memory: the newest
}

describe("compactLogs", () => {
  it("merges consecutive segments of one group into one: every scan returns the same rows, equal keys oldest first; inputs are deleted", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    await sixHours(s);
    const want = await answers(s);
    expect(want.a.filter((x) => x.startsWith("a:5:"))).toEqual(["a:5:v2", "a:5:v3", "a:5:v4"]); // three versions: two sealed in different hours, one in memory
    expect(segs(s).length).toBe(6);
    const before = segObjects(objects);
    // hours 0..3 by their newest key
    expect(await s.compactLogs({ table: "ev", group: (g) => ((g.sortMax[0] as number) < 40 ? "old" : null) })).toBe(3);
    expect(segs(s).map((r) => r.rows)).toEqual([33, 9, 8]);
    expect(await answers(s)).toEqual(want);
    expect(segObjects(objects).length).toBe(3);
    expect(before.filter((k) => segObjects(objects).includes(k)).length).toBe(2); // hours 4 and 5 untouched
    // nothing left to merge
    expect(await s.compactLogs({ table: "ev", group: (g) => ((g.sortMax[0] as number) < 40 ? "old" : null) })).toBe(0);
    await s.close();
    const again = await openStore(opts(objects, await tmp())); // from S3 alone
    expect(await answers(again)).toEqual(want);
  });

  it("cuts runs at maxRows, merges only consecutive segments of a group (the order between groups stays), stops after maxRuns", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    await sixHours(s);
    const want = await answers(s);
    // groups by hour: x x y x x x (hour 2 alone in y): never merged across it
    const group = (g: { sortMax: (string | number)[] }) => ((g.sortMax[0] as number) >= 20 && (g.sortMax[0] as number) < 30 ? "y" : "x");
    expect(await s.compactLogs({ table: "ev", group, maxRows: 17, maxRuns: 1 })).toBe(1); // hours 0, 1 (8 + 8 rows)
    expect(segs(s).map((r) => r.rows)).toEqual([16, 9, 8, 9, 8]);
    expect(await s.compactLogs({ table: "ev", group, maxRows: 17 })).toBe(1); // hours 3, 4 (8 + 9); 5 would exceed 17
    expect(segs(s).map((r) => r.rows)).toEqual([16, 9, 17, 8]);
    expect(await answers(s)).toEqual(want);
    expect(await s.compactLogs({ table: "ev", group })).toBe(1);
    expect(segs(s).map((r) => r.rows)).toEqual([16, 9, 25]);
    expect(await answers(s)).toEqual(want);
    await expect(s.compactLogs({ table: "nope", group })).rejects.toThrow(/nope/);
  });

  it("consecutive: false merges the segments of a group wherever they are (a table without keys in two segments)", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    for (let h = 0; h < 6; h++) {
      await s.commit([0, 1, 2].flatMap((i) => [ev("a", h * 10 + i), ev("b", h * 10 + i)]));
      await s.sealLogs({ tables: ["ev"] });
    }
    const want = await answers(s);
    const group = (g: { sortMax: (string | number)[] }) => ((g.sortMax[0] as number) % 20 < 10 ? "even" : "odd");
    expect(await s.compactLogs({ table: "ev", group })).toBe(0); // x y x y x y: nothing consecutive
    expect(await s.compactLogs({ table: "ev", group, consecutive: false })).toBe(4);
    expect(segs(s).map((r) => r.rows)).toEqual([18, 18]);
    expect(await answers(s)).toEqual(want);
    expect(segs(s, "other")).toEqual([]);
  });

  it("merges run off the S3 lane: a commit waiting for S3 resolves while a long compaction is still uploading", async () => {
    let hold: Promise<void> | null = null;
    let release = () => {};
    let held = 0;
    const objects = new (class extends MemoryObjectStore {
      override async put(...args: Parameters<MemoryObjectStore["put"]>) {
        // the compaction's merged segments (inputs are sealed before `hold` is set)
        if (hold && args[0].includes("/seg/ev/")) {
          held++;
          await hold;
        }
        return super.put(...args);
      }
    })();
    const s = await openStore(opts(objects, await tmp()));
    await sixHours(s);
    const want = await answers(s);
    hold = new Promise<void>((r) => (release = r));
    let compacted = false;
    const compacting = s.compactLogs({ table: "ev", group: (g) => String(Math.floor((g.sortMax[0] as number) / 20)), maxRuns: 3 }).then((n) => ((compacted = true), n));
    await new Promise((r) => setTimeout(r, 50));
    expect(held).toBe(1); // the first merge is uploading
    await s.commit([ev("b", 77)], { durable: "s3" }); // published meanwhile
    expect(compacted).toBe(false);
    release();
    hold = null;
    expect(await compacting).toBe(3); // three runs of two
    expect(await answers(s)).toEqual({ ...want, all: await all(s), b: await all(s, "b") });
    expect((await all(s, "b")).at(-1)).toBe("b:77:b77");
  });

  it("close() waits for a compaction in flight (it stops after its current run); nothing is lost", async () => {
    let hold: Promise<void> | null = null;
    let release = () => {};
    const objects = new (class extends MemoryObjectStore {
      override async put(...args: Parameters<MemoryObjectStore["put"]>) {
        if (hold && args[0].includes("/seg/ev/")) await hold;
        return super.put(...args);
      }
    })();
    const s = await openStore(opts(objects, await tmp()));
    await sixHours(s);
    const want = await answers(s);
    hold = new Promise<void>((r) => (release = r));
    let compactionDone = false;
    const compacting = s.compactLogs({ table: "ev", group: (g) => String(Math.floor((g.sortMax[0] as number) / 20)), maxRuns: 3 }).finally(() => (compactionDone = true));
    compacting.catch(() => {});
    await new Promise((r) => setTimeout(r, 50));
    let closed = false;
    const closing = s.close().then(() => (closed = true));
    await new Promise((r) => setTimeout(r, 50));
    expect(closed).toBe(false); // waiting for the merge being uploaded
    release();
    hold = null;
    await closing;
    expect(compactionDone).toBe(true);
    const again = await openStore(opts(objects, await tmp()));
    expect(await answers(again)).toEqual(want);
  });

  it("killed right before its manifest PUT: a fresh process sees the old segments; the orphan is deleted later", async () => {
    const objects = new FaultStore();
    const s = await openStore(opts(objects, await tmp()));
    await sixHours(s);
    await s.commit([ev("b", 99)], { durable: "s3" });
    const want = await answers(s);
    const before = segObjects(objects);
    objects.atManifest = "kill";
    await expect(s.compactLogs({ table: "ev", group: () => "all" })).rejects.toThrow(/killed/);
    expect(segObjects(objects).length).toBe(before.length + 1); // the merged segment, unpublished
    const s2 = await openStore(opts(objects.view(), await tmp()));
    expect(segs(s2).length).toBe(6);
    expect(await answers(s2)).toEqual(want);
    await s2.snapshot(); // its cleanup deletes the orphan (an older epoch's object no manifest names)
    expect(segObjects(objects)).toEqual(before);
    expect(await s2.compactLogs({ table: "ev", group: () => "all" })).toBe(5);
    expect(await answers(s2)).toEqual(want);
    expect(segObjects(objects).length).toBe(1);
  });

  it("a manifest PUT that lands but reports failure: the compaction rejects, the next publish adopts it, nothing is lost or doubled", async () => {
    const objects = new FaultStore();
    const s = await openStore(opts(objects, await tmp()));
    await sixHours(s);
    const want = await answers(s);
    objects.atManifest = "lose";
    await expect(s.compactLogs({ table: "ev", group: () => "all" })).rejects.toThrow(/lost/);
    expect(await answers(s)).toEqual(want);
    await s.commit([{ t: "put", table: "meta", key: "k", value: 1 }], { durable: "s3" }); // adopts the landed manifest
    expect(segs(s).length).toBe(1);
    expect(await answers(s)).toEqual(want);
    await s.snapshot();
    expect(segObjects(objects).length).toBe(1);
    const s2 = await openStore(opts(objects.view(), await tmp()));
    expect(await answers(s2)).toEqual(want);
  });

  it("a scan in flight keeps the segments it may read until it ends (they are deleted at a later cleanup)", async () => {
    const objects = new MemoryObjectStore();
    const s = await openStore(opts(objects, await tmp()));
    await sixHours(s);
    const want = await all(s, "a");
    const before = segObjects(objects);
    const it = s.logTable<Ev>("ev").scan({ partition: "a", from: [0], to: [1e9] })[Symbol.asyncIterator]();
    const got = [(await it.next()).value as Ev];
    expect(await s.compactLogs({ table: "ev", group: () => "all" })).toBe(5);
    expect(segObjects(objects).filter((k) => before.includes(k))).toEqual(before); // still there: the scan holds them
    for (let r = await it.next(); !r.done; r = await it.next()) got.push(r.value);
    expect(got.map((r) => `a:${r.n}:${r.v}`)).toEqual(want);
    await s.snapshot();
    expect(segObjects(objects).filter((k) => before.includes(k))).toEqual([]);
    expect(await all(s, "a")).toEqual(want);
  });
});

describe("sealLogs tables", () => {
  it("rejects a table named twice", async () => {
    const s = await openStore(opts(new MemoryObjectStore(), await tmp()));
    await s.commit([ev("a", 1)]);
    await expect(s.sealLogs({ tables: ["ev", "ev"] })).rejects.toThrow(/twice/);
    expect(await s.sealLogs({ tables: ["ev"] })).toBe(1);
  });
});
