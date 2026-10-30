import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Json } from "./codec.js";
import { MemoryObjectStore } from "./memory-store.js";
import { S3Error, type ObjectInfo } from "./s3.js";
import { Store, type StoreOptions } from "./store.js";

const dirs: string[] = [];
const tmp = async () => {
  const d = await mkdtemp(join(tmpdir(), "hopstore-crash-"));
  dirs.push(d);
  return d;
};
const open: Store[] = [];
afterEach(async () => {
  for (const s of open.splice(0)) await s.close().catch(() => {});
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

/** A process that can be killed: once `dead`, every call fails (so an abandoned Store cannot flush); `killAtManifest` kills it right before the next manifest PUT. */
class KillableStore extends MemoryObjectStore {
  dead = false;
  killAtManifest = false;
  /** A second process over the same bucket: shares the objects, not the kill switch. */
  view(): KillableStore {
    const v = new KillableStore();
    Object.defineProperty(v, "objects", { value: this.objects });
    return v;
  }
  private check() {
    if (this.dead) throw new S3Error("process killed", 500);
  }
  override async put(key: string, body: Uint8Array, o: { ifNoneMatch?: "*"; ifMatch?: string; contentType?: string } = {}) {
    this.check();
    if (this.killAtManifest && key.includes("/manifest/")) {
      this.dead = true;
      this.check();
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
  override async list(prefix: string): Promise<ObjectInfo[]> {
    this.check();
    return super.list(prefix);
  }
  override async delete(key: string) {
    this.check();
    return super.delete(key);
  }
}

const opts = (objects: MemoryObjectStore, localDir: string): StoreOptions => ({
  objects,
  prefix: "hop/test/",
  localDir,
  flushMs: 3_600_000, // only commits with durable: "s3" flush
  snapshotEveryMs: 3_600_000,
  tables: { state: ["data", "meta"], log: [{ name: "ev", keepPerPartition: 1000, sortKey: (r: Json) => [(r as { n: number }).n] }] },
  onError: () => {},
});
const openStore = async (o: StoreOptions) => {
  const s = await Store.open(o);
  open.push(s);
  return s;
};
const batch = (n: number) => [
  { t: "put" as const, table: "data", key: `k${n}`, value: n },
  { t: "put" as const, table: "data", key: "sum", value: (n * (n + 1)) / 2 }, // overwritten by every batch, so the final value shows which batch was applied last
  { t: "put" as const, table: "meta", key: "cursor", value: n },
  { t: "append" as const, table: "ev", partition: "p", row: { n } }, // not idempotent: a batch replayed twice would add a second row
];
const dump = (s: Store, t: string) => Object.fromEntries([...s.stateTable(t).entries()].sort(([a], [b]) => (a < b ? -1 : 1)));
/** Rows of the log table: the model has exactly one per batch, so a double replay would show duplicates. */
const events = async (s: Store) => {
  const out: number[] = [];
  for await (const r of s.logTable<{ n: number }>("ev").scan({ partition: "p", from: [0], to: [1e9] })) out.push(r.n);
  return out;
};
const upTo = (k: number) => Array.from({ length: k }, (_, i) => i + 1);
const model = (upTo: number) => {
  const data: Record<string, unknown> = {};
  for (let n = 1; n <= upTo; n++) for (const op of batch(n)) if (op.t === "put" && op.table === "data") data[op.key] = op.value;
  return data;
};

describe("crash recovery", () => {
  it("lost local disk resumes from the s3-durable cursor", async () => {
    const objects = new KillableStore();
    const s1 = await openStore(opts(objects, await tmp()));
    for (let n = 1; n <= 100; n++) await s1.commit(batch(n), n === 100 ? { durable: "s3" } : {});
    expect(s1.durableSeq()).toBe(100);
    for (let n = 101; n <= 150; n++) await s1.commit(batch(n)); // local only
    expect(s1.stateTable("meta").get("cursor")).toBe(150);
    // the machine and its disk are gone: a new process with an empty localDir
    const s2 = await openStore(opts(objects.view(), await tmp()));
    expect(s2.stateTable("meta").get("cursor")).toBe(100);
    expect(dump(s2, "data")).toEqual(model(100));
    expect(await events(s2)).toEqual(upTo(100));
    expect(s2.seq).toBe(100);
    // the dead writer is fenced if it comes back
    await expect(s1.commit(batch(151), { durable: "s3" })).rejects.toThrow(/fenced/);
    // resuming from the cursor reapplies 101..150 exactly once
    for (let n = 101; n <= 150; n++) await s2.commit(batch(n));
    expect(dump(s2, "data")).toEqual(model(150));
    expect(await events(s2)).toEqual(upTo(150));
    expect(await events(s2)).toEqual(upTo(150));
    expect(s2.seq).toBe(150);
  });

  it("kill during group commit (after the wal object, before the manifest): same local disk", async () => {
    const objects = new KillableStore();
    const dir = await tmp();
    const s1 = await openStore(opts(objects, dir));
    for (let n = 1; n <= 5; n++) await s1.commit(batch(n), { durable: "s3" });
    const walBefore = [...objects.objects.keys()].filter((k) => k.includes("/wal/")).length;
    objects.killAtManifest = true;
    void s1.commit(batch(6), { durable: "s3" }).catch(() => {}); // never acknowledged: the process dies mid-flush
    await vi.waitFor(() => expect(objects.dead).toBe(true));
    expect([...objects.objects.keys()].filter((k) => k.includes("/wal/")).length).toBe(walBefore + 1); // orphan WAL object
    const s2 = await openStore(opts(objects.view(), dir));
    expect(s2.seq).toBe(6); // batch 6 survived in the local WAL, applied once
    expect(dump(s2, "data")).toEqual(model(6));
    expect(await events(s2)).toEqual(upTo(6));
    await s2.commit(batch(7), { durable: "s3" });
    expect(s2.durableSeq()).toBe(7);
    await s2.close();
    const s3 = await openStore(opts(objects.view(), await tmp()));
    expect(s3.seq).toBe(7);
    expect(dump(s3, "data")).toEqual(model(7));
    expect(await events(s3)).toEqual(upTo(7));
  });

  it("kill during group commit: fresh disk ignores the orphan wal object and reuses its seq", async () => {
    const objects = new KillableStore();
    const s1 = await openStore(opts(objects, await tmp()));
    for (let n = 1; n <= 5; n++) await s1.commit(batch(n), { durable: "s3" });
    objects.killAtManifest = true;
    void s1.commit(batch(6), { durable: "s3" }).catch(() => {});
    await vi.waitFor(() => expect(objects.dead).toBe(true));
    const s2 = await openStore(opts(objects.view(), await tmp()));
    expect(s2.seq).toBe(5); // unpublished batch 6 is not visible
    expect(dump(s2, "data")).toEqual(model(5));
    expect(await events(s2)).toEqual(upTo(5));
    // the replacement writes a different batch 6; the orphan (same seq, older epoch) must not leak in
    await s2.commit([{ t: "put", table: "data", key: "other", value: "x" }, { t: "put", table: "meta", key: "cursor", value: 6 }], { durable: "s3" });
    await s2.snapshot();
    await s2.close();
    const s3 = await openStore(opts(objects.view(), await tmp()));
    expect(s3.seq).toBe(6);
    expect(dump(s3, "data")).toEqual({ ...model(5), other: "x" });
    expect(await events(s3)).toEqual(upTo(5)); // the orphan batch 6 (with its row) never leaks in
    expect(s3.stateTable("meta").get("cursor")).toBe(6);
  });
});
