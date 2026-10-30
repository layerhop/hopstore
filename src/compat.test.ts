/**
 * Stores written before the API hardening still open. `fixtures/store-0.1.0.json` is a MemoryObjectStore dump written
 * by the engine as of commit 53bd7cd: manifests, a snapshot, a log segment and a WAL object after the snapshot.
 */
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MemoryObjectStore, Store, type Json, type StoreOptions } from "./index.js";

const dirs: string[] = [];
const tmp = async () => {
  const d = await mkdtemp(join(tmpdir(), "hopstore-compat-"));
  dirs.push(d);
  return d;
};
afterEach(async () => {
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

const opts = (objects: MemoryObjectStore, localDir: string): StoreOptions => ({
  objects,
  prefix: "fixture/v0/",
  localDir,
  flushMs: 3_600_000,
  snapshotEveryMs: 3_600_000,
  onError: () => {},
  tables: {
    state: ["players", "meta"],
    ranked: [{ name: "by_points", table: "players", group: (k) => k.slice(0, 1), member: (k) => k, score: (v) => BigInt((v as { points: number }).points) }],
    log: [{ name: "events", keepPerPartition: 2, sortKey: (r: Json) => [(r as { at: number }).at] }],
  },
});

describe("stores written by 0.1.0 before the API hardening", () => {
  it("open unchanged from S3 alone: snapshot, WAL object, segment and ranked index", async () => {
    const dump = JSON.parse(await readFile(new URL("./fixtures/store-0.1.0.json", import.meta.url), "utf8")) as Record<string, string>;
    const objects = new MemoryObjectStore();
    for (const [k, v] of Object.entries(dump)) objects.objects.set(k, new Uint8Array(Buffer.from(v, "base64")));
    const s = await Store.open(opts(objects, await tmp()));
    try {
      expect([...s.stateTable("players").scan()]).toEqual([
        ["abe", { points: 0 }],
        ["ada", { points: 120, tags: ["a", "b"] }],
        ["cy", { points: 95 }],
      ]);
      expect(s.stateTable("meta").get("cursor")).toBe(2);
      expect([s.ranked("by_points").top("a", 5), s.ranked("by_points").top("c", 5), s.ranked("by_points").count("b")]).toEqual([[{ member: "ada", score: 120n }], [{ member: "cy", score: 95n }], 0]);
      const rows: Json[] = [];
      for await (const r of s.logTable("events").scan({ from: [0], to: [100] })) rows.push(r);
      expect(rows.map((r) => (r as { at: number }).at)).toEqual([0, 2, 4, 10, 1, 3, 5]);
      expect(s.describe()).toMatchObject({ epoch: 2, manifestSeq: 5, seq: 9, durableSeq: 9, snapshotSeq: 7, walObjects: 1, log: { events: { memoryRows: 5, segments: 1, segmentRows: 2 } } });
      // and it keeps working: commit, snapshot, reopen
      await s.commit([{ t: "put", table: "meta", key: "cursor", value: 3 }], { durable: "s3" });
      await s.snapshot();
    } finally {
      await s.close();
    }
    const again = await Store.open(opts(objects, await tmp()));
    expect([again.stateTable("meta").get("cursor"), again.stateTable("players").size, again.seq]).toEqual([3, 3, 10]);
    await again.close();
  });
});
