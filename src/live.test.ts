import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { Json } from "./codec.js";
import { S3ObjectStore } from "./s3.js";
import { Store, type StoreOptions } from "./store.js";

/** Runs against the real bucket: HOPSTORE_LIVE_S3=1 plus S3_ENDPOINT, S3_REGION, S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY. */
const live = process.env.HOPSTORE_LIVE_S3 === "1";
type Row = { p: number; n: number };

describe.skipIf(!live)("live S3 smoke test", () => {
  it("state + log table round trip on the real bucket", { timeout: 300_000 }, async () => {
    const need = (k: string) => process.env[k] ?? (() => { throw new Error(`missing ${k}`); })();
    const objects = new S3ObjectStore({ endpoint: need("S3_ENDPOINT"), region: need("S3_REGION"), bucket: need("S3_BUCKET"), accessKeyId: need("S3_ACCESS_KEY_ID"), secretAccessKey: need("S3_SECRET_ACCESS_KEY") });
    const prefix = `hopstore-test/${randomUUID()}/`;
    const dirs: string[] = [];
    const tmp = async () => (dirs.push(await mkdtemp(join(tmpdir(), "hopstore-live-"))), dirs[dirs.length - 1]!);
    const opts = (localDir: string): StoreOptions => ({
      objects,
      prefix,
      localDir,
      flushMs: 200,
      snapshotEveryMs: 3_600_000,
      tables: { state: ["kv"], log: [{ name: "events", keepPerPartition: 10, sortKey: (r: Json) => [(r as Row).n] }] },
    });
    const timings: Record<string, number> = {};
    const time = async <T>(name: string, f: () => Promise<T>) => {
      const t = performance.now();
      try {
        return await f();
      } finally {
        timings[name] = Math.round(performance.now() - t);
      }
    };
    const opened: Store[] = [];
    let leftover = -1;
    try {
      const s1 = await time("open", async () => Store.open(opts(await tmp())));
      opened.push(s1);
      const ops = Array.from({ length: 1000 }, (_, i) => ({ t: "put" as const, table: "kv", key: `k${String(i).padStart(4, "0")}`, value: { i } }));
      await time("commit1000Durable", () => s1.commit(ops, { durable: "s3" }));
      const logOps = Array.from({ length: 1000 }, (_, i) => ({ t: "append" as const, table: "events", partition: `P${i % 5}`, row: { p: i % 5, n: i } }));
      await s1.commit(logOps, { durable: "s3" });
      const sealed = await time("seal", () => s1.sealLogs());
      expect(sealed).toBeGreaterThan(0);
      await s1.close();

      const s2 = await time("reopenFresh", async () => {
        const s = await Store.open(opts(await tmp()));
        opened.push(s);
        return s;
      });
      const kv = s2.stateTable<{ i: number }>("kv");
      expect(kv.size).toBe(1000);
      for (let i = 0; i < 1000; i += 97) expect(kv.get(`k${String(i).padStart(4, "0")}`)).toEqual({ i });
      const rows: Row[] = [];
      await time("scanPartition", async () => {
        for await (const r of s2.logTable<Row>("events").scan({ partition: "P3", from: [0], to: [10_000] })) rows.push(r);
      });
      expect(rows.map((r) => r.n)).toEqual(Array.from({ length: 200 }, (_, i) => i * 5 + 3));
      expect(s2.seq).toBe(2);
      await s2.close();
    } finally {
      for (const s of opened) await s.close().catch(() => {});
      const t = performance.now();
      for (const o of await objects.list(prefix)) await objects.delete(o.key);
      timings.cleanup = Math.round(performance.now() - t);
      leftover = (await objects.list(prefix)).length;
      for (const d of dirs) await rm(d, { recursive: true, force: true });
      console.log(`hopstore live timings (ms): ${JSON.stringify(timings)}; objects left under ${prefix}: ${leftover}`);
    }
    expect(leftover).toBe(0);
  });
});
