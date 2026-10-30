/**
 * The public surface: what `import ... from "hopstore"` offers. Table views are read-only types (writes go through
 * `Store.commit`); the `@ts-expect-error` lines are checked by `tsc` (pnpm typecheck), the rest at runtime.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import * as hop from "./index.js";
import { MemoryObjectStore, Store, type LogTable, type RankedIndex, type StateTable } from "./index.js";

describe("public API", () => {
  it("exports exactly these values", () => {
    expect(Object.keys(hop).sort()).toEqual(["FencedError", "MemoryObjectStore", "PreconditionFailed", "S3Error", "S3ObjectStore", "Store", "compareSort"]);
  });

  it("table views and the store expose no mutators or internals through their types", async () => {
    const dir = await mkdtemp(join(tmpdir(), "hopstore-api-"));
    const s = await Store.open({
      objects: new MemoryObjectStore(),
      prefix: "api/",
      localDir: dir,
      tables: { state: ["t"], ranked: [{ name: "r", table: "t", group: () => "g", member: (k) => k, score: () => 1n }], log: [{ name: "l", keepPerPartition: 1, sortKey: (r) => [r as number] }] },
    });
    try {
      const t: StateTable = s.stateTable("t");
      const r: RankedIndex = s.ranked("r");
      const l: LogTable = s.logTable("l");
      const blocked = () => {
        // @ts-expect-error not part of the public type
        t.apply({ t: "put", table: "t", key: "k", value: 1 });
        // @ts-expect-error not part of the public type
        t.load("k", 1);
        // @ts-expect-error not part of the public type
        t.freeze();
        // @ts-expect-error not part of the public type
        t.thaw();
        // @ts-expect-error not part of the public type
        r.apply("g", "m", 1n);
        // @ts-expect-error not part of the public type
        r.load("g", []);
        // @ts-expect-error not part of the public type
        l.apply({ t: "append", table: "l", partition: "p", row: 1 });
        // @ts-expect-error not part of the public type
        l.insert({ p: "p", s: [1], row: 1 });
        // @ts-expect-error not part of the public type
        l.sortKeyOf(1);
        // @ts-expect-error not part of the public type
        l.unsealed();
        // @ts-expect-error not part of the public type
        l.sealable();
        // @ts-expect-error not part of the public type
        l.dropSealed([]);
        // @ts-expect-error not part of the public type
        void l.parts;
        // @ts-expect-error not part of the public type
        void l.source;
        // @ts-expect-error not part of the public type
        void s.settled();
        // @ts-expect-error not part of the public type
        void s.segmentStats;
      };
      void blocked;
      expect([t.size, r.count("g"), l.size]).toEqual([0, 0, 0]);
    } finally {
      await s.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
});
