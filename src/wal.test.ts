import { mkdtemp, readFile, rm, truncate } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { decodeRecords, encodeRecords, type Batch } from "./codec.js";
import { LocalWal, readLocalWals } from "./wal.js";

const dirs: string[] = [];
const tmp = async () => {
  const d = await mkdtemp(join(tmpdir(), "hopstore-wal-"));
  dirs.push(d);
  return d;
};
afterEach(async () => {
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

const b = (seq: number): Batch => ({ seq, ops: [{ t: "put", table: "t", key: `k${seq}`, value: { n: seq, s: "é" } }] });

describe("codec", () => {
  it("round-trips length-prefixed records", () => {
    const recs = [b(1), b(2), b(3)];
    expect(decodeRecords(encodeRecords(recs)).records).toEqual(recs);
  });

  it("stops at a torn tail and reports the valid length", () => {
    const whole = encodeRecords([b(1), b(2)]);
    const torn = whole.subarray(0, whole.byteLength - 3);
    const { records, validBytes } = decodeRecords(torn);
    expect(records).toEqual([b(1)]);
    expect(validBytes).toBe(encodeRecords([b(1)]).byteLength);
  });
});

describe("local wal", () => {
  it("appends with fsync and reads back header plus batches", async () => {
    const dir = await tmp();
    const wal = await LocalWal.create(dir, { epoch: 7, writer: "w1" }, [b(1)]);
    await wal.append([b(2), b(3)]);
    await wal.close();
    const files = await readLocalWals(dir);
    expect(files).toEqual([{ file: expect.stringContaining("00000000000000000007"), header: { epoch: 7, writer: "w1" }, batches: [b(1), b(2), b(3)] }]);
  });

  it("ignores a torn last record", async () => {
    const dir = await tmp();
    const wal = await LocalWal.create(dir, { epoch: 1, writer: "w" }, []);
    await wal.append([b(1)]);
    await wal.append([b(2)]);
    await wal.close();
    const file = join(dir, "wal-00000000000000000001.log");
    await truncate(file, (await readFile(file)).byteLength - 2);
    expect((await readLocalWals(dir))[0]?.batches).toEqual([b(1)]);
  });

  it("rewrite keeps only the given batches and keeps appending", async () => {
    const dir = await tmp();
    const wal = await LocalWal.create(dir, { epoch: 2, writer: "w" }, [b(1), b(2)]);
    await wal.rewrite([b(2)]);
    await wal.append([b(3)]);
    await wal.close();
    expect((await readLocalWals(dir))[0]?.batches).toEqual([b(2), b(3)]);
  });
});
