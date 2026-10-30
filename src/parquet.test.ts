import { parquetMetadata, parquetReadObjects } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it } from "vitest";
import { ROW_GROUP_SIZE, writeParquetRows } from "./parquet.js";
import { readStateSnapshot, writeStateSnapshot } from "./state-table.js";

describe("parquet snapshots", () => {
  it("snapshot parquet round-trips 100k entries", async () => {
    const rows: [string, { amount: string; n: number }][] = Array.from({ length: 100_000 }, (_, i) => [`k${String(i).padStart(6, "0")}`, { amount: String(BigInt(i) * 10n ** 18n), n: i }]);
    const buf = await writeStateSnapshot("balances", rows);
    expect(await readStateSnapshot(buf)).toEqual({ table: "balances", rows });

    // plain hyparquet sees the documented layout: key, value (JSON text), row groups of 10,000, hopstore metadata
    const file = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) as ArrayBuffer;
    const meta = parquetMetadata(file);
    expect(meta.row_groups.length).toBe(10);
    expect(Object.fromEntries((meta.key_value_metadata ?? []).map((kv) => [kv.key, kv.value]))).toMatchObject({ "hopstore.table": "balances", "hopstore.version": "1" });
    const plain = await parquetReadObjects({ file, rowStart: 99_999, rowEnd: 100_000 });
    expect(plain).toEqual([{ key: "k099999", value: JSON.stringify(rows[99_999]![1]) }]);
  });

  it("writeParquetRows: the same bytes as one parquetWriteBuffer call, yielding to the event loop between row groups", async () => {
    const n = ROW_GROUP_SIZE * 2 + 345;
    const keys = Array.from({ length: n }, (_, i) => `k${i}`);
    const vals = keys.map((k, i) => JSON.stringify({ k, i }));
    const kv = { "hopstore.table": "t", "hopstore.version": "1" };
    const expected = new Uint8Array(
      parquetWriteBuffer({
        columnData: [
          { name: "key", data: keys, type: "STRING", nullable: false },
          { name: "value", data: vals, type: "STRING", nullable: false, statistics: false },
        ],
        kvMetadata: Object.entries(kv).map(([key, value]) => ({ key, value })),
        rowGroupSize: ROW_GROUP_SIZE,
      }),
    );
    let ticks = 0;
    let spinning = true;
    const spin = () => spinning && (ticks++, setImmediate(spin));
    setImmediate(spin);
    const write = (hint?: number) =>
      writeParquetRows(
        [
          { name: "key", type: "STRING", nullable: false },
          { name: "value", type: "STRING", nullable: false, statistics: false },
        ],
        keys.map((k, i) => [k, vals[i]]),
        () => ({ "hopstore.table": "t" }),
        hint,
      );
    const got = await write(n);
    spinning = false;
    expect(got).toEqual(expected);
    expect(ticks).toBeGreaterThanOrEqual(2);
    // a missing, low or high row-count hint only changes how the buffer grows
    for (const hint of [undefined, ROW_GROUP_SIZE + 1, n * 10]) expect(await write(hint)).toEqual(expected);
    const empty = await writeParquetRows([{ name: "key", type: "STRING", nullable: false }], [], () => ({}));
    expect(parquetMetadata(empty.slice().buffer).num_rows).toBe(0n);
  });
});
