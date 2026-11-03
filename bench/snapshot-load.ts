/**
 * Peak memory of loading a large state snapshot (a balances table) the way a store opens it: whole file decoded at once
 * (`whole`, the old path) vs one row group at a time (`groups`, readStateSnapshotGroups). Each mode runs in its own
 * process; the peak is the process's max RSS, the table size its heap after GC.
 *
 *   tsx bench/snapshot-load.ts [rows=1000000]   (children reuse the TypeScript loader tsx started it with)
 */
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Json } from "../src/codec.js";
import { bytesBuffer, readParquet } from "../src/parquet.js";
import { readStateSnapshotGroups, writeStateSnapshot } from "../src/state-table.js";

const self = fileURLToPath(import.meta.url);
const mb = (b: number) => Math.round(b / 1_048_576);
const [mode, arg] = process.argv.slice(2);

if (mode === "whole" || mode === "groups") {
  const gc = (globalThis as { gc?: () => void }).gc!;
  const buf = new Uint8Array(await readFile(arg!));
  gc();
  const before = process.memoryUsage();
  const table = new Map<string, Json>();
  if (mode === "whole") {
    const { rows } = await readParquet(buf);
    for (const r of rows.map((x): [string, Json] => [String(x.key), JSON.parse(String(x.value)) as Json])) table.set(r[0], r[1]);
  } else {
    const { groups } = await readStateSnapshotGroups(bytesBuffer(buf));
    for await (const g of groups) for (const [k, v] of g) table.set(k, v);
  }
  gc();
  const after = process.memoryUsage();
  // ru_maxrss: kilobytes (libuv normalises macOS's bytes)
  const peak = process.resourceUsage().maxRSS * 1024;
  console.log(JSON.stringify({ mode, rows: table.size, fileMb: mb(buf.byteLength), rssBeforeMb: mb(before.rss), peakRssMb: mb(peak), peakOverBaseMb: mb(peak - before.rss), tableHeapMb: mb(after.heapUsed - before.heapUsed) }));
} else {
  const rows = Number(mode ?? 1_000_000);
  const dir = await mkdtemp(join(tmpdir(), "hop-snap-bench-"));
  const file = join(dir, "balances.parquet");
  const token = `0x${"7".repeat(40)}`;
  const gen = function* () {
    for (let i = 0; i < rows; i++) yield [`${token}|0x${i.toString(16).padStart(40, "0")}`, String(BigInt(1 + ((i * 7919) % 1_000_003)) * 10n ** 18n)] as [string, Json];
  };
  await writeFile(file, await writeStateSnapshot("balances", gen(), rows));
  for (const m of ["whole", "groups"]) process.stdout.write(execFileSync(process.execPath, [...process.execArgv, "--expose-gc", self, m, file], { encoding: "utf8" }));
  await rm(dir, { recursive: true, force: true });
}
