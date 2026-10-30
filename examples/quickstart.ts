/**
 * Quick start: a leaderboard on an in-memory object store, so it runs without credentials.
 *
 *   npx tsx examples/quickstart.ts
 *
 * A state table holds one row per player, a ranked index keeps them ordered by points, and a log table keeps the
 * recent games per player. The store is closed (final flush and snapshot) and reopened from an empty local
 * directory, so everything it reads afterwards comes back from the object store alone.
 * In your own project, import from "hopstore" instead of "../src/index.js".
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { MemoryObjectStore, Store, type Json, type ObjectStore, type StoreOptions } from "../src/index.js";

type Player = { name: string; points: number };
type Game = { at: number; delta: number };

/** Table declarations: the same on every open. Ranked scores are bigints; JSON values never hold one. */
const options = (objects: ObjectStore, localDir: string): StoreOptions => ({
  objects,
  prefix: "examples/leaderboard/",
  localDir,
  tables: {
    state: ["players"],
    ranked: [{ name: "by_points", table: "players", group: () => "all", member: (key) => key, score: (v) => BigInt((v as Player).points) }],
    log: [{ name: "games", keepPerPartition: 100, sortKey: (row: Json) => [(row as Game).at] }],
  },
});

/** Runs the example and returns the top three after the reopen (the test checks it). */
export async function main(log: (line: string) => void = console.log): Promise<{ member: string; score: bigint }[]> {
  const objects = new MemoryObjectStore(); // swap in S3ObjectStore for real storage (see examples/s3.ts)
  const dirs = [await mkdtemp(join(tmpdir(), "hopstore-quickstart-")), await mkdtemp(join(tmpdir(), "hopstore-quickstart-"))];
  try {
    const store = await Store.open(options(objects, dirs[0]!));
    const players = store.stateTable<Player>("players");
    const games = store.logTable<Game>("games");

    // One atomic batch: every op applies or none does. The default resolves once it is fsynced to the local WAL.
    await store.commit([
      { t: "put", table: "players", key: "ada", value: { name: "Ada", points: 120 } },
      { t: "put", table: "players", key: "bob", value: { name: "Bob", points: 80 } },
      { t: "put", table: "players", key: "cy", value: { name: "Cy", points: 95 } },
      { t: "put", table: "players", key: "dee", value: { name: "Dee", points: 60 } },
    ]);

    // Read-modify-write: reads are synchronous and come from memory. `durable: "s3"` waits until S3 has the batch.
    const bob = players.get("bob")!;
    await store.commit(
      [
        { t: "put", table: "players", key: "bob", value: { ...bob, points: bob.points + 50 } },
        { t: "append", table: "games", partition: "bob", row: { at: 1, delta: 50 } },
      ],
      { durable: "s3" },
    );
    log(`seq ${store.seq}, durable on S3 up to ${store.durableSeq()}`);
    log(`top 3: ${fmt(store.ranked("by_points").top("all", 3))}`);
    log(`bob's games: ${JSON.stringify(games.latest("bob", 10))}`);

    // close() flushes and writes a Parquet snapshot of every table.
    await store.close();

    // Reopen with an empty local directory: state comes back from the object store only.
    const again = await Store.open(options(objects, dirs[1]!));
    const top = again.ranked("by_points").top("all", 3);
    log(`after reopen (epoch ${again.epoch}): ${again.stateTable("players").size} players, top 3: ${fmt(top)}`);
    log(`objects in the store: ${[...objects.objects.keys()].filter((k) => !k.includes("/manifest/")).join(", ")}`);
    await again.close();
    return top;
  } finally {
    for (const d of dirs) await rm(d, { recursive: true, force: true });
  }
}

const fmt = (top: { member: string; score: bigint }[]) => top.map((e) => `${e.member}=${e.score}`).join(" ");

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
