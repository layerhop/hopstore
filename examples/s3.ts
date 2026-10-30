/**
 * The quick start on real S3-compatible storage. Reads its configuration from the environment:
 *
 *   S3_ENDPOINT=https://eu2.contabostorage.com S3_REGION=default S3_BUCKET=my-bucket \
 *   S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... npx tsx examples/s3.ts
 *
 * Every run adds a game for one player and prints the leaderboard, so a second run (even from another machine, or
 * after deleting HOPSTORE_DIR) shows what the first one stored. Objects go under `hopstore-examples/leaderboard/`;
 * delete that prefix when you are done. Not run by the test suite.
 * In your own project, import from "hopstore" instead of "../src/index.js".
 */
import { join } from "node:path";
import { tmpdir } from "node:os";
import { S3ObjectStore, Store, type Json } from "../src/index.js";

type Player = { name: string; points: number };
type Game = { at: number; delta: number };

const need = (k: string): string => {
  const v = process.env[k];
  if (!v) throw new Error(`set ${k}`);
  return v;
};

const objects = new S3ObjectStore({
  endpoint: need("S3_ENDPOINT"),
  region: need("S3_REGION"),
  bucket: need("S3_BUCKET"),
  accessKeyId: need("S3_ACCESS_KEY_ID"),
  secretAccessKey: need("S3_SECRET_ACCESS_KEY"),
});

const store = await Store.open({
  objects,
  prefix: "hopstore-examples/leaderboard/",
  // the local WAL and snapshot cache; on a server, a persistent volume
  localDir: process.env.HOPSTORE_DIR ?? join(tmpdir(), "hopstore-example-s3"),
  tables: {
    state: ["players"],
    ranked: [{ name: "by_points", table: "players", group: () => "all", member: (key) => key, score: (v) => BigInt((v as Player).points) }],
    log: [{ name: "games", keepPerPartition: 100, sortKey: (row: Json) => [(row as Game).at] }],
  },
});

const players = store.stateTable<Player>("players");
console.log(`opened epoch ${store.epoch} at batch ${store.seq}: ${players.size} players`);

const names = ["ada", "bob", "cy", "dee"];
const key = names[Math.floor(Math.random() * names.length)]!;
const cur = players.get(key) ?? { name: key, points: 0 };
const delta = 10 + Math.floor(Math.random() * 40);
await store.commit(
  [
    { t: "put", table: "players", key, value: { ...cur, points: cur.points + delta } },
    { t: "append", table: "games", partition: key, row: { at: Date.now(), delta } },
  ],
  { durable: "s3" },
);
console.log(`${key} +${delta}; top 3: ${store.ranked("by_points").top("all", 3).map((e) => `${e.member}=${e.score}`).join(" ")}`);
console.log(`${key}'s last games: ${JSON.stringify(store.logTable<Game>("games").latest(key, 5))}`);

await store.close();
