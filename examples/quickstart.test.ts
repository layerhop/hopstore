import { expect, it } from "vitest";
import { main } from "./quickstart.js";

// keeps the README's quick start honest: it must run and recover the leaderboard
it("examples/quickstart.ts runs and recovers from the object store", async () => {
  const lines: string[] = [];
  const top = await main((l) => lines.push(l));
  expect(top).toEqual([
    { member: "bob", score: 130n },
    { member: "ada", score: 120n },
    { member: "cy", score: 95n },
  ]);
  expect(lines.some((l) => l.startsWith("after reopen (epoch 2): 4 players"))).toBe(true);
});
