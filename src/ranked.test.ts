import { describe, expect, it } from "vitest";
import { RankedIndexImpl as RankedIndex } from "./ranked.js";

describe("RankedIndex", () => {
  it("top returns highest first with member tiebreak", () => {
    const r = new RankedIndex();
    r.apply("g", "b", 5n);
    r.apply("g", "a", 5n);
    r.apply("g", "c", 9n);
    r.apply("g", "d", 1n);
    expect(r.top("g", 3)).toEqual([{ member: "c", score: 9n }, { member: "a", score: 5n }, { member: "b", score: 5n }]);
    expect(r.top("g", 0)).toEqual([]);
    expect(r.top("none", 5)).toEqual([]);
  });

  it("member dropping to zero leaves count and top", () => {
    const r = new RankedIndex();
    r.apply("g", "m", 5n);
    expect(r.count("g")).toBe(1);
    r.apply("g", "m", 0n);
    expect(r.count("g")).toBe(0);
    expect(r.top("g", 5)).toEqual([]);
    r.apply("g", "m", 3n);
    expect(r.count("g")).toBe(1);
    expect(r.top("g", 5)).toEqual([{ member: "m", score: 3n }]);
  });

  it("negative score is excluded", () => {
    const r = new RankedIndex();
    r.apply("g", "m", -4n);
    r.apply("g", "n", 2n);
    expect(r.count("g")).toBe(1);
    expect(r.sum("g")).toBe(2n);
    r.apply("g", "n", -1n);
    expect(r.count("g")).toBe(0);
    expect(r.sum("g")).toBe(0n);
    r.apply("g", "n", null);
    expect([...r.groups()]).toEqual(["g"]);
  });

  it("sum tracks updates", () => {
    const r = new RankedIndex();
    r.apply("g", "a", 10n);
    r.apply("g", "b", 20n);
    expect(r.sum("g")).toBe(30n);
    r.apply("g", "a", 15n);
    expect(r.sum("g")).toBe(35n);
    r.apply("g", "b", null);
    expect(r.sum("g")).toBe(15n);
    expect(r.sum("other")).toBe(0n);
  });

  it("minScore 0n ranks zero scores, null ranks every score", () => {
    const zero = new RankedIndex(0n);
    zero.apply("g", "z", 0n);
    zero.apply("g", "n", -1n);
    zero.load("h", [{ member: "a", score: 0n }, { member: "b", score: -2n }]);
    expect([zero.top("g", 5), zero.count("h")]).toEqual([[{ member: "z", score: 0n }], 1]);
    const all = new RankedIndex(null);
    all.apply("g", "n", -5n);
    all.apply("g", "z", 0n);
    all.apply("g", "p", 3n);
    all.load("h", [{ member: "a", score: -1n }]);
    expect(all.top("g", 5)).toEqual([{ member: "p", score: 3n }, { member: "z", score: 0n }, { member: "n", score: -5n }]);
    expect([all.count("g"), all.sum("g"), all.count("h"), all.sum("h")]).toEqual([3, -2n, 1, -1n]);
    all.apply("g", "n", null);
    expect(all.count("g")).toBe(2);
  });

  it.each([[1n], [0n], [-5n], [null]])("10k members, 50k random updates match a brute-force model (minScore %s)", (minScore) => {
    let s = 12345;
    const rnd = (n: number) => {
      s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
      return s % n;
    };
    const r = new RankedIndex(minScore);
    const model = new Map<string, Map<string, bigint>>();
    for (let i = 0; i < 50_000; i++) {
      const g = `g${rnd(3)}`;
      const m = `m${rnd(10_000)}`;
      const score = rnd(5) === 0 ? null : BigInt(rnd(200) - 20);
      r.apply(g, m, score);
      const mm = model.get(g) ?? new Map<string, bigint>();
      model.set(g, mm);
      if (score === null) mm.delete(m);
      else mm.set(m, score);
    }
    for (const [g, mm] of model) {
      const pos = [...mm].filter(([, v]) => minScore === null || v >= minScore).map(([member, score]) => ({ member, score }));
      pos.sort((a, b) => (a.score === b.score ? (a.member < b.member ? -1 : 1) : a.score > b.score ? -1 : 1));
      expect(r.count(g)).toBe(pos.length);
      expect(r.sum(g)).toBe(pos.reduce((n, p) => n + p.score, 0n));
      expect(r.top(g, 50)).toEqual(pos.slice(0, 50));
      expect(r.top(g, 100_000)).toEqual(pos);
    }
  });
});
