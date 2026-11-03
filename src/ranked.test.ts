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
    expect([...r.groups()]).toEqual([]); // a group without members is dropped
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

  it("applyMany equals apply for each change in turn (small and large sets, removals, unranked scores, new groups, block splits)", () => {
    let seed = 7;
    const rnd = (n: number) => ((seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648), seed % n);
    const a = new RankedIndex();
    const b = new RankedIndex();
    for (let round = 0; round < 40; round++) {
      const group = `g${rnd(3)}`;
      const changes = new Map<string, bigint | null>();
      const k = round % 2 ? 1 + rnd(10) : 20 + rnd(400);
      for (let i = 0; i < k; i++) {
        const r = rnd(10);
        changes.set(`m${rnd(500)}`, r === 0 ? null : r === 1 ? 0n : BigInt(rnd(50)));
      }
      for (const [m, s] of changes) a.apply(group, m, s);
      b.applyMany(group, changes);
      for (const g of ["g0", "g1", "g2"]) {
        expect(b.top(g, 1_000)).toEqual(a.top(g, 1_000));
        expect(b.count(g)).toBe(a.count(g));
        expect(b.sum(g)).toBe(a.sum(g));
      }
    }
  });

  it("10,000 changes on 500,000 members take well under a second (blocked order: no group-sized splice)", () => {
    const r = new RankedIndex();
    const m = (i: number) => `0x${i.toString(16).padStart(40, "0")}`;
    r.load("g", Array.from({ length: 500_000 }, (_, i) => ({ member: m(i), score: BigInt(1 + ((i * 7919) % 1_000_003)) })));
    const changes = new Map<string, bigint | null>();
    for (let k = 0; k < 10_000; k++) changes.set(m((k * 97) % 500_000), k % 3 ? BigInt(5 + k) : null);
    const t = performance.now();
    r.applyMany("g", changes);
    expect(performance.now() - t).toBeLessThan(1_000);
    expect(r.count("g")).toBe(500_000 - Math.ceil(10_000 / 3));
  });

  it("blocks split and empty out correctly: random changes on 6,000 members match a plain sorted list", () => {
    let seed = 3;
    const rnd = (n: number) => ((seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648), seed % n);
    const r = new RankedIndex();
    const ref = new Map<string, bigint>();
    const check = () => {
      const want = [...ref].map(([member, score]) => ({ member, score })).sort((a, b) => (a.score === b.score ? (a.member < b.member ? -1 : 1) : a.score > b.score ? -1 : 1));
      expect(r.top("g", 100_000)).toEqual(want);
      expect(r.count("g")).toBe(want.length);
      expect(r.sum("g")).toBe(want.reduce((n, e) => n + e.score, 0n));
    };
    r.load("g", Array.from({ length: 3_000 }, (_, i) => ({ member: `m${i}`, score: BigInt(1 + (i % 97)) })));
    for (let i = 0; i < 3_000; i++) ref.set(`m${i}`, BigInt(1 + (i % 97)));
    for (let round = 0; round < 6; round++) {
      for (let i = 0; i < 4_000; i++) {
        const m = `m${rnd(6_000)}`;
        // rounds 3-5 mostly remove: blocks empty out and are dropped
        const s = rnd(round >= 3 ? 3 : 8) === 0 ? null : BigInt(rnd(60) - 3);
        r.apply("g", m, s);
        if (s === null || s < 1n) ref.delete(m);
        else ref.set(m, s);
      }
      check();
    }
    // all at one score: ties ordered by member across blocks
    for (let i = 0; i < 2_500; i++) {
      r.apply("g", `t${i}`, 1_000n);
      ref.set(`t${i}`, 1_000n);
    }
    check();
  });

  it("top takes a whole count: fractions count down, NaN and negatives are 0, Infinity is everything", () => {
    const r = new RankedIndex();
    for (const m of ["a", "b", "c"]) r.apply("g", m, 5n);
    expect(r.top("g", Number.NaN)).toEqual([]);
    expect(r.top("g", 0.5)).toEqual([]);
    expect(r.top("g", 2.9).map((e) => e.member)).toEqual(["a", "b"]);
    expect(r.top("g", -1)).toEqual([]);
    expect(r.top("g", Number.POSITIVE_INFINITY)).toHaveLength(3);
  });

  it("heavy deletes merge small blocks instead of leaving a trail of tiny ones, and an emptied group is dropped", () => {
    const r = new RankedIndex();
    r.load("g", Array.from({ length: 100_000 }, (_, i) => ({ member: `m${String(i).padStart(6, "0")}`, score: BigInt(1 + (i % 1_000)) })));
    expect(r.blockCount("g")).toBe(Math.ceil(100_000 / 512));
    // keep one member in 100: every block would shrink to ~5 entries without merging
    for (let i = 0; i < 100_000; i++) if (i % 100) r.apply("g", `m${String(i).padStart(6, "0")}`, null);
    expect(r.count("g")).toBe(1_000);
    expect(r.blockCount("g")).toBeLessThanOrEqual(Math.ceil(1_000 / 128) + 1);
    expect(r.top("g", 3).map((e) => [e.member, e.score])).toEqual([["m000900", 901n], ["m001900", 901n], ["m002900", 901n]]); // kept: i % 100 == 0, score 1 + i % 1000
    for (let i = 0; i < 100_000; i += 100) r.apply("g", `m${String(i).padStart(6, "0")}`, 0n);
    expect([r.count("g"), r.blockCount("g"), [...r.groups()]]).toEqual([0, 0, []]);
  });

  it("random multi-group batches (applyMany), minScore null, top-N prefixes: match a plain sorted list", () => {
    let seed = 99;
    const rnd = (n: number) => ((seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648), seed % n);
    for (const minScore of [1n, null] as const) {
      const r = new RankedIndex(minScore);
      const ref = new Map<string, Map<string, bigint>>();
      for (let round = 0; round < 30; round++) {
        const group = `g${rnd(4)}`;
        const changes = new Map<string, bigint | null>();
        for (let i = 0, k = 1 + rnd(round % 3 ? 50 : 3_000); i < k; i++) changes.set(`m${rnd(4_000)}`, rnd(7) === 0 ? null : BigInt(rnd(80) - 20));
        r.applyMany(group, changes);
        const m = ref.get(group) ?? new Map<string, bigint>();
        ref.set(group, m);
        for (const [mem, sc] of changes) {
          if (sc === null || (minScore !== null && sc < minScore)) m.delete(mem);
          else m.set(mem, sc);
        }
        for (const [g, mm] of ref) {
          const want = [...mm].map(([member, score]) => ({ member, score })).sort((a, b) => (a.score === b.score ? (a.member < b.member ? -1 : 1) : a.score > b.score ? -1 : 1));
          for (const n of [0, 1, 7, 600, 100_000]) expect(r.top(g, n)).toEqual(want.slice(0, n));
          expect([r.count(g), r.sum(g)]).toEqual([want.length, want.reduce((s, e) => s + e.score, 0n)]);
        }
      }
      expect(new Set(r.groups())).toEqual(new Set([...ref].filter(([, mm]) => mm.size).map(([g]) => g)));
    }
  });
});
