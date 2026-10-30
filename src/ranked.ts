/** Ranked index: per group, members ordered by bigint score (top-N, count and sum over the ranked scores). */
interface Entry {
  score: bigint;
  member: string;
}

/** Order: score desc, then member asc. */
const cmp = (a: Entry, b: Entry) => (a.score === b.score ? (a.member < b.member ? -1 : a.member > b.member ? 1 : 0) : a.score > b.score ? -1 : 1);

interface Group {
  /** ranked scores only, sorted by `cmp` */
  sorted: Entry[];
  /** every member with a ranked score */
  scores: Map<string, bigint>;
  sum: bigint;
}

/** Read-only view of a ranked index (what `Store.ranked` returns), maintained by the store on every commit. */
export interface RankedIndex {
  /** Highest scores first, ties by member ascending. */
  top(group: string, n: number): { member: string; score: bigint }[];
  /** Members with a ranked score (at least `minScore`). */
  count(group: string): number;
  /** Sum of the ranked scores. */
  sum(group: string): bigint;
  groups(): IterableIterator<string>;
}

/** The index itself, internal to the package: the store maintains it through `apply` and `load`. */
export class RankedIndexImpl implements RankedIndex {
  private readonly g = new Map<string, Group>();

  /** @param minScore scores below it are not ranked (default 1n: only positive scores); null ranks every score */
  constructor(private readonly minScore: bigint | null = 1n) {}

  private ranks(score: bigint): boolean {
    return this.minScore === null || score >= this.minScore;
  }

  /** Sets a member's score; `null` removes it. Scores below `minScore` are not ranked. */
  apply(group: string, member: string, score: bigint | null): void {
    let grp = this.g.get(group);
    if (!grp) {
      if (score === null || !this.ranks(score)) return;
      grp = { sorted: [], scores: new Map(), sum: 0n };
      this.g.set(group, grp);
    }
    const old = grp.scores.get(member);
    if (old !== undefined) {
      grp.sorted.splice(this.find(grp.sorted, { score: old, member }), 1);
      grp.scores.delete(member);
      grp.sum -= old;
    }
    if (score !== null && this.ranks(score)) {
      const e = { score, member };
      grp.sorted.splice(this.find(grp.sorted, e), 0, e);
      grp.scores.set(member, score);
      grp.sum += score;
    }
  }

  /** Bulk-fills a group (sorts once). Scores below `minScore` are skipped; members must be unique and the group not yet present. */
  load(group: string, entries: { member: string; score: bigint }[]): void {
    const sorted = entries.filter((e) => this.ranks(e.score)).sort(cmp);
    if (!sorted.length) return;
    this.g.set(group, { sorted, scores: new Map(sorted.map((e) => [e.member, e.score])), sum: sorted.reduce((n, e) => n + e.score, 0n) });
  }

  /** Index of the first entry not ordered before `e`. */
  private find(a: Entry[], e: Entry): number {
    let lo = 0;
    let hi = a.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (cmp(a[mid]!, e) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /** Highest scores first, ties by member ascending. */
  top(group: string, n: number): { member: string; score: bigint }[] {
    return (this.g.get(group)?.sorted.slice(0, Math.max(0, n)) ?? []).map((e) => ({ member: e.member, score: e.score }));
  }

  count(group: string): number {
    return this.g.get(group)?.sorted.length ?? 0;
  }

  sum(group: string): bigint {
    return this.g.get(group)?.sum ?? 0n;
  }

  groups(): IterableIterator<string> {
    return this.g.keys();
  }
}
