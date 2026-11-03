/**
 * Ranked index: per group, members ordered by bigint score (top-N, count and sum over the ranked scores).
 *
 * A group's order is a list of sorted blocks (at most 2 x BLOCK entries each) rather than one sorted array: a change
 * costs two binary searches and a splice inside one small block, O(log n + BLOCK), whatever the group size. Measured on
 * 2,000,000 members: a few microseconds a change, where one big array spent ~1 ms a splice (and a commit of 10,000
 * holder balances blocked the event loop for seconds).
 */
interface Entry {
  score: bigint;
  member: string;
}

/**
 * Target entries per block: a block splits past 2 x BLOCK, and one that falls under BLOCK / 4 merges with a neighbour
 * (split again in halves if that makes it too big), so heavy deletes never leave a trail of tiny blocks. A group whose
 * last member goes is dropped (it no longer appears in `groups()`).
 */
export const BLOCK = 512;

/** Order: score desc, then member asc. */
const cmp = (a: Entry, b: Entry) => (a.score === b.score ? (a.member < b.member ? -1 : a.member > b.member ? 1 : 0) : a.score > b.score ? -1 : 1);

interface Group {
  /** ranked scores only: sorted by `cmp` within and across blocks, no block empty */
  blocks: Entry[][];
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

/** Index of the first entry of `a` not ordered before `e`. */
function lowerBound(a: Entry[], e: Entry): number {
  let lo = 0;
  let hi = a.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cmp(a[mid]!, e) < 0) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** The block `e` belongs in: the first whose last entry is not ordered before it (the last block when none). */
function blockOf(blocks: Entry[][], e: Entry): number {
  let lo = 0;
  let hi = blocks.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    const b = blocks[mid]!;
    if (cmp(b[b.length - 1]!, e) < 0) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** The index itself, internal to the package: the store maintains it through `apply` and `load`. */
export class RankedIndexImpl implements RankedIndex {
  private readonly g = new Map<string, Group>();

  /** @param minScore scores below it are not ranked (default 1n: only positive scores); null ranks every score */
  constructor(private readonly minScore: bigint | null = 1n) {}

  private ranks(score: bigint): boolean {
    return this.minScore === null || score >= this.minScore;
  }

  private remove(grp: Group, e: Entry): void {
    const bi = blockOf(grp.blocks, e);
    const b = grp.blocks[bi]!;
    b.splice(lowerBound(b, e), 1);
    if (!b.length) grp.blocks.splice(bi, 1);
    else if (b.length < BLOCK / 4 && grp.blocks.length > 1) {
      const at = bi > 0 ? bi - 1 : bi; // merge with the previous block (the next one for the first)
      const merged = grp.blocks[at]!.concat(grp.blocks[at + 1]!);
      grp.blocks.splice(at, 2, ...(merged.length > 2 * BLOCK ? [merged.slice(0, merged.length >> 1), merged.slice(merged.length >> 1)] : [merged]));
    }
  }

  private insert(grp: Group, e: Entry): void {
    if (!grp.blocks.length) {
      grp.blocks.push([e]);
      return;
    }
    const bi = blockOf(grp.blocks, e);
    const b = grp.blocks[bi]!;
    b.splice(lowerBound(b, e), 0, e);
    if (b.length > 2 * BLOCK) grp.blocks.splice(bi + 1, 0, b.splice(BLOCK));
  }

  /** Sets a member's score; `null` removes it. Scores below `minScore` are not ranked. */
  apply(group: string, member: string, score: bigint | null): void {
    let grp = this.g.get(group);
    if (!grp) {
      if (score === null || !this.ranks(score)) return;
      grp = { blocks: [], scores: new Map(), sum: 0n };
      this.g.set(group, grp);
    }
    const old = grp.scores.get(member);
    const ranked = score !== null && this.ranks(score);
    if (old !== undefined) {
      if (ranked && old === score) return;
      this.remove(grp, { score: old, member });
      grp.scores.delete(member);
      grp.sum -= old;
    }
    if (ranked) {
      this.insert(grp, { score, member });
      grp.scores.set(member, score);
      grp.sum += score;
    } else if (!grp.scores.size) this.g.delete(group);
  }

  /** Sets many members' scores of one group (`null` removes), as `apply` for each in turn. */
  applyMany(group: string, changes: ReadonlyMap<string, bigint | null>): void {
    for (const [member, score] of changes) this.apply(group, member, score);
  }

  /** Bulk-fills a group (sorts once). Scores below `minScore` are skipped; members must be unique and the group not yet present. */
  load(group: string, entries: { member: string; score: bigint }[]): void {
    const sorted = entries.filter((e) => this.ranks(e.score)).sort(cmp);
    if (!sorted.length) return;
    const blocks: Entry[][] = [];
    for (let i = 0; i < sorted.length; i += BLOCK) blocks.push(sorted.slice(i, i + BLOCK));
    this.g.set(group, { blocks, scores: new Map(sorted.map((e) => [e.member, e.score])), sum: sorted.reduce((n, e) => n + e.score, 0n) });
  }

  /** Highest scores first, ties by member ascending. */
  top(group: string, n: number): { member: string; score: bigint }[] {
    const out: { member: string; score: bigint }[] = [];
    const grp = this.g.get(group);
    // a fraction counts down, anything not a finite positive number (NaN, -1) is 0
    const want = Number.isFinite(n) ? Math.floor(n) : n === Number.POSITIVE_INFINITY ? Number.MAX_SAFE_INTEGER : 0;
    if (!grp || want <= 0) return out;
    for (const b of grp.blocks) {
      for (const e of b) {
        out.push({ member: e.member, score: e.score });
        if (out.length >= want) return out;
      }
    }
    return out;
  }

  /** Blocks of a group (tests and diagnostics; not part of the RankedIndex view). */
  blockCount(group: string): number {
    return this.g.get(group)?.blocks.length ?? 0;
  }

  count(group: string): number {
    return this.g.get(group)?.scores.size ?? 0;
  }

  sum(group: string): bigint {
    return this.g.get(group)?.sum ?? 0n;
  }

  groups(): IterableIterator<string> {
    return this.g.keys();
  }
}
