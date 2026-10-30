/**
 * Local write-ahead log: one append-only file per epoch (`wal-<epoch 20 digits>.log`) in the store's local directory.
 * The first record is a header naming the epoch and the writer id that owns it; every append is fsynced.
 */
import { open, readdir, readFile, rename, rm, type FileHandle } from "node:fs/promises";
import { join } from "node:path";
import { decodeRecords, encodeRecords, type Batch } from "./codec.js";

export interface WalHeader {
  epoch: number;
  writer: string;
}

const fileName = (epoch: number) => `wal-${String(epoch).padStart(20, "0")}.log`;

/** fsync a directory so a created or renamed file survives a crash (not supported everywhere; best effort). */
async function syncDir(dir: string) {
  let fh: FileHandle | undefined;
  try {
    fh = await open(dir, "r");
    await fh.sync();
  } catch {
    // some platforms refuse fsync on a directory handle
  } finally {
    await fh?.close().catch(() => {});
  }
}

async function writeFresh(path: string, header: WalHeader, batches: Batch[]): Promise<void> {
  const fh = await open(path, "w");
  try {
    await fh.write(encodeRecords([{ h: 1, ...header }, ...batches]));
    await fh.sync();
  } finally {
    await fh.close();
  }
}

export class LocalWal {
  private constructor(
    private readonly dir: string,
    readonly header: WalHeader,
    private fh: FileHandle,
  ) {}

  /** Creates (or replaces) the file for `header.epoch` holding `batches`, fsynced before it returns. */
  static async create(dir: string, header: WalHeader, batches: Batch[]): Promise<LocalWal> {
    const path = join(dir, fileName(header.epoch));
    const tmp = `${path}.tmp`;
    await writeFresh(tmp, header, batches);
    await rename(tmp, path);
    await syncDir(dir);
    return new LocalWal(dir, header, await open(path, "a"));
  }

  get path(): string {
    return join(this.dir, fileName(this.header.epoch));
  }

  /** Appends batches (or their pre-encoded records) with one write and one fsync. */
  async append(batches: Batch[] | Uint8Array): Promise<void> {
    await this.fh.write(batches instanceof Uint8Array ? batches : encodeRecords(batches));
    await this.fh.sync();
  }

  /** Atomically replaces the file with the header plus `batches` (drops what S3 already holds). Not concurrent with append. */
  async rewrite(batches: Batch[]): Promise<void> {
    const tmp = `${this.path}.tmp`;
    await writeFresh(tmp, this.header, batches);
    await this.fh.close();
    await rename(tmp, this.path);
    await syncDir(this.dir);
    this.fh = await open(this.path, "a");
  }

  async close(): Promise<void> {
    await this.fh.close().catch(() => {});
  }
}

/** Reads every local WAL file (header plus batches up to the first torn record). Files without a valid header are skipped. */
export async function readLocalWals(dir: string): Promise<{ file: string; header: WalHeader; batches: Batch[] }[]> {
  let names: string[];
  try {
    names = await readdir(dir);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw e;
  }
  const out: { file: string; header: WalHeader; batches: Batch[] }[] = [];
  for (const name of names.filter((n) => /^wal-\d{20}\.log$/.test(n)).sort()) {
    const file = join(dir, name);
    const [head, ...rest] = decodeRecords<{ h?: number } & WalHeader & Batch>(await readFile(file)).records;
    if (!head || head.h !== 1) continue;
    out.push({ file, header: { epoch: head.epoch, writer: head.writer }, batches: rest.map((b) => ({ seq: b.seq, ops: b.ops })) });
  }
  return out;
}

/** Deletes local WAL files other than `keep` (leftovers of earlier epochs or of an open that lost its claim). */
export async function removeLocalWals(dir: string, keep?: string): Promise<void> {
  for (const w of await readLocalWals(dir)) if (w.file !== keep) await rm(w.file, { force: true });
}
