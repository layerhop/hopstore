import { createHash } from "node:crypto";
import { PreconditionFailed, S3Error, type ObjectInfo, type ObjectStore } from "./s3.js";

const md5 = (b: Uint8Array) => createHash("md5").update(b).digest("hex");

/** In-memory ObjectStore that mimics Contabo: unquoted hex-md5 etags, conditional writes fail with PreconditionFailed. */
export class MemoryObjectStore implements ObjectStore {
  readonly objects = new Map<string, Uint8Array>();
  private failures = 0;
  private failStatus = 503;

  /** The next `n` calls throw an error carrying `status` (default 503), for fault-injection tests. */
  failNext(n: number, status = 503): void {
    this.failures = n;
    this.failStatus = status;
  }

  private maybeFail(op: string) {
    if (this.failures > 0) {
      this.failures--;
      throw new S3Error(`MemoryObjectStore ${op}: injected HTTP ${this.failStatus}`, this.failStatus);
    }
  }

  async put(key: string, body: Uint8Array, opts: { ifNoneMatch?: "*"; ifMatch?: string; contentType?: string } = {}): Promise<{ etag: string }> {
    this.maybeFail("put");
    const cur = this.objects.get(key);
    if (opts.ifNoneMatch === "*" && cur) throw new PreconditionFailed(`${key} exists`);
    if (opts.ifMatch !== undefined && (!cur || md5(cur) !== opts.ifMatch.replace(/"/g, ""))) throw new PreconditionFailed(`${key} etag mismatch`);
    const copy = body.slice();
    this.objects.set(key, copy);
    return { etag: md5(copy) };
  }

  async get(key: string, range?: { start: number; end: number }): Promise<Uint8Array | null> {
    this.maybeFail("get");
    const o = this.objects.get(key);
    if (!o) return null;
    return (range ? o.subarray(range.start, range.end + 1) : o).slice();
  }

  async head(key: string): Promise<ObjectInfo | null> {
    this.maybeFail("head");
    const o = this.objects.get(key);
    return o ? { key, size: o.byteLength, etag: md5(o) } : null;
  }

  async list(prefix: string): Promise<ObjectInfo[]> {
    this.maybeFail("list");
    return [...this.objects]
      .filter(([k]) => k.startsWith(prefix))
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, o]) => ({ key, size: o.byteLength, etag: md5(o) }));
  }

  async delete(key: string): Promise<void> {
    this.maybeFail("delete");
    this.objects.delete(key);
  }
}
