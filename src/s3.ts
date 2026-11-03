/**
 * Object store over S3-compatible storage (Contabo): path-style URLs by default (virtual-hosted-style on request),
 * SigV4 via aws4fetch, conditional writes, retry of 5xx/network errors, and a bound on in-flight requests. Contabo
 * specifics: If-Match needs the ETag WITHOUT quotes, and a lost conditional race answers 412 or 409 (both map to
 * PreconditionFailed).
 */
import { AwsClient } from "aws4fetch";

export interface ObjectInfo {
  key: string;
  size: number;
  etag: string;
}

/** A conditional write lost its race (HTTP 412 or 409). */
export class PreconditionFailed extends Error {}

export interface ObjectStore {
  /**
   * Conditional writes (ifNoneMatch/ifMatch) lose with PreconditionFailed. A put retried after a 5xx or network error
   * may already have succeeded, so a later PreconditionFailed can mean "my own earlier attempt won": callers must
   * re-read and compare when that matters.
   */
  put(key: string, body: Uint8Array, opts?: { ifNoneMatch?: "*"; ifMatch?: string; contentType?: string }): Promise<{ etag: string }>;
  /** `range` is inclusive of `end` (HTTP Range semantics). null = 404. */
  get(key: string, range?: { start: number; end: number }): Promise<Uint8Array | null>;
  head(key: string): Promise<ObjectInfo | null>;
  /** Sorted by key; follows continuation tokens. */
  list(prefix: string): Promise<ObjectInfo[]>;
  delete(key: string): Promise<void>;
}

export interface S3Config {
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  /** Max concurrent requests (Contabo copes with about 16). */
  maxConcurrency?: number;
  /**
   * Path-style URLs, `<endpoint>/<bucket>/<key>` (default true: Contabo, MinIO, Ceph RGW). false: virtual-hosted-style,
   * `<scheme>://<bucket>.<endpoint host>/<key>`, for providers that only serve that (AWS S3 for new buckets).
   */
  forcePathStyle?: boolean;
}

class Retryable {
  constructor(readonly status: number) {}
}

/** HTTP failure with its status code (also what MemoryObjectStore.failNext throws). */
export class S3Error extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

const unquote = (s: string) => s.replace(/&quot;/g, "").replace(/"/g, "");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** URI-encodes a path segment or query value as SigV4 canonicalises it: everything but A-Z a-z 0-9 - . _ ~ is escaped. */
const encodeS3 = (s: string) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);

/** An XML element: local name (namespace prefix dropped), child elements and its own decoded text (CDATA included). */
interface XmlElement {
  name: string;
  children: XmlElement[];
  text: string;
}

const ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" };

/** Decodes the five predefined entities and numeric character references; anything else is malformed. */
function decodeXmlText(s: string): string {
  if (!s.includes("&")) return s;
  return s.replace(/&([^;&]*)(;?)/g, (_m, ref: string, semi: string) => {
    const code = !semi ? Number.NaN : /^#x[0-9a-fA-F]+$/.test(ref) ? Number.parseInt(ref.slice(2), 16) : /^#[0-9]+$/.test(ref) ? Number(ref.slice(1)) : Number.NaN;
    if (semi && ENTITIES[ref] !== undefined) return ENTITIES[ref]!;
    if (Number.isNaN(code) || code > 0x10ffff) throw new Error(`bad entity &${ref}${semi}`);
    return String.fromCodePoint(code);
  });
}

/**
 * A minimal XML parser for S3 responses: elements, attributes (ignored), text with entity decoding, CDATA, comments
 * and the XML declaration. DOCTYPEs are refused (no entity expansion). Throws on anything unbalanced.
 */
function parseXml(xml: string): XmlElement {
  const root: XmlElement = { name: "", children: [], text: "" };
  const stack = [root];
  const local = (qname: string) => qname.slice(qname.indexOf(":") + 1);
  let i = 0;
  while (i < xml.length) {
    const top = stack[stack.length - 1]!;
    const lt = xml.indexOf("<", i);
    const text = xml.slice(i, lt < 0 ? xml.length : lt);
    if (text) top.text += decodeXmlText(text);
    if (lt < 0) break;
    const skip = (open: string, close: string) => {
      const end = xml.indexOf(close, lt + open.length);
      if (end < 0) throw new Error(`unterminated ${open}`);
      return end;
    };
    if (xml.startsWith("<!--", lt)) i = skip("<!--", "-->") + 3;
    else if (xml.startsWith("<![CDATA[", lt)) {
      const end = skip("<![CDATA[", "]]>");
      top.text += xml.slice(lt + 9, end);
      i = end + 3;
    } else if (xml.startsWith("<?", lt)) i = skip("<?", "?>") + 2;
    else if (xml.startsWith("<!", lt)) throw new Error("DOCTYPE and other declarations are not supported");
    else {
      let gt = lt + 1;
      for (let quote = ""; gt < xml.length && (quote || xml[gt] !== ">"); gt++) {
        const c = xml[gt]!;
        if (quote) quote = c === quote ? "" : quote;
        else if (c === '"' || c === "'") quote = c;
      }
      if (gt >= xml.length) throw new Error("unterminated tag");
      const tag = xml.slice(lt + 1, gt);
      if (tag.startsWith("/")) {
        const name = local(tag.slice(1).trim());
        if (stack.length < 2 || top.name !== name) throw new Error(`unexpected </${name}>`);
        stack.pop();
      } else {
        const qname = /^[^\s/>]+/.exec(tag)?.[0];
        if (!qname) throw new Error("empty tag");
        const el: XmlElement = { name: local(qname), children: [], text: "" };
        top.children.push(el);
        if (!tag.endsWith("/")) stack.push(el);
      }
      i = gt + 1;
    }
  }
  if (stack.length !== 1) throw new Error(`unclosed <${stack[stack.length - 1]!.name}>`);
  if (root.children.length !== 1 || root.text.trim()) throw new Error("expected exactly one root element");
  return root.children[0]!;
}

/** One page of a ListObjectsV2 response. */
function parseListObjectsV2(xml: string): { objects: ObjectInfo[]; truncated: boolean; nextToken: string | undefined } {
  const doc = parseXml(xml);
  if (doc.name !== "ListBucketResult") throw new Error(`root element is ${doc.name}, not ListBucketResult`);
  const child = (el: XmlElement, name: string) => el.children.find((c) => c.name === name);
  const objects: ObjectInfo[] = [];
  for (const c of doc.children) {
    if (c.name !== "Contents") continue;
    const key = child(c, "Key")?.text;
    const size = Number(child(c, "Size")?.text);
    if (!key || !Number.isSafeInteger(size) || size < 0) throw new Error("Contents without a Key or a valid Size");
    objects.push({ key, size, etag: unquote(child(c, "ETag")?.text ?? "") });
  }
  return { objects, truncated: child(doc, "IsTruncated")?.text.trim() === "true", nextToken: child(doc, "NextContinuationToken")?.text || undefined };
}

export class S3ObjectStore implements ObjectStore {
  private readonly aws: AwsClient;
  private readonly max: number;
  private inflight = 0;
  private readonly waiters: (() => void)[] = [];

  /** the bucket's URL without a trailing slash: `<endpoint>/<bucket>` or `<scheme>://<bucket>.<host>` */
  private readonly base: string;
  /** ListObjectsV2 URL before the query */
  private readonly listBase: string;

  constructor(
    private readonly cfg: S3Config,
    private readonly fetchFn: typeof fetch = fetch,
  ) {
    this.aws = new AwsClient({ accessKeyId: cfg.accessKeyId, secretAccessKey: cfg.secretAccessKey, region: cfg.region, service: "s3", retries: 0 });
    this.max = cfg.maxConcurrency ?? 16;
    const endpoint = cfg.endpoint.replace(/\/+$/, "");
    if (cfg.forcePathStyle ?? true) {
      this.base = `${endpoint}/${cfg.bucket}`;
      this.listBase = this.base;
    } else {
      const u = new URL(endpoint);
      u.hostname = `${cfg.bucket}.${u.hostname}`;
      this.base = `${u.origin}${u.pathname.replace(/\/+$/, "")}`;
      this.listBase = `${this.base}/`;
    }
  }

  /**
   * Each path segment is URI-encoded exactly as SigV4 canonicalises it (so the signed path is the path sent). Refused:
   * empty keys, a leading "/", "." or ".." segments (URL parsers would resolve them) and control characters.
   */
  private url(key: string) {
    const bad = () => new Error(`unsupported object key: ${JSON.stringify(key)}`);
    if (!key || key.startsWith("/") || /[\u0000-\u001f\u007f]/.test(key)) throw bad();
    const segs = key.split("/");
    if (segs.some((x) => x === "." || x === "..")) throw bad();
    try {
      return `${this.base}/${segs.map(encodeS3).join("/")}`;
    } catch {
      throw bad(); // a lone surrogate
    }
  }

  private async slot<T>(fn: () => Promise<T>): Promise<T> {
    if (this.inflight >= this.max) await new Promise<void>((r) => this.waiters.push(r));
    else this.inflight++;
    try {
      return await fn();
    } finally {
      const next = this.waiters.shift();
      if (next) next(); // hand the slot over
      else this.inflight--;
    }
  }

  /**
   * Signed request with retries (5xx and network errors, 3 retries at 200 ms * 2^n). `consume` reads the response
   * inside the concurrency slot, so the slot is held until the body is fully transferred; 4xx go to `consume` too.
   */
  private async send<T>(url: string, init: { method: string; body?: Uint8Array; headers?: Record<string, string> }, consume: (r: Response) => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.slot(async () => {
          const req = await this.aws.sign(url, { method: init.method, body: init.body as RequestInit["body"], headers: init.headers });
          const r = await this.fetchFn(req);
          if (r.status >= 500) {
            await r.arrayBuffer().catch(() => undefined);
            throw new Retryable(r.status);
          }
          return consume(r);
        });
      } catch (e) {
        if (e instanceof S3Error || e instanceof PreconditionFailed) throw e;
        if (attempt >= 3) throw e instanceof Retryable ? new S3Error(`S3 ${init.method} ${url.split("?")[0]}: HTTP ${e.status}`, e.status) : e;
      }
      await sleep(200 * 2 ** attempt);
    }
  }

  private async fail(op: string, key: string, res: Response, precondition = false): Promise<never> {
    const text = (await res.text().catch(() => "")).slice(0, 200);
    if (precondition && (res.status === 412 || res.status === 409)) throw new PreconditionFailed(`S3 ${op} ${key}: HTTP ${res.status}`);
    throw new S3Error(`S3 ${op} ${key}: HTTP ${res.status} ${text}`, res.status);
  }

  async put(key: string, body: Uint8Array, opts: { ifNoneMatch?: "*"; ifMatch?: string; contentType?: string } = {}): Promise<{ etag: string }> {
    const headers: Record<string, string> = { "content-type": opts.contentType ?? "application/octet-stream", "content-length": String(body.byteLength) };
    if (opts.ifNoneMatch) headers["if-none-match"] = opts.ifNoneMatch;
    if (opts.ifMatch !== undefined) headers["if-match"] = unquote(opts.ifMatch);
    return this.send(this.url(key), { method: "PUT", body, headers }, async (res) => {
      if (!res.ok) return this.fail("PUT", key, res, true);
      return { etag: unquote(res.headers.get("etag") ?? "") };
    });
  }

  async get(key: string, range?: { start: number; end: number }): Promise<Uint8Array | null> {
    return this.send(this.url(key), { method: "GET", headers: range ? { range: `bytes=${range.start}-${range.end}` } : undefined }, async (res) => {
      if (res.status === 404) return null;
      if (!res.ok) return this.fail("GET", key, res);
      return new Uint8Array(await res.arrayBuffer());
    });
  }

  async head(key: string): Promise<ObjectInfo | null> {
    return this.send(this.url(key), { method: "HEAD" }, async (res) => {
      if (res.status === 404) return null;
      if (!res.ok) return this.fail("HEAD", key, res);
      return { key, size: Number(res.headers.get("content-length") ?? 0), etag: unquote(res.headers.get("etag") ?? "") };
    });
  }

  /** ListObjectsV2, every page (continuation tokens); a malformed page, or a truncated one without a token, throws. */
  async list(prefix: string): Promise<ObjectInfo[]> {
    const out: ObjectInfo[] = [];
    let token: string | undefined;
    do {
      let q = `list-type=2&prefix=${encodeS3(prefix)}`;
      if (token) q += `&continuation-token=${encodeS3(token)}`;
      const xml = await this.send(`${this.listBase}?${q}`, { method: "GET" }, async (res) => {
        if (!res.ok) return this.fail("LIST", prefix, res);
        return res.text();
      });
      let page: ReturnType<typeof parseListObjectsV2>;
      try {
        page = parseListObjectsV2(xml);
      } catch (e) {
        throw new Error(`S3 LIST ${prefix}: malformed ListObjectsV2 response: ${e instanceof Error ? e.message : String(e)}`);
      }
      for (const o of page.objects) out.push(o); // never spread a data-sized array into arguments (call stack)
      if (page.truncated && !page.nextToken) throw new Error(`S3 LIST ${prefix}: truncated listing without NextContinuationToken`);
      token = page.truncated ? page.nextToken : undefined;
    } while (token);
    return out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key: string): Promise<void> {
    await this.send(this.url(key), { method: "DELETE" }, async (res) => {
      if (!res.ok && res.status !== 404) return this.fail("DELETE", key, res);
    });
  }
}
