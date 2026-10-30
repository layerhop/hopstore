import { AwsV4Signer } from "aws4fetch";
import { afterAll, describe, expect, it } from "vitest";
import { MemoryObjectStore, PreconditionFailed, S3Error, S3ObjectStore, type ObjectStore } from "./index.js";

const enc = (s: string) => new TextEncoder().encode(s);
const live = process.env.HOPSTORE_LIVE_S3 === "1" && process.env.S3_BUCKET;
const livePrefix = `hopstore-test/${Math.random().toString(36).slice(2, 10)}/`;
let liveStore: S3ObjectStore | undefined;
if (live) {
  const e = process.env;
  liveStore = new S3ObjectStore({ endpoint: e.S3_ENDPOINT!, region: e.S3_REGION!, bucket: e.S3_BUCKET!, accessKeyId: e.S3_ACCESS_KEY_ID!, secretAccessKey: e.S3_SECRET_ACCESS_KEY! });
  afterAll(async () => {
    for (const o of await liveStore!.list(livePrefix)) await liveStore!.delete(o.key);
  });
}

const impls: [string, () => { store: ObjectStore; p: string }][] = [["memory", () => ({ store: new MemoryObjectStore(), p: "t/" })]];
if (liveStore) impls.push(["live S3", () => ({ store: liveStore!, p: `${livePrefix}${Math.random().toString(36).slice(2, 8)}/` })]);

describe.each(impls)("ObjectStore (%s)", (_n, make) => {
  it("ifNoneMatch * creates once", async () => {
    const { store, p } = make();
    await store.put(`${p}a`, enc("one"), { ifNoneMatch: "*" });
    await expect(store.put(`${p}a`, enc("two"), { ifNoneMatch: "*" })).rejects.toBeInstanceOf(PreconditionFailed);
    expect(new TextDecoder().decode((await store.get(`${p}a`))!)).toBe("one");
  });

  it("ifMatch with current etag replaces, stale etag fails", async () => {
    const { store, p } = make();
    const { etag: e1 } = await store.put(`${p}m`, enc("v1"));
    const { etag: e2 } = await store.put(`${p}m`, enc("v2"), { ifMatch: e1 });
    expect(e2).not.toBe(e1);
    await expect(store.put(`${p}m`, enc("v3"), { ifMatch: e1 })).rejects.toBeInstanceOf(PreconditionFailed);
    expect(new TextDecoder().decode((await store.get(`${p}m`))!)).toBe("v2");
    expect((await store.head(`${p}m`))?.etag).toBe(e2);
  });

  it("range get returns the slice", async () => {
    const { store, p } = make();
    await store.put(`${p}r`, Uint8Array.from([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
    expect([...(await store.get(`${p}r`, { start: 2, end: 5 }))!]).toEqual([2, 3, 4, 5]);
  });

  it("list returns sorted keys under prefix only", async () => {
    const { store, p } = make();
    for (const k of ["b", "a/2", "a/1", "c"]) await store.put(`${p}${k}`, enc(k));
    await store.put(`${p}other-x`, enc("x"));
    const got = await store.list(`${p}`);
    expect(got.map((o) => o.key)).toEqual([`${p}a/1`, `${p}a/2`, `${p}b`, `${p}c`, `${p}other-x`]);
    expect(got[0]).toMatchObject({ size: 3 });
    expect((await store.list(`${p}a/`)).map((o) => o.key)).toEqual([`${p}a/1`, `${p}a/2`]);
  });

  it("round-trips keys with spaces, &, <, = and unicode through put, list, get and delete", async () => {
    const { store, p } = make();
    const odd = ["a b", "x&y<z>", "k=v+1", "café/日本"].map((k) => `${p}${k}`);
    for (const k of odd) await store.put(k, enc(k));
    expect((await store.list(p)).map((o) => o.key)).toEqual([...odd].sort());
    for (const k of odd) expect(new TextDecoder().decode((await store.get(k))!)).toBe(k);
    for (const k of odd) await store.delete(k);
    expect(await store.list(p)).toEqual([]);
  });

  it("get of missing key is null; head too; delete is idempotent", async () => {
    const { store, p } = make();
    expect(await store.get(`${p}nope`)).toBeNull();
    expect(await store.head(`${p}nope`)).toBeNull();
    await store.put(`${p}d`, enc("d"));
    await store.delete(`${p}d`);
    await store.delete(`${p}d`);
    expect(await store.get(`${p}d`)).toBeNull();
  });
});

describe("MemoryObjectStore", () => {
  it("failNext throws the given status for the next n calls", async () => {
    const s = new MemoryObjectStore();
    s.failNext(2, 503);
    await expect(s.get("k")).rejects.toBeInstanceOf(S3Error);
    await expect(s.put("k", enc("x"))).rejects.toMatchObject({ status: 503 });
    expect(await s.get("k")).toBeNull();
  });
});

const cfg = { endpoint: "https://s3.example.com", region: "default", bucket: "b", accessKeyId: "AK", secretAccessKey: "SK" };

describe("S3ObjectStore", () => {
  it("strips quotes from If-Match and maps 409 to PreconditionFailed", async () => {
    const seen: Request[] = [];
    const fetchFn = (async (req: Request) => {
      seen.push(req);
      return new Response("", { status: 409 });
    }) as unknown as typeof fetch;
    const s = new S3ObjectStore(cfg, fetchFn);
    await expect(s.put("k/a", enc("x"), { ifMatch: '"abc123"' })).rejects.toBeInstanceOf(PreconditionFailed);
    expect(seen).toHaveLength(1); // 4xx is never retried
    expect(seen[0]!.headers.get("if-match")).toBe("abc123");
    expect(seen[0]!.url).toBe("https://s3.example.com/b/k/a");
  });

  it("never runs more than maxConcurrency requests", async () => {
    let inflight = 0;
    let peak = 0;
    const fetchFn = (async () => {
      inflight++;
      peak = Math.max(peak, inflight);
      await new Promise((r) => setTimeout(r, 5));
      inflight--;
      return new Response("", { status: 404 });
    }) as unknown as typeof fetch;
    const s = new S3ObjectStore({ ...cfg, maxConcurrency: 4 }, fetchFn);
    await Promise.all(Array.from({ length: 40 }, (_, i) => s.get(`k/${i}`)));
    expect(peak).toBe(4);
  });

  it("retries 5xx with backoff then succeeds; gives up after 3 retries", async () => {
    let n = 0;
    const ok = (async () => (++n < 3 ? new Response("", { status: 503 }) : new Response("hi", { status: 200 }))) as unknown as typeof fetch;
    expect(new TextDecoder().decode((await new S3ObjectStore(cfg, ok).get("k"))!)).toBe("hi");
    expect(n).toBe(3);
    n = 0;
    const bad = (async () => (n++, new Response("", { status: 500 }))) as unknown as typeof fetch;
    await expect(new S3ObjectStore(cfg, bad).get("k")).rejects.toMatchObject({ status: 500 });
    expect(n).toBe(4);
  });

  it("rejects unsupported keys and parses paginated listings", async () => {
    const pages = [
      `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>tok/1=</NextContinuationToken><Contents><Key>p/a</Key><Size>3</Size><ETag>&quot;e1&quot;</ETag></Contents></ListBucketResult>`,
      `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>p/b</Key><Size>5</Size><ETag>"e2"</ETag></Contents></ListBucketResult>`,
    ];
    const urls: string[] = [];
    const fetchFn = (async (req: Request) => (urls.push(req.url), new Response(pages.shift()!, { status: 200 }))) as unknown as typeof fetch;
    const s = new S3ObjectStore(cfg, fetchFn);
    expect(await s.list("p/")).toEqual([
      { key: "p/a", size: 3, etag: "e1" },
      { key: "p/b", size: 5, etag: "e2" },
    ]);
    expect(urls[1]).toContain("continuation-token=tok%2F1%3D");
    await expect(s.get("p/../x")).rejects.toThrow(/unsupported object key/);
  });

  it("parses listings with escaped, CDATA and unicode keys, namespaces and whitespace", async () => {
    const pages = [
      `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>b</Name><Prefix>p/</Prefix><KeyCount>3</KeyCount><MaxKeys>1000</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>a&amp;b+c=</NextContinuationToken>
  <Contents>
    <Key>p/a &amp; b</Key><LastModified>2026-10-30T00:00:00.000Z</LastModified>
    <ETag>&quot;e1&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents><Key>p/&lt;tag&gt; &#x26; &#38;</Key><ETag>"e2"</ETag><Size>2</Size><Owner><ID>x</ID><DisplayName>y &amp; z</DisplayName></Owner></Contents>
  <!-- a comment with <Key>not/a/key</Key> -->
  <Contents><Key><![CDATA[p/c<d>&e]]></Key><ETag>e3</ETag><Size>3</Size></Contents>
</ListBucketResult>`,
      `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>p/café 日本 caf&#xE9;</Key><Size>4</Size><ETag>"e4"</ETag></Contents></ListBucketResult>`,
    ];
    const urls: string[] = [];
    const fetchFn = (async (req: Request) => (urls.push(req.url), new Response(pages.shift()!, { status: 200 }))) as unknown as typeof fetch;
    const got = await new S3ObjectStore(cfg, fetchFn).list("p/");
    expect(got.map((o) => o.key)).toEqual(["p/<tag> & &", "p/a & b", "p/c<d>&e", "p/café 日本 café"]);
    expect(got.map((o) => [o.size, o.etag])).toEqual([[2, "e2"], [1, "e1"], [3, "e3"], [4, "e4"]]);
    expect(new URL(urls[1]!).searchParams.get("continuation-token")).toBe("a&b+c=");
    expect(urls[1]).toContain("continuation-token=a%26b%2Bc%3D");
  });

  it("fails loudly on a truncated listing without a token and on malformed XML", async () => {
    const reply = (xml: string) => (async () => new Response(xml, { status: 200 })) as unknown as typeof fetch;
    await expect(new S3ObjectStore(cfg, reply("<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>")).list("p/")).rejects.toThrow(/truncated listing without NextContinuationToken/);
    await expect(new S3ObjectStore(cfg, reply("<ListBucketResult><Contents><Key>p/a</Contents></ListBucketResult>")).list("p/")).rejects.toThrow(/malformed ListObjectsV2 response/);
    await expect(new S3ObjectStore(cfg, reply("<Error><Code>X</Code></Error>")).list("p/")).rejects.toThrow(/malformed ListObjectsV2 response/);
    await expect(new S3ObjectStore(cfg, reply("<ListBucketResult><Contents><Size>1</Size></Contents></ListBucketResult>")).list("p/")).rejects.toThrow(/malformed ListObjectsV2 response/);
    await expect(new S3ObjectStore(cfg, reply("<ListBucketResult><Contents><Key>a &bogus; b</Key><Size>1</Size></Contents></ListBucketResult>")).list("p/")).rejects.toThrow(/malformed ListObjectsV2 response/);
  });

  it("encodes any other key into the path the way SigV4 canonicalises it", async () => {
    const urls: string[] = [];
    const fetchFn = (async (req: Request) => (urls.push(req.url), new Response("", { status: 404 }))) as unknown as typeof fetch;
    const s = new S3ObjectStore(cfg, fetchFn);
    for (const k of ["p/a & b", "p/café 日本", "p/x=1+(2)!*'~"]) await s.get(k);
    expect(urls).toEqual(["https://s3.example.com/b/p/a%20%26%20b", "https://s3.example.com/b/p/caf%C3%A9%20%E6%97%A5%E6%9C%AC", "https://s3.example.com/b/p/x%3D1%2B%282%29%21%2A%27~"]);
    // the path on the wire is exactly the canonical path the request is signed with
    for (const url of urls) expect(new AwsV4Signer({ url, accessKeyId: "AK", secretAccessKey: "SK", service: "s3", region: "default" }).encodedPath).toBe(new URL(url).pathname);
    for (const k of ["", "/p/a", "p/./a", "p/../a", "p/a\nb", "p/\u0000", "p/\ud800"]) await expect(s.get(k)).rejects.toThrow(/unsupported object key/);
  });

  it("virtual-hosted-style URLs with forcePathStyle: false (path style is the default)", async () => {
    const urls: string[] = [];
    const fetchFn = (async (req: Request) => (urls.push(req.url), req.method === "GET" && req.url.includes("list-type") ? new Response("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>") : new Response("", { status: 404 }))) as unknown as typeof fetch;
    const vh = new S3ObjectStore({ ...cfg, endpoint: "https://s3.example.com:8443/", forcePathStyle: false }, fetchFn);
    await vh.get("k/a b");
    await vh.list("p/");
    const ps = new S3ObjectStore(cfg, fetchFn);
    await ps.list("p/");
    expect(urls).toEqual(["https://b.s3.example.com:8443/k/a%20b", "https://b.s3.example.com:8443/?list-type=2&prefix=p%2F", "https://s3.example.com/b?list-type=2&prefix=p%2F"]);
  });

  it("holds the slot until the body is consumed", async () => {
    let inflight = 0;
    let peak = 0;
    const fetchFn = (async () => {
      inflight++;
      peak = Math.max(peak, inflight);
      const body = new ReadableStream<Uint8Array>({
        async start(c) {
          await new Promise((r) => setTimeout(r, 10)); // body arrives after the headers
          c.enqueue(enc("x"));
          c.close();
          inflight--;
        },
      });
      return new Response(body, { status: 200 });
    }) as unknown as typeof fetch;
    const s = new S3ObjectStore({ ...cfg, maxConcurrency: 3 }, fetchFn);
    await Promise.all(Array.from({ length: 20 }, (_, i) => s.get(`k/${i}`)));
    expect(peak).toBeLessThanOrEqual(3);
  });

  it("maps 409/412 to PreconditionFailed only for put", async () => {
    const fetchFn = (async () => new Response("", { status: 412 })) as unknown as typeof fetch;
    const s = new S3ObjectStore(cfg, fetchFn);
    await expect(s.get("k")).rejects.toBeInstanceOf(S3Error);
    await expect(s.delete("k")).rejects.toMatchObject({ status: 412 });
    await expect(s.put("k", enc("x"))).rejects.toBeInstanceOf(PreconditionFailed);
  });
});
