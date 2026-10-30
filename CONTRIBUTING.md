# Contributing

Issues and pull requests are welcome. For anything larger than a small fix, open an issue first so we can agree on the
approach.

## Setup and checks

Node.js 20 or newer.

```
npm install
npm run typecheck
npm test
npm run build
```

`npm test` runs the offline suite on `MemoryObjectStore`, including the crash tests in `src/crash.test.ts` (lost local
disk, a kill between the WAL object and the manifest, injected S3 failures) and the quick start example. Changes to
commit, flush, snapshot, seal or recovery code should come with a test there or in `src/store.test.ts` that fails
without the change.

## Live S3 test

`src/live.test.ts` runs against a real bucket and is skipped unless you opt in:

```
HOPSTORE_LIVE_S3=1 S3_ENDPOINT=... S3_REGION=... S3_BUCKET=... S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... npm test
```

It writes under `hopstore-test/<random>/` and deletes that prefix afterwards. CI runs it on every push against a
local SeaweedFS server (see `.github/workflows/ci.yml`); you can do the same with Docker and no cloud account. Results from providers other than Contabo
(AWS S3, MinIO, R2, other Ceph deployments) are especially welcome; please include them in an issue or pull request.

## Style

TypeScript strict, ES modules, no new runtime dependencies without discussion. Keep comments on the why, as in the
existing source. By contributing you agree that your contributions are licensed under the Apache License 2.0.
