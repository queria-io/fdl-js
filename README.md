# fdl-js

Read DuckLake tables published as [FDL](https://fdl.flo8s.com) repositories from JavaScript, without DuckDB: resolve a ref to a version, list the data files of its tables from `files.json`, and read the Parquet files by range with [hyparquet](https://github.com/hyparam/hyparquet). It runs in Cloudflare Workers, browsers and Node.

It reads; it does not write. It reads only tables whose files can be read as they are: a table with deleted rows or renamed columns is refused, since reading its Parquet files without the catalog would return deleted rows or miss columns. Read those with DuckDB.

## Install

```bash
npm install @flo8s/fdl-js
```

It is an ES module with type definitions, for Node 20 or later and any bundler.

## Use

```ts
import { httpStore, open, openParquet, readWhere, tableFiles } from "@flo8s/fdl-js";

const store = httpStore("https://data.example.org/towns/");
const version = await open(store); // HEAD, or open(store, "2025-final"), or a version ID
const [file] = tableFiles(version, "main.town") ?? [];
const parquet = await openParquet(store, file);
const rows = await readWhere(parquet, "lg_code", "131016", ["name"]);
```

`readWhere` reads only the row groups whose min/max can hold the value, so it reads little from files sorted by that column.

## Stores

A store is where the repository's root is. Keys are the layout's paths (`fdl.json`, `refs/heads/main`, `versions/<ID>/files.json`, `data/...`).

| Store | Reads from |
| --- | --- |
| `httpStore(root, { fetch, headers })` | A URL, with `Range` requests |
| `r2Store(bucket, prefix)` | An R2 binding in Workers |
| `fileStore(dir)` from `@flo8s/fdl-js/node` | A local folder |

Each retries a failed request once. A store is three functions (`text`, `size`, `range`), so another one is a few lines.

## Caching

- Refs move. `refreshing(() => open(store), 5 * 60_000)` keeps the version for five minutes and keeps the last one when a ref cannot be read for a moment
- Data files never change, so their footers can be kept forever: pass `{ cache: cacheApiFooterCache(caches.default) }` (Workers) or `memoryFooterCache()` to `openParquet`. With `footer_bytes` in `files.json`, an uncached footer is one read

## What it follows

[Reading](https://fdl.flo8s.com/spec/reading) in the specification: `HEAD` and refs are followed, `version.json` must name its own ID and a known `fdl` major version, a version with `expired.json` is refused, and file paths from `files.json` are read under `data_dir`. A version without `files.json` cannot be read with this library.

## Develop

```bash
pnpm install
pnpm test
pnpm typecheck
pnpm check
pnpm build       # dist/
```

To release, set `version` in `package.json` in a pull request, merge it, and push the tag `v<version>`. The Release workflow publishes it to npm.

`test/fixture/repo` is a small repository made by the `fdl` command: `FDL=fdl test/make-fixture.sh` remakes it.
