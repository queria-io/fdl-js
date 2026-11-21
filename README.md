# fdl-js

Read DuckLake tables published as [FDL](https://fdl.queria.io) repositories from JavaScript, without DuckDB: resolve a ref to a version, list the data files of its tables from `files.json`, and read the Parquet files by range with [hyparquet](https://github.com/hyparam/hyparquet). It runs in Cloudflare Workers, browsers and Node.

Reading never writes to the store; only the [retention](#retention) functions for a repository's host do. Rows are read as DuckLake returns them: `openParquet` reads a data file's delete file, and `readRows` and `readWhere` leave out the deleted rows. Columns are found by the `id` that `files.json` lists for each column, which DuckLake writes as the Parquet `field_id`, so a renamed column is read under its new name, a dropped column is left out, and a column or `struct` field added after a file was written holds its default (or null) in that file's rows. Fields inside a `struct`, a `list` or a `map` are found the same way. Defaults of every type DuckLake takes a default for are typed as hyparquet returns the column: a `Date` for dates and timestamps, microseconds (nanoseconds for `time_ns`) as a `bigint` for times, with a `timetz` moved to UTC. A table with renamed columns whose `files.json` has no `columns` (written before fdl-cli 0.24) is refused; read it with DuckDB.

## Install

```bash
npm install @queria-io/fdl-js
```

It is an ES module with type definitions, for Node 20 or later and any bundler.

## Use

```ts
import { httpStore, open, openParquet, readWhere, tableFiles } from "@queria-io/fdl-js";

const store = httpStore("https://data.example.org/towns/");
const version = await open(store); // HEAD, or open(store, "2025-final"), or a version ID
const [file] = tableFiles(version, "main.town") ?? [];
const parquet = await openParquet(store, file);
const rows = await readWhere(parquet, "lg_code", "131016", ["name"]);
```

`readWhere` reads only the row groups whose min/max can hold the value, so it reads little from files sorted by that column.

## Stores

A store is where the repository's root is. Keys are the layout's paths (`fdl.json`, `refs/heads/main`, `versions/<ID>/files.json`, `tables/...`).

| Store | Reads from |
| --- | --- |
| `httpStore(root, { fetch, headers })` | A URL, with `Range` requests |
| `r2Store(bucket, prefix)` | An R2 binding in Workers |
| `fileStore(dir)` from `@queria-io/fdl-js/node` | A local folder |

Each retries a failed request once. A store is three functions (`text`, `size`, `range`), so another one is a few lines.

## Caching

- Refs move. `refreshing(() => open(store), 5 * 60_000)` keeps the version for five minutes and keeps the last one when a ref cannot be read for a moment. Pass `{ onError }` to see those failures
- Data files never change, so their footers can be kept forever: pass `{ cache: cacheApiFooterCache(caches.default) }` (Workers) or `memoryFooterCache()` to `openParquet`. With `footer_bytes` in `files.json`, an uncached footer is one read

## Retention

For the host of a repository: `expire(store, { keep, graceMs })` takes versions out of retention as `fdl expire` does, keeping the newest `keep` on each branch, the versions refs point at, their merge bases with main, and those made within the grace period, and `collect(store, { graceMs })` deletes the data files and tree files (`objects/`) no live version reaches once past the grace period, as `fdl gc` does. Both read `files.json` and never open a catalog. Assets are deleted only while no live version's rows hold assets.

```ts
const store = r2MaintainedStore(env.BUCKET, "towns/");
await expire(store, { keep: 7, graceMs: 86_400_000 });
await collect(store, { graceMs: 86_400_000 });
```

`expire` and `collect` take `{ dryRun: true }` to list the versions or keys without changing anything. Any store with `list` (keys and when they were written), `put` and `delete` works. `expire` also takes `by` (a person, or a process such as a scheduled job) and `message` (why, in free text), which it records in each `expired.json`.

## What it follows

[Reading](https://fdl.queria.io/spec/reading) in the specification: `HEAD` and refs are followed, `version.json` must name its own ID and a known `fdl` major version, a version with `expired.json` is refused, and file paths from `files.json` are read under `data_dir`. A version without `files.json` cannot be read with this library.

## Develop

```bash
pnpm install
pnpm test
pnpm typecheck
pnpm check
pnpm build       # dist/
```

Releases are made from the commit messages ([Conventional Commits](https://www.conventionalcommits.org/)). Each push to `main` runs the Release workflow: when the commits since the last tag include a `fix:` it publishes a patch version, a `feat:` a minor one, and a breaking change (`!` after the type, or a `BREAKING CHANGE:` footer) a minor one. It publishes to npm, tags the commit `v<version>` and makes a GitHub release. `docs:`, `chore:`, `test:` and the like release nothing.

`test/fixture/repo` is a small repository made by the `fdl` command: `FDL=fdl test/make-fixture.sh` remakes it.
