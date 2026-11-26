# fdl-js

Read DuckLake tables published as [FDL](https://fdl.queria.io) datasets from JavaScript, without DuckDB: resolve a ref to a version, list the data files of its tables from `tables.json`, and read the Parquet files by range with [hyparquet](https://github.com/hyparam/hyparquet). It runs in Cloudflare Workers, browsers and Node.

Reading never writes to the store; only the [retention](#retention) functions for a dataset's host do. Rows are read as DuckLake returns them: `openParquet` reads a data file's delete file, and `readRows` and `readWhere` leave out the deleted rows. Columns are found by the `id` that `tables.json` lists for each column, which DuckLake writes as the Parquet `field_id`, so a renamed column is read under its new name, a dropped column is left out, and a column or `struct` field added after a file was written holds its default (or null) in that file's rows. Fields inside a `struct`, a `list` or a `map` are found the same way. Defaults of every type DuckLake takes a default for are typed as hyparquet returns the column: a `Date` for dates and timestamps, microseconds (nanoseconds for `time_ns`) as a `bigint` for times, with a `timetz` moved to UTC.

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

`readWhere` reads only the row groups whose min/max can hold the value, so it reads little from files sorted by that column. Besides a value, it takes a condition:

```ts
await readWhere(parquet, "lg_code", { in: ["131016", "131024"] });
await readWhere(parquet, "population", { gte: 1000, lt: 5000 }); // >=, >, <=, < and BETWEEN
await readWhere(parquet, "address", { prefix: "東京都千代田区" }); // LIKE '東京都千代田区%'
```

Strings compare in code point order, as Parquet's statistics do, and null matches nothing. For a value or `in`, when min/max leaves more than one row group, `readWhere` also reads the columns' bloom filters (in one request when they are next to each other, as DuckDB writes them) and leaves out the row groups whose filter holds none of the values. DuckDB writes bloom filters only for dictionary-encoded columns, so they help on columns with repeated values that sit in a few row groups, and on values a file does not hold at all.

## Views and declarations

`viewNames(version)` lists the views in `tables.json`, and `viewOf(version, "main.town_count")` gives a view's `sql` and the `columns` its query returns, with DuckDB's type names.

`version.files` is the version's `tables.json`. Each table there declares its `primary_key`, its asset columns (`assets`) and `profile: false` when the writer makes no profile for it, and a view may declare `profile: false` too. `dependencies` gives each other dataset the views read, keyed by the name their SQL reads it under: its `url` and, when pinned, the `version` to read. Descriptions of the dataset live in its own tables, which read like any other table.

## Attaching in DuckDB

`@queria-io/fdl-js/duckdb` gives the statements to read datasets in DuckDB, as [Reading](https://fdl.queria.io/spec/reading) describes. It does not run DuckDB: run the statements wherever yours is (WASM, Node, a server).

```ts
import { httpStore } from "@queria-io/fdl-js";
import { attachSql, planAttach } from "@queria-io/fdl-js/duckdb";

const plan = await planAttach((url) => httpStore(url), {
  towns: { url: "https://data.example.org/towns/" },
  census: { url: "https://data.example.org/census/", version: "2025-final" },
});
for (const a of plan) await conn.query(attachSql(a)); // INSTALL ducklake and LOAD ducklake first
```

`planAttach` takes the datasets keyed by the name SQL reads them under, in the shape of `dependencies` in `tables.json`. Each entry reads the version `version` names (a version ID, a tag or a branch), or the one `main` holds. The datasets the views of each version read come first in the plan, followed to the end. Each entry has the version ID it resolved to, the URL of that version's metadata, its `sha256` from `version.json` to check the metadata against, and the data path. One name at two versions is refused, since SQL could not tell them apart, and so is a name that `dependencies` would not allow (`[a-z_][a-z0-9_]*`, not `memory`, `system` or `temp`). The function you pass opens each dataset's root, so credentials stay with you. It is also called with the URLs that the datasets' `tables.json` name, so send a token only to the hosts it is meant for.

## Stores

A store is where the dataset's root is. Keys are the layout's paths (`fdl.json`, `refs/heads/main`, `versions/<ID>/tables.json`, `tables/...`).

| Store | Reads from |
| --- | --- |
| `httpStore(root, { fetch, headers })` | A URL, with `Range` requests |
| `r2Store(bucket, prefix)` | An R2 binding in Workers |
| `fileStore(dir)` from `@queria-io/fdl-js/node` | A local folder |

Each retries a failed request once. A store is three functions (`text`, `size`, `range`), so another one is a few lines.

## Caching

- Refs move. `refreshing(() => open(store), 5 * 60_000)` keeps the version for five minutes and keeps the last one when a ref cannot be read for a moment. Pass `{ onError }` to see those failures
- Data files never change, so their footers can be kept forever: pass `{ cache: cacheApiFooterCache(caches.default) }` (Workers) or `memoryFooterCache()` to `openParquet`. With `footer_bytes` in `tables.json`, an uncached footer is one read

## Retention

For the host of a dataset: `expire(store, { keep, graceMs })` takes versions out of retention as `fdl expire` does, keeping the newest `keep` on each branch, the versions refs point at, their merge bases with main, and those made within the grace period, and `collect(store, { graceMs })` deletes the data files and assets no live version reaches once past the grace period, as `fdl gc` does. Both read `tables.json` and never open the metadata. The assets a version reaches are read from the asset columns its `tables.json` declares, in its data files; pass `assetValues` to read them another way. While a live version's assets cannot be read, no asset is deleted.

```ts
const store = r2MaintainedStore(env.BUCKET, "towns/");
await expire(store, { keep: 7, graceMs: 86_400_000 });
await collect(store, { graceMs: 86_400_000 });
```

`expire` and `collect` take `{ dryRun: true }` to list the versions or keys without changing anything. Any store with `list` (keys and when they were written), `put` and `delete` works. `expire` also takes `by` (a person, or a process such as a scheduled job) and `message` (why, in free text), which it records in each `expired.json`.

## What it follows

[Reading](https://fdl.queria.io/spec/reading) in the specification: `HEAD` and refs are followed, `version.json` must name its own ID and a known `fdl` major version, a version with `expired.json` is refused, and file paths from `tables.json` are read under `data_dir`. A version without `tables.json` cannot be read with this library.

## Develop

```bash
pnpm install
pnpm test
pnpm typecheck
pnpm check
pnpm build       # dist/
```

Releases are made from the commit messages ([Conventional Commits](https://www.conventionalcommits.org/)). Each push to `main` runs the Release workflow: when the commits since the last tag include a `fix:` it publishes a patch version, a `feat:` a minor one, and a breaking change (`!` after the type, or a `BREAKING CHANGE:` footer) a minor one. It publishes to npm, tags the commit `v<version>` and makes a GitHub release. `docs:`, `chore:`, `test:` and the like release nothing.

`test/fixture/repo` is a small dataset made by the `fdl` command: `FDL=fdl test/make-fixture.sh` remakes it.
