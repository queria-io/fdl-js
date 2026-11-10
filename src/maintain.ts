// Retention for a repository's host, following https://fdl.queria.io/spec/data-files:
// take older versions out of retention (as `fdl expire` does) and delete the data files no
// live version reaches once past the grace period (as `fdl gc` does). Catalogs are never
// opened: what a version reads comes from its files.json
import { FdlError, readConfig, resolve } from "./repo.js";
import { type R2Like, type Store, r2Store, retryOnce } from "./store.js";

/** A listed object: its key and when it was written */
export type Placed = { key: string; uploaded: Date };

/** A store a host can change: a reader's store, plus listing, writing and deleting */
export type MaintainedStore = Store & {
  list: (prefix: string) => Promise<Placed[]>;
  put: (key: string, text: string) => Promise<void>;
  delete: (keys: string[]) => Promise<void>;
};

/** The part of an R2 binding that maintenance needs, besides what a reader needs */
export type R2Writable = R2Like & {
  list: (options: { prefix: string; cursor?: string }) => Promise<{
    objects: { key: string; uploaded: Date }[];
    truncated: boolean;
    cursor?: string;
  }>;
  put: (key: string, value: string) => Promise<unknown>;
  delete: (keys: string[]) => Promise<void>;
};

/** Up to this many keys go in one R2 delete */
const DELETE_BATCH = 1000;

const chunks = <T>(items: T[], size: number): T[][] =>
  Array.from({ length: Math.ceil(items.length / size) }, (_, i) =>
    items.slice(i * size, (i + 1) * size),
  );

/**
 * A repository in an R2 bucket that maintenance may change. Reads go through `reader`, which
 * defaults to `r2Store(bucket, prefix)`; pass another one (with a cache, say) for the same place
 */
export const r2MaintainedStore = (
  bucket: R2Writable,
  prefix: string,
  reader: Store = r2Store(bucket, prefix),
): MaintainedStore => {
  const list = async (key: string): Promise<Placed[]> => {
    const placed: Placed[] = [];
    let cursor: string | undefined;
    for (;;) {
      const found = await retryOnce(() => bucket.list({ prefix: prefix + key, cursor }));
      for (const o of found.objects)
        placed.push({ key: o.key.slice(prefix.length), uploaded: o.uploaded });
      if (!found.truncated) return placed;
      cursor = found.cursor;
    }
  };
  return {
    ...reader,
    list,
    put: async (key, text) => {
      await retryOnce(() => bucket.put(prefix + key, text));
    },
    delete: async (keys) => {
      for (const batch of chunks(keys, DELETE_BATCH))
        await retryOnce(() => bucket.delete(batch.map((key) => prefix + key)));
    },
  };
};

type Head = { id: string; parents: string[]; created_at: string; files?: { path: string } };

const record = async (store: Store, id: string): Promise<Head | null> => {
  const text = await store.text(`versions/${id}/version.json`);
  return text === null ? null : (JSON.parse(text) as Head);
};

const isExpired = async (store: Store, id: string) =>
  (await store.text(`versions/${id}/expired.json`)) !== null;

/**
 * The versions some ref points at. A ref that does not hold a version ID is skipped; a failed
 * read is not, since a version a ref points at must not be taken out
 */
const held = async (store: MaintainedStore): Promise<Set<string>> => {
  const keys = (await store.list("refs/")).map((p) => p.key);
  const targets = await Promise.all(
    keys.map((key) =>
      resolve(store, key).catch((e: unknown) => {
        if (e instanceof FdlError) return null;
        throw e;
      }),
    ),
  );
  return new Set(targets.filter((id): id is string => id !== null));
};

/** Takes one version out of retention: the mark first, then its catalogs and indexes */
const expireOne = async (store: MaintainedStore, id: string, now: Date) => {
  const dir = `versions/${id}/`;
  if (!(await isExpired(store, id)))
    await store.put(
      `${dir}expired.json`,
      `${JSON.stringify({ expired_at: now.toISOString(), reason: "retention" })}\n`,
    );
  const indexed = (await store.list(dir))
    .map((p) => p.key)
    .filter(
      (key) =>
        key === `${dir}ducklake.duckdb` ||
        key === `${dir}files.json` ||
        key === `${dir}tree.json` ||
        key.startsWith(`${dir}catalogs/`) ||
        key.startsWith(`${dir}profiles/`),
    );
  await store.delete(indexed);
};

export type ExpireOptions = {
  /** How many of the newest versions on the ref's first-parent line to keep (at least 1) */
  keep: number;
  ref?: string;
  /** Also keep the versions created at or after this time */
  since?: Date;
  now?: Date;
};

/**
 * Takes the versions behind the newest `keep` on the ref's first-parent line out of retention,
 * as `fdl expire` does. Versions a ref points at, and with `since` those created at or after
 * it, are kept. Stops at the first version already
 * expired. Returns the IDs expired, newest first
 */
export const expire = async (
  store: MaintainedStore,
  { keep, ref = "refs/heads/main", since, now = new Date() }: ExpireOptions,
): Promise<string[]> => {
  if (keep < 1) throw new RangeError(`keep must be at least 1, got ${keep}`);
  const newest = await resolve(store, ref).catch(() => null);
  if (newest === null) return [];
  const kept = await held(store);
  const walk = async (id: string | undefined, position: number): Promise<string[]> => {
    if (id === undefined || (position > 0 && (await isExpired(store, id)))) return [];
    const current = await record(store, id);
    if (current === null) return [];
    const recent = since !== undefined && new Date(current.created_at) >= since;
    const expired = position >= keep && !recent && !kept.has(id);
    if (expired) await expireOne(store, id, now);
    const rest = await walk(current.parents[0], position + 1);
    return expired ? [id, ...rest] : rest;
  };
  return walk(newest, 0);
};

type FileList = {
  tables: { [table: string]: { files: { path: string; deletes?: { path: string } }[] } };
};
type Described = { tables?: { kind?: string; fields?: { asset?: boolean }[] }[] };
type Tree = { entries: { sha256: string }[] };

const SHA256 = /^assets\/[0-9a-f]{64}$/;
/** A data or delete file as DuckLake names it, under data_dir */
const DATA_FILE = /(^|\/)ducklake-[^/]+\.parquet$/;

/** What a live version reaches: its data files, its tree's assets, and whether rows hold assets */
const reachedBy = async (store: Store, dataDir: string, id: string) => {
  const dir = `versions/${id}/`;
  const current = await record(store, id);
  const [files, described, tree] = await Promise.all([
    store.text(dir + (current?.files?.path ?? "files.json")),
    store.text(`${dir}dataset.json`),
    store.text(`${dir}tree.json`),
  ]);
  if (files === null)
    throw new Error(`version ${id} is live but has no files.json to say what it reads`);
  const list = JSON.parse(files) as FileList;
  const data = Object.values(list.tables).flatMap((t) =>
    t.files.flatMap((f) => [f.path, ...(f.deletes ? [f.deletes.path] : [])]),
  );
  const fields = ((described ? (JSON.parse(described) as Described) : {}).tables ?? [])
    .filter((t) => t.kind !== "view")
    .flatMap((t) => t.fields ?? []);
  return {
    keys: [
      ...data.map((path) => dataDir + path),
      ...(tree ? (JSON.parse(tree) as Tree).entries.map((e) => `assets/${e.sha256}`) : []),
    ],
    assetRows: fields.some((f) => f.asset === true),
  };
};

/** The versions without expired.json, from one listing of versions/ */
const liveVersions = async (store: MaintainedStore): Promise<string[]> => {
  const keys = new Set((await store.list("versions/")).map((p) => p.key));
  return [...keys]
    .map((key) => key.match(/^versions\/([^/]+)\/version\.json$/)?.[1])
    .filter((id): id is string => id !== undefined && !keys.has(`versions/${id}/expired.json`));
};

export type CollectOptions = {
  /** Objects written more recently than this are kept */
  graceMs: number;
  now?: Date;
  /** List only. Default false */
  dryRun?: boolean;
};

/**
 * Deletes the data files and assets no live version reaches, written longer ago than the grace
 * period, as `fdl gc` does. These are files only expired versions reached, and files working
 * catalogs wrote and never pushed. Assets are kept while a live version's rows hold assets,
 * since reading those rows needs the catalog. The objects are listed before the versions are
 * read, and the versions are listed again just before deleting, so a version pushed meanwhile
 * keeps what it reaches. Returns the keys deleted
 */
export const collect = async (
  store: MaintainedStore,
  { graceMs, now = new Date(), dryRun = false }: CollectOptions,
): Promise<string[]> => {
  const { data_dir: dataDir } = await readConfig(store);
  const before = now.getTime() - graceMs;
  const [data, assets] = await Promise.all([store.list(dataDir), store.list("assets/")]);
  const kept = new Set<string>();
  let assetRows = false;
  const keep = async (ids: string[]) => {
    for (const r of await Promise.all(ids.map((id) => reachedBy(store, dataDir, id)))) {
      for (const key of r.keys) kept.add(key);
      assetRows ||= r.assetRows;
    }
  };
  const read = await liveVersions(store);
  await keep(read);
  const old = (placed: Placed[]) =>
    placed.filter((p) => p.uploaded.getTime() < before && !kept.has(p.key)).map((p) => p.key);
  const planned = () =>
    [
      ...old(data).filter((key) => DATA_FILE.test(key.slice(dataDir.length))),
      ...(assetRows ? [] : old(assets).filter((key) => SHA256.test(key))),
    ].sort();
  if (dryRun) return planned();
  const seen = new Set(read);
  await keep((await liveVersions(store)).filter((id) => !seen.has(id)));
  const deleted = planned();
  await store.delete(deleted);
  return deleted;
};
