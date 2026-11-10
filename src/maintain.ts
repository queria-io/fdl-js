// Retention for a repository's host, following https://fdl.queria.io/spec/data-files:
// take older versions out of retention (as `fdl expire` does) and delete the data files no
// live version reaches once past the grace period (as `fdl gc` does). Catalogs are never
// opened: what a version reads comes from its files.json
import { readConfig, resolve } from "./repo.js";
import { type R2Like, type Store, retryOnce } from "./store.js";

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

/** A repository in an R2 bucket that maintenance may change */
export const r2MaintainedStore = (
  bucket: R2Writable,
  prefix: string,
  reader: Store,
): MaintainedStore => {
  const page = async (key: string, cursor?: string): Promise<Placed[]> => {
    const found = await retryOnce(() => bucket.list({ prefix: prefix + key, cursor }));
    const here = found.objects.map((o) => ({
      key: o.key.slice(prefix.length),
      uploaded: o.uploaded,
    }));
    return found.truncated ? [...here, ...(await page(key, found.cursor))] : here;
  };
  return {
    ...reader,
    list: (key) => page(key),
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

/** The versions some ref points at */
const held = async (store: MaintainedStore): Promise<Set<string>> => {
  const keys = (await store.list("refs/")).map((p) => p.key);
  const targets = await Promise.all(keys.map((key) => resolve(store, key).catch(() => null)));
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
  now?: Date;
};

/**
 * Takes the versions behind the newest `keep` on the ref's first-parent line out of retention,
 * as `fdl expire` does. Versions a ref points at are kept. Stops at the first version already
 * expired. Returns the IDs expired, newest first
 */
export const expire = async (
  store: MaintainedStore,
  { keep, ref = "refs/heads/main", now = new Date() }: ExpireOptions,
): Promise<string[]> => {
  if (keep < 1) throw new RangeError(`keep must be at least 1, got ${keep}`);
  const newest = await resolve(store, ref).catch(() => null);
  if (newest === null) return [];
  const kept = await held(store);
  const walk = async (id: string | undefined, position: number): Promise<string[]> => {
    if (id === undefined || (position > 0 && (await isExpired(store, id)))) return [];
    const current = await record(store, id);
    if (current === null) return [];
    const expired = position >= keep && !kept.has(id);
    if (expired) await expireOne(store, id, now);
    const rest = await walk(current.parents[0], position + 1);
    return expired ? [id, ...rest] : rest;
  };
  return walk(newest, 0);
};

type FileList = {
  tables: { [table: string]: { files: { path: string; deletes?: { path: string } }[] } };
};
type Described = { tables?: { columns?: { asset?: boolean }[] }[] };
type Tree = { entries: { sha256: string }[] };

const SHA256 = /^assets\/[0-9a-f]{64}$/;

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
  const columns = ((described ? (JSON.parse(described) as Described) : {}).tables ?? []).flatMap(
    (t) => t.columns ?? [],
  );
  return {
    keys: [
      ...data.map((path) => dataDir + path),
      ...(tree ? (JSON.parse(tree) as Tree).entries.map((e) => `assets/${e.sha256}`) : []),
    ],
    assetRows: columns.some((c) => c.asset === true),
  };
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
 * read, so a version pushed meanwhile keeps what it reaches. Returns the keys deleted
 */
export const collect = async (
  store: MaintainedStore,
  { graceMs, now = new Date(), dryRun = false }: CollectOptions,
): Promise<string[]> => {
  const { data_dir: dataDir } = await readConfig(store);
  const before = now.getTime() - graceMs;
  const [data, assets] = await Promise.all([store.list(dataDir), store.list("assets/")]);
  const ids = (await store.list("versions/"))
    .map((p) => p.key.match(/^versions\/([^/]+)\/version\.json$/)?.[1])
    .filter((id): id is string => id !== undefined);
  const live = (
    await Promise.all(ids.map(async (id) => ((await isExpired(store, id)) ? null : id)))
  ).filter((id): id is string => id !== null);
  const reached = await Promise.all(live.map((id) => reachedBy(store, dataDir, id)));
  const kept = new Set(reached.flatMap((r) => r.keys));
  const assetRows = reached.some((r) => r.assetRows);
  const old = (placed: Placed[]) =>
    placed.filter((p) => p.uploaded.getTime() < before && !kept.has(p.key)).map((p) => p.key);
  const deleted = [
    ...old(data),
    ...(assetRows ? [] : old(assets).filter((key) => SHA256.test(key))),
  ].sort();
  if (!dryRun) await store.delete(deleted);
  return deleted;
};
