// Retention for a dataset's host, following https://fdl.queria.io/spec/data-files:
// take older versions out of retention (as `fdl expire` does) and delete the data files no
// live version reaches once past the grace period (as `fdl gc` does). Metadata files are never
// opened: what a version reaches comes from its tables.json, or `named` in an earlier version's
// files.json, and the assets it reaches from the rows of the asset columns tables.json declares
import { openParquet, readRows } from "./parquet.js";
import {
  type Config,
  type FileList as Listed,
  FdlError,
  type Version,
  isFileName,
  isVersionId,
  readConfig,
  resolve,
  tableFiles,
} from "./repo.js";
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
 * A dataset in an R2 bucket that maintenance may change. Reads go through `reader`, which
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

type Head = {
  id: string;
  parents: string[];
  created_at: string;
  tables?: { path?: unknown };
  files?: { path?: unknown };
  dataset?: { path: string };
  /** The metadata file. Records written by fdl 0.26 and earlier name it `catalog` */
  metadata?: { path?: unknown };
  catalog?: { path?: unknown };
};

const record = async (store: Store, id: string): Promise<Head | null> => {
  const text = await store.text(`versions/${id}/version.json`);
  return text === null ? null : (JSON.parse(text) as Head);
};

const isExpired = async (store: Store, id: string) =>
  (await store.text(`versions/${id}/expired.json`)) !== null;

/**
 * Every ref that holds a version, with the version. A ref that does not hold a version ID is
 * skipped; a failed read is not, since a version a ref points at must not be taken out
 */
const refTargets = async (store: MaintainedStore): Promise<Map<string, string>> => {
  const keys = (await store.list("refs/")).map((p) => p.key);
  const targets = await Promise.all(
    keys.map((key) =>
      resolve(store, key).catch((e: unknown) => {
        if (e instanceof FdlError) return null;
        throw e;
      }),
    ),
  );
  return new Map(
    keys.flatMap((key, i) => {
      const target = targets[i];
      return typeof target === "string" ? [[key, target] as const] : [];
    }),
  );
};

/** The versions some ref points at */
const held = async (store: MaintainedStore): Promise<Set<string>> =>
  new Set((await refTargets(store)).values());

/** Reads each version's record once. A version without `version.json` cannot be accounted for */
const records = (store: Store) => {
  const read = new Map<string, Promise<Head>>();
  return (id: string): Promise<Head> => {
    const known = read.get(id);
    if (known) return known;
    const reading = record(store, id).then((head) => {
      if (head === null) throw new FdlError(`version ${id} has no version.json`);
      return head;
    });
    read.set(id, reading);
    return reading;
  };
};

/** The version IDs among a record's parents. A parent in any other form is not followed */
const parentsOf = (head: Head): string[] => head.parents.filter(isVersionId);

/** `id` and every version behind it, through every parent */
const ancestors = async (id: string, read: (id: string) => Promise<Head>): Promise<Set<string>> => {
  const found = new Set<string>();
  const pending = [id];
  for (let current = pending.pop(); current !== undefined; current = pending.pop()) {
    if (found.has(current)) continue;
    found.add(current);
    pending.push(...parentsOf(await read(current)).filter((parent) => !found.has(parent)));
  }
  return found;
};

/** The merge bases of two versions, from everything behind each: the common ancestors no other common ancestor descends from */
const mergeBases = async (
  mine: Set<string>,
  other: Set<string>,
  read: (id: string) => Promise<Head>,
): Promise<string[]> => {
  const common = [...mine].filter((id) => other.has(id));
  const behind = new Set((await Promise.all(common.map(read))).flatMap(parentsOf));
  return common.filter((id) => !behind.has(id));
};

/** `id` and the versions behind it by first parent, `count` in all at most */
const firstParents = async (
  id: string,
  count: number,
  read: (id: string) => Promise<Head>,
): Promise<string[]> => {
  const found = [id];
  let current = id;
  while (found.length < count) {
    const parent = parentsOf(await read(current))[0];
    if (parent === undefined) break;
    found.push(parent);
    current = parent;
  }
  return found;
};

/**
 * The metadata files to delete: the one the record names (`metadata`, or `catalog` in records
 * written by fdl 0.26 and earlier), or both default names when the record names none it can use
 */
const metadataPaths = (head: Head): string[] => {
  const path = (head.metadata ?? head.catalog)?.path;
  return isFileName(path) ? [path] : ["metadata.db", "ducklake.duckdb"];
};

/** Takes one version out of retention: the mark first, then its metadata and indexes */
const expireOne = async (
  store: MaintainedStore,
  id: string,
  head: Head,
  now: Date,
  { by, message }: Pick<ExpireOptions, "by" | "message">,
) => {
  const dir = `versions/${id}/`;
  const metadata = new Set(metadataPaths(head).map((path) => dir + path));
  // tables.json or files.json, by the name the record gives and by the default names
  const listings = new Set(
    ["tables.json", "files.json", ...[head.tables?.path, head.files?.path].filter(isFileName)].map(
      (path) => dir + path,
    ),
  );
  if (!(await isExpired(store, id)))
    await store.put(
      `${dir}expired.json`,
      `${JSON.stringify({ expired_at: now.toISOString(), reason: "retention", ...(by ? { by } : {}), ...(message ? { message } : {}) })}\n`,
    );
  const indexed = (await store.list(dir))
    .map((p) => p.key)
    .filter(
      (key) =>
        metadata.has(key) ||
        listings.has(key) ||
        key === `${dir}dataset.json` ||
        key === `${dir}tree.json` ||
        key.startsWith(`${dir}metadata/`) ||
        // Other DuckLake formats, as fdl 0.26 and earlier lay them out
        key.startsWith(`${dir}catalogs/`) ||
        key.startsWith(`${dir}profiles/`),
    );
  await store.delete(indexed);
};

export type ExpireOptions = {
  /** How many of the newest versions on each branch's first-parent line to keep (at least 1) */
  keep: number;
  /** Also keep the versions created at or after this time */
  since?: Date;
  /** Also keep the versions created less than this long ago: a writer puts a version before it moves a ref to it */
  graceMs: number;
  /** Who or what takes the versions out, recorded in `expired.json` */
  by?: string;
  /** Why, in free text for people, recorded in `expired.json` */
  message?: string;
  now?: Date;
  /** List only. Default false */
  dryRun?: boolean;
};

const MAIN = "refs/heads/main";

/**
 * Takes versions out of retention, as `fdl expire` does, choosing them as the spec does
 * (https://fdl.queria.io/spec/data-files#choosing-the-versions-to-take-out). Kept are the
 * versions a ref points at, their merge bases with main, the newest `keep` on each branch's
 * first-parent line, and those created at or after `since` or within the grace period. Nothing
 * is taken out before main has a version. Returns the IDs taken out, newest first
 */
export const expire = async (
  store: MaintainedStore,
  { keep, since, graceMs, by, message, now = new Date(), dryRun = false }: ExpireOptions,
): Promise<string[]> => {
  if (keep < 1) throw new RangeError(`keep must be at least 1, got ${keep}`);
  const targets = await refTargets(store);
  const main = targets.get(MAIN);
  if (main === undefined) return [];
  const read = records(store);
  const kept = new Set(targets.values());
  const behindMain = await ancestors(main, read);
  for (const [name, target] of targets) {
    if (target !== main)
      for (const id of await mergeBases(behindMain, await ancestors(target, read), read))
        kept.add(id);
    if (name.startsWith("refs/heads/"))
      for (const id of await firstParents(target, keep, read)) kept.add(id);
  }
  const cutoff = now.getTime() - graceMs;
  const taken: string[] = [];
  for (const id of await liveVersions(store)) {
    if (kept.has(id)) continue;
    const made = new Date((await read(id)).created_at).getTime();
    if (Number.isNaN(made)) throw new FdlError(`version ${id} has no valid created_at`);
    if (made >= cutoff || (since !== undefined && made >= since.getTime())) continue;
    taken.push(id);
  }
  taken.sort().reverse();
  if (dryRun) return taken;
  // A ref may have come to point at one of them meanwhile
  const stillHeld = await held(store);
  const expired = taken.filter((id) => !stillHeld.has(id));
  for (const id of expired) await expireOne(store, id, await read(id), now, { by, message });
  return expired;
};

type FileList = Listed & {
  /** Every data file the metadata names, at any snapshot. Absent in files.json written before it */
  named?: string[];
};
type Described = { tables?: { kind?: string; fields?: { asset?: boolean }[] }[] };

const SHA256 = /^[0-9a-f]{64}$/;
const ASSET = /^assets\/[0-9a-f]{64}$/;
/** A data or delete file as DuckLake names it, under data_dir */
const DATA_FILE = /(^|\/)ducklake-[^/]+\.parquet$/;

/** Reads the values the given columns of a table hold, in every row of the version `id` */
export type AssetValues = (
  store: Store,
  version: Pick<Version, "config" | "files"> & { id: string },
  table: string,
  columns: string[],
) => Promise<unknown[]>;

/** Reads the columns from the table's data files, leaving out deleted rows */
export const readAssetValues: AssetValues = async (store, version, table, columns) => {
  const files = await Promise.all(
    (tableFiles(version, table) ?? []).map(async (file) =>
      readRows(await openParquet(store, file), { columns }),
    ),
  );
  return files.flat().flatMap((row) => columns.map((c) => row[c]));
};

/**
 * What a live version reaches: the data files its metadata names, the assets its rows hold, and
 * whether every data file and asset it reaches is known. A version with tables.json keeps one
 * snapshot, so its `tables` list every data file, and the asset columns it declares hold every
 * asset. An earlier version's files.json lists the data files in `named`; without it, the metadata
 * may name more. Its assets are not read: while it declares asset columns (in files.json, or in a
 * version written before tags, a field with `asset: true` in dataset.json), no asset is known
 */
const reachedBy = async (store: Store, config: Config, id: string, assetValues: AssetValues) => {
  const dir = `versions/${id}/`;
  const current = await record(store, id);
  // A record that names tables.json is read from it alone, since only tables.json lists every
  // data file the version reaches
  const listing =
    current?.tables !== undefined ? current.tables.path : (current?.files?.path ?? "files.json");
  if (!isFileName(listing)) throw new FdlError(`version ${id} names a file outside it: ${listing}`);
  const datasetPath = current?.dataset?.path ?? "dataset.json";
  if (!isFileName(datasetPath))
    throw new FdlError(`version ${id} names a file outside it: ${datasetPath}`);
  const [files, described] = await Promise.all([
    store.text(dir + listing),
    current?.tables !== undefined ? null : store.text(dir + datasetPath),
  ]);
  if (files === null)
    throw new Error(
      `version ${id} is live but has no tables.json or files.json to say what it reads`,
    );
  const list = JSON.parse(files) as FileList;
  const data = [
    ...(list.named ?? []),
    ...Object.values(list.tables).flatMap((t) =>
      t.files.flatMap((f) => [f.path, ...(f.deletes ? [f.deletes.path] : [])]),
    ),
  ];
  const declared = Object.entries(list.tables).flatMap(([table, t]) =>
    t.assets && t.assets.length > 0 ? [{ table, columns: t.assets }] : [],
  );
  const fields = ((described ? (JSON.parse(described) as Described) : {}).tables ?? [])
    .filter((t) => t.kind !== "view")
    .flatMap((t) => t.fields ?? []);
  const assets =
    current?.tables === undefined
      ? declared.length > 0 || fields.some((f) => f.asset === true)
        ? undefined
        : []
      : await Promise.all(
          declared.map(({ table, columns }) =>
            assetValues(store, { id, config, files: list }, table, columns),
          ),
        ).then(
          (values) =>
            values
              .flat()
              .filter((v): v is string => typeof v === "string" && SHA256.test(v))
              .map((v) => `assets/${v}`),
          // Assets that cannot be read are not known
          () => undefined,
        );
  return {
    keys: data.map((path) => config.data_dir + path),
    /** The assets the version's rows hold, or undefined when they are not known */
    assets,
    named: current?.tables !== undefined || list.named !== undefined,
  };
};

/** The versions without expired.json, from one listing of versions/ */
const liveVersions = async (store: MaintainedStore): Promise<string[]> => {
  const keys = new Set((await store.list("versions/")).map((p) => p.key));
  return [...keys]
    .map((key) => key.match(/^versions\/([^/]+)\/version\.json$/)?.[1])
    .filter((id): id is string => isVersionId(id) && !keys.has(`versions/${id}/expired.json`));
};

export type CollectOptions = {
  /** Objects written more recently than this are kept */
  graceMs: number;
  now?: Date;
  /** List only. Default false */
  dryRun?: boolean;
  /** Reads the values of a version's asset columns. Default readAssetValues */
  assetValues?: AssetValues;
};

/**
 * Deletes the data files and assets no live version reaches, written longer ago than the grace
 * period, as `fdl gc` does. These are files only expired versions reached, and files working
 * metadata files wrote and never pushed. A version reaches every data file its metadata names: those
 * in `tables` of its tables.json, or in `named` of an earlier version's files.json; while a live
 * version's files.json lacks `named`, no data file is deleted. A version reaches the assets the
 * asset columns of its tables.json hold; while a live version's assets cannot be read, no asset is
 * deleted. The objects are listed before the versions are read, and the versions are listed again
 * just before deleting, so a version pushed meanwhile keeps what it reaches. Returns the keys
 * deleted
 */
export const collect = async (
  store: MaintainedStore,
  { graceMs, now = new Date(), dryRun = false, assetValues = readAssetValues }: CollectOptions,
): Promise<string[]> => {
  const config = await readConfig(store);
  const dataDir = config.data_dir;
  const before = now.getTime() - graceMs;
  const [data, assets] = await Promise.all([store.list(dataDir), store.list("assets/")]);
  const kept = new Set<string>();
  let assetsUnknown = false;
  let unnamed = false;
  const keep = async (ids: string[]) => {
    for (const r of await Promise.all(ids.map((id) => reachedBy(store, config, id, assetValues)))) {
      for (const key of [...r.keys, ...(r.assets ?? [])]) kept.add(key);
      assetsUnknown ||= r.assets === undefined;
      unnamed ||= !r.named;
    }
  };
  const read = await liveVersions(store);
  await keep(read);
  const old = (placed: Placed[]) =>
    placed.filter((p) => p.uploaded.getTime() < before && !kept.has(p.key)).map((p) => p.key);
  const planned = () =>
    [
      // A live version whose files.json lacks `named` may reach data files no listing names
      ...(unnamed ? [] : old(data).filter((key) => DATA_FILE.test(key.slice(dataDir.length)))),
      ...(assetsUnknown ? [] : old(assets).filter((key) => ASSET.test(key))),
    ].sort();
  if (dryRun) return planned();
  const seen = new Set(read);
  await keep((await liveVersions(store)).filter((id) => !seen.has(id)));
  const deleted = planned();
  await store.delete(deleted);
  return deleted;
};
