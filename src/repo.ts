// Resolve a ref to a version and list the data files of its tables, following
// https://fdl.queria.io/spec/reading. Only tables.json is read:
// the metadata is never opened
import { NotFoundError, type Store } from "./store.js";

/** The FDL major versions this reader knows */
const KNOWN_MAJOR = "0";

export class FdlError extends Error {}

export type Config = { fdl: string; data_dir: string };

export type VersionRecord = {
  fdl: string;
  id: string;
  seq: number;
  parents: string[];
  created_at: string;
  author: { name: string; [k: string]: unknown };
  message: string;
  /** The size, sha256 and storage format of metadata.db. A record past retention may have none */
  metadata?: { bytes?: number; sha256?: string; storage?: string };
  /** The version's tables.json, at its fixed name in the version folder. A record past retention may have none */
  tables?: { bytes: number; sha256: string };
  [k: string]: unknown;
};

export type FileEntry = {
  path: string;
  rows: number;
  bytes: number;
  footer_bytes?: number;
  deletes?: { path: string; rows: number; bytes: number };
};
/** A column of a table in tables.json. id is the field_id DuckLake writes in every data file */
export type Column = {
  id: number;
  name: string;
  type: string;
  nullable: boolean;
  /** The value, as text, that rows of files written before the column was added hold */
  default?: string;
  children?: Column[];
};

/** A version's tables.json */
export type FileList = {
  tables: Record<
    string,
    {
      files: FileEntry[];
      /** The table's columns */
      columns: Column[];
      renamed_columns?: boolean;
      /** The columns that identify a row, when declared */
      primary_key?: string[];
      /** The columns that hold the sha256 of an asset, when declared */
      assets?: string[];
      /** false: the writer makes no profile for the table */
      profile?: boolean;
    }
  >;
  /** Every view alive at the snapshot, keyed schema.view */
  views?: Record<string, ViewEntry>;
  /** Each other dataset the views read, keyed by the name their SQL reads it under */
  dependencies?: Record<string, Dependency>;
};

/** Another dataset a view reads: the URL of its root and, when pinned, the version to read */
export type Dependency = { url: string; version?: string };

/** A view in tables.json */
export type ViewEntry = {
  /** The view's SQL as the metadata records it, with DuckLake's `{DUCKLAKE_CATALOG}.` removed */
  sql: string;
  /** The columns its query returns, with DuckDB's type names */
  columns?: { name: string; type: string }[];
  /** false: the writer makes no profile for the view */
  profile?: boolean;
};

/** One version of a dataset, with its tables.json */
export type Version = { config: Config; record: VersionRecord; files: FileList };

/** A data file to read: its key in the store, rows and sizes from tables.json */
export type DataFile = {
  key: string;
  rows: number;
  bytes: number;
  footerBytes?: number;
  /** The delete file that lists the positions of the file's deleted rows */
  deletes?: { key: string; rows: number; bytes: number };
  /** The table's columns, to find them in the file by field_id and fill those it lacks */
  columns?: Column[];
};

const VERSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

/** Whether a value is a version ID: a lower-case UUIDv7 */
export const isVersionId = (value: unknown): value is string =>
  typeof value === "string" && VERSION_ID.test(value);

const required = async (store: Store, key: string): Promise<string> => {
  const text = await store.text(key);
  if (text === null) throw new NotFoundError(key);
  return text;
};

const json = async <T>(store: Store, key: string): Promise<T> =>
  JSON.parse(await required(store, key)) as T;

const checkMajor = (fdl: string, what: string) => {
  if (fdl.split(".")[0] !== KNOWN_MAJOR)
    throw new FdlError(`${what} is FDL ${fdl}, unknown to this reader`);
};

// A relative path that stays under the root. Dot segments are also caught percent-encoded,
// since URLs resolve %2e%2e as ..
const safe = (path: string) =>
  !path.startsWith("/") &&
  !path.includes("://") &&
  !path.replace(/%2e/gi, ".").split(/[/\\]/).includes("..");

/**
 * Whether a value may be fdl.json's data_dir: a folder under the root, apart from the format's own
 * (versions/, refs/, assets/). It is taken as written, never corrected
 */
export const isDataDir = (value: unknown): value is string =>
  typeof value === "string" &&
  value.endsWith("/") &&
  !["versions", "refs", "assets"].includes(value.split("/")[0]!) &&
  value
    .slice(0, -1)
    .split("/")
    .every((segment) => /^[a-z0-9_.-]+$/.test(segment) && !/^\.+$/.test(segment));

export const readConfig = async (store: Store): Promise<Config> => {
  const raw = await json<Partial<Config>>(store, "fdl.json");
  checkMajor(String(raw.fdl), "the dataset");
  const dataDir: unknown = "data_dir" in raw ? raw.data_dir : "tables/";
  if (!isDataDir(dataDir))
    throw new FdlError(`data_dir is not a folder under the root: ${JSON.stringify(dataDir)}`);
  return { ...raw, fdl: String(raw.fdl), data_dir: dataDir };
};

/** Candidates for a short name, in Git's order (refs/<name>, refs/tags/<name>, refs/heads/<name>) */
const candidates = (rev: string): string[] =>
  rev === "HEAD" || rev.startsWith("refs/")
    ? [rev]
    : [`refs/${rev}`, `refs/tags/${rev}`, `refs/heads/${rev}`];

/**
 * The version ID that rev names: a version ID itself, "HEAD", a full ref ("refs/tags/2025-final")
 * or a short one ("main", "2025-final"). Symbolic refs ("ref: refs/heads/main") are followed.
 * Refs change, so callers decide how long to keep the answer
 */
export const resolve = async (store: Store, rev = "HEAD"): Promise<string> => {
  if (VERSION_ID.test(rev)) return rev;
  if (!safe(rev)) throw new FdlError(`no ref ${rev}`);
  const follow = async (key: string, depth: number): Promise<string | null> => {
    const text = await store.text(key);
    if (text === null) return null;
    const value = text.trim();
    if (!value.startsWith("ref: ")) {
      if (!VERSION_ID.test(value)) throw new FdlError(`${key} does not hold a version ID`);
      return value;
    }
    if (depth > 4) throw new FdlError(`${key}: too many symbolic refs`);
    const target = value.slice(5).trim();
    if (!target.startsWith("refs/") || !safe(target))
      throw new FdlError(`${key} names a ref outside refs/: ${target}`);
    return follow(target, depth + 1);
  };
  const found = await candidates(rev).reduce<Promise<string | null>>(
    async (prev, key) => (await prev) ?? follow(key, 0),
    Promise.resolve(null),
  );
  if (!found) throw new FdlError(`no ref ${rev}`);
  return found;
};

/**
 * The branch HEAD names ("refs/heads/main"): the default branch, which readers that name no ref
 * read. A dataset without HEAD has refs/heads/main
 */
export const defaultBranch = async (store: Store): Promise<string> => {
  const text = await store.text("HEAD");
  if (text === null) return "refs/heads/main";
  const value = text.trim();
  if (!value.startsWith("ref: ")) throw new FdlError(`HEAD does not name a ref: ${value}`);
  const target = value.slice(5).trim();
  if (!target.startsWith("refs/heads/") || !safe(target))
    throw new FdlError(`HEAD names no branch: ${target}`);
  return target;
};

/** Reads a version: checks its record and expiry, and reads its tables.json */
export const readVersion = async (store: Store, id: string, config?: Config): Promise<Version> => {
  const dir = `versions/${id}/`;
  const [cfg, text, expired] = await Promise.all([
    config ?? readConfig(store),
    store.text(dir + "version.json"),
    store.text(dir + "expired.json"),
  ]);
  // A withdrawn version may have lost its version.json, so expiry is checked first
  if (expired !== null) throw new FdlError(`version ${id} is past retention or withdrawn`);
  if (text === null) throw new NotFoundError(dir + "version.json");
  const record = JSON.parse(text) as VersionRecord;
  if (record.id !== id) throw new FdlError(`${dir}version.json holds ${record.id}`);
  checkMajor(String(record.fdl), `version ${id}`);
  // The files of a version have fixed names; the record's `path` is not read
  const files = await store.text(dir + "tables.json");
  if (files === null) throw new FdlError(`version ${id} has no tables.json`);
  return { config: cfg, record, files: JSON.parse(files) as FileList };
};

/** Resolves rev and reads the version it names */
export const open = async (store: Store, rev = "HEAD"): Promise<Version> => {
  const [config, id] = await Promise.all([readConfig(store), resolve(store, rev)]);
  return readVersion(store, id, config);
};

/** The names (schema.table) of the version's tables */
export const tableNames = (version: Version): string[] => Object.keys(version.files.tables);

/** The names (schema.view) of the version's views */
export const viewNames = (version: Version): string[] => Object.keys(version.files.views ?? {});

/** A view (schema.view): its SQL and columns, or undefined when the version does not list it */
export const viewOf = (version: Version, view: string): ViewEntry | undefined => {
  const views = version.files.views;
  return views && Object.hasOwn(views, view) ? views[view] : undefined;
};

/**
 * The data files of a table (schema.table), or undefined when the version has no such table.
 * Each carries its delete file and the table's columns, so openParquet and readRows leave out
 * deleted rows and find renamed columns by field_id
 */
export const tableFiles = (
  version: Pick<Version, "config" | "files">,
  table: string,
): DataFile[] | undefined => {
  const { tables } = version.files;
  const entry = Object.hasOwn(tables, table) ? tables[table] : undefined;
  if (!entry) return undefined;
  const key = (path: string) => {
    if (!safe(path)) throw new FdlError(`${table} names a file outside the dataset: ${path}`);
    return version.config.data_dir + path;
  };
  return entry.files.map((f) => ({
    key: key(f.path),
    rows: f.rows,
    bytes: f.bytes,
    ...(f.footer_bytes !== undefined ? { footerBytes: f.footer_bytes } : {}),
    ...(f.deletes
      ? { deletes: { key: key(f.deletes.path), rows: f.deletes.rows, bytes: f.deletes.bytes } }
      : {}),
    columns: entry.columns,
  }));
};

export type RefreshingOptions = {
  /** Called when a reload fails and the last result is kept, so the failure can be logged */
  onError?: (error: unknown) => void;
  now?: () => number;
};

/**
 * Wraps load so that its result is kept for ttlMs. When a reload fails, the last result is
 * used until the next try, so a ref that cannot be read for a moment does not stop a reader.
 * The failure goes to onError; with no last result, the returned promise rejects instead
 */
export const refreshing = <T>(
  load: () => Promise<T>,
  ttlMs: number,
  { onError, now = () => Date.now() }: RefreshingOptions = {},
) => {
  let kept: { at: number; value: Promise<T> } | null = null;
  return (): Promise<T> => {
    if (kept && now() - kept.at < ttlMs) return kept.value;
    const previous = kept?.value;
    const value = previous
      ? load().catch((error: unknown) => {
          onError?.(error);
          return previous;
        })
      : load();
    const entry = { at: now(), value };
    kept = entry;
    value.catch(() => {
      if (kept === entry) kept = null;
    });
    return value;
  };
};
