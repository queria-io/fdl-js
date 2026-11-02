// Resolve a ref to a version and list the data files of its tables, following
// https://fdl.flo8s.com/spec/reading. Only files.json is read: the catalog is never opened
import { NotFoundError, type Store } from "./store";

/** The FDL major versions this reader knows */
const KNOWN_MAJOR = "0";

export class FdlError extends Error {}

export type Config = { fdl: string; id?: string; data_dir: string };

export type VersionRecord = {
  fdl: string;
  id: string;
  seq: number;
  parents: string[];
  created_at: string;
  author: { name: string; [k: string]: unknown };
  message: string;
  files?: { path: string; bytes: number; sha256: string };
  [k: string]: unknown;
};

export type FileEntry = {
  path: string;
  rows: number;
  bytes: number;
  footer_bytes?: number;
  deletes?: { path: string; rows: number; bytes: number };
};
export type FileList = {
  tables: Record<string, { files: FileEntry[]; renamed_columns?: boolean }>;
};

/** One version of a repository, with its files.json */
export type Version = { config: Config; record: VersionRecord; files: FileList };

/** A data file to read: its key in the store, rows and sizes from files.json */
export type DataFile = { key: string; rows: number; bytes: number; footerBytes?: number };

const VERSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

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

export const readConfig = async (store: Store): Promise<Config> => {
  const raw = await json<Partial<Config>>(store, "fdl.json");
  checkMajor(String(raw.fdl), "the repository");
  const dataDir = raw.data_dir ?? "data/";
  if (!safe(dataDir)) throw new FdlError(`data_dir is outside the repository: ${dataDir}`);
  return {
    ...raw,
    fdl: String(raw.fdl),
    data_dir: dataDir === "" || dataDir.endsWith("/") ? dataDir : `${dataDir}/`,
  };
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

/** Reads a version: checks its record and expiry, and reads its files.json */
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
  const filesPath = record.files?.path ?? "files.json";
  if (!safe(filesPath))
    throw new FdlError(`version ${id} names files.json outside it: ${filesPath}`);
  const files = await store.text(dir + filesPath);
  if (files === null) throw new FdlError(`version ${id} has no files.json; read it with DuckDB`);
  return { config: cfg, record, files: JSON.parse(files) as FileList };
};

/** Resolves rev and reads the version it names */
export const open = async (store: Store, rev = "HEAD"): Promise<Version> => {
  const [config, id] = await Promise.all([readConfig(store), resolve(store, rev)]);
  return readVersion(store, id, config);
};

/** The names (schema.table) of the version's tables */
export const tableNames = (version: Version): string[] => Object.keys(version.files.tables);

/**
 * The data files of a table (schema.table), or undefined when the version has no such table.
 * Throws for a table with delete files or renamed columns: reading its Parquet files as they are
 * would return deleted rows or miss columns
 */
export const tableFiles = (version: Version, table: string): DataFile[] | undefined => {
  const entry = version.files.tables[table];
  if (!entry) return undefined;
  if (entry.renamed_columns) throw new FdlError(`${table} has renamed columns`);
  return entry.files.map((f) => {
    if (f.deletes) throw new FdlError(`${table} has deleted rows`);
    if (!safe(f.path))
      throw new FdlError(`${table} names a file outside the repository: ${f.path}`);
    return {
      key: version.config.data_dir + f.path,
      rows: f.rows,
      bytes: f.bytes,
      ...(f.footer_bytes !== undefined ? { footerBytes: f.footer_bytes } : {}),
    };
  });
};

/**
 * Wraps load so that its result is kept for ttlMs. When a reload fails, the last result is
 * used until the next try, so a ref that cannot be read for a moment does not stop a reader
 */
export const refreshing = <T>(load: () => Promise<T>, ttlMs: number, now = () => Date.now()) => {
  let kept: { at: number; value: Promise<T> } | null = null;
  return (): Promise<T> => {
    if (kept && now() - kept.at < ttlMs) return kept.value;
    const previous = kept?.value;
    const value = previous ? load().catch(() => previous) : load();
    const entry = { at: now(), value };
    kept = entry;
    value.catch(() => {
      if (kept === entry) kept = null;
    });
    return value;
  };
};
