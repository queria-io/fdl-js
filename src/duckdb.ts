// What to ATTACH in DuckDB to read FDL datasets, following
// https://fdl.queria.io/spec/reading. Nothing here runs DuckDB: it returns the statements,
// and the caller runs them wherever its DuckDB is (WASM, Node, a server)
import {
  ExpiredError,
  FdlError,
  isVersionId,
  readConfig,
  readVersion,
  resolve,
  type Dependency,
} from "./repo.js";
import type { Store } from "./store.js";

/** A dataset to attach: the name SQL reads it under, the version, and the URLs of its metadata and data */
export type Attachment = {
  name: string;
  /** The dataset's root, ending in "/" */
  url: string;
  version: string;
  metadata: string;
  /** metadata.sha256 of the version's record, to check the metadata against before attaching it */
  sha256?: string;
  dataPath: string;
};

/**
 * The store for a dataset's root. The caller decides how to reach it, with which credentials.
 * It is also called with the URLs the datasets' tables.json name as dependencies
 */
export type Opener = (url: string) => Store;

const withSlash = (url: string) => (url.endsWith("/") ? url : url + "/");

/** The names DuckDB keeps for its own catalogs */
const RESERVED = new Set(["memory", "system", "temp"]);

/** A name to attach under, as tables.json's dependencies allow: DuckDB's catalog names ignore case */
const checkName = (name: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(name) || RESERVED.has(name))
    throw new FdlError(`cannot attach a dataset as ${JSON.stringify(name)}`);
};

const isAbsoluteUrl = (url: unknown): url is string => {
  if (typeof url !== "string") return false;
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
};

/**
 * The version an entry names, or the one HEAD names when it names none, checked as a reader must
 * (its record, its FDL version, not past retention), with the datasets its views read
 */
const attachment = async (
  open: Opener,
  url: string,
  rev: string,
): Promise<{ a: Omit<Attachment, "name">; deps: Record<string, Dependency> }> => {
  const store = open(url);
  const [config, id] = await Promise.all([readConfig(store), resolve(store, rev)]);
  const version = await readVersion(store, id, config);
  const sha256 = version.record.metadata?.sha256;
  return {
    a: {
      url,
      version: id,
      metadata: `${url}versions/${id}/metadata.db`,
      ...(typeof sha256 === "string" ? { sha256 } : {}),
      dataPath: url + config.data_dir,
    },
    deps: version.files.dependencies ?? {},
  };
};

/**
 * The datasets to attach to read the given ones, each keyed by the name SQL reads it under, as
 * tables.json's dependencies are. The datasets their views read come first, followed to the end.
 * Each entry is the version it names, or the one HEAD names. A dependency whose recorded version
 * is past retention or withdrawn is the one HEAD names. Throws when one name would be two
 * versions, since SQL could not tell them apart
 */
export const planAttach = async (
  open: Opener,
  datasets: Readonly<Record<string, Dependency>>,
): Promise<Attachment[]> => {
  // Each root and rev is read once, so HEAD read twice cannot give two versions
  const read = new Map<string, ReturnType<typeof attachment>>();
  const done = new Map<string, Awaited<ReturnType<typeof attachment>>>();
  // What each name was first found at, to refuse one name at two versions
  const seen = new Map<string, Awaited<ReturnType<typeof attachment>>>();
  const keyOf = (dep: Dependency) => `${withSlash(dep.url)}\n${dep.version ?? "HEAD"}`;
  // First every entry is read, siblings side by side. A name's dependencies are followed the
  // first time it is found, so a shared dataset is followed once and a cycle ends there
  const visit = async (name: string, dep: Dependency, from?: string): Promise<void> => {
    checkName(name);
    if (!isAbsoluteUrl(dep.url))
      throw new FdlError(`${from ?? "the plan"} names ${name} at no absolute URL`);
    if (from !== undefined && dep.version !== undefined && !isVersionId(dep.version))
      throw new FdlError(`${from} names ${name} at ${dep.version}, not a version ID`);
    const url = withSlash(dep.url);
    const key = keyOf(dep);
    let pending = read.get(key);
    if (!pending) {
      pending = attachment(open, url, dep.version ?? "HEAD");
      // A version a dataset recorded that is gone since is read at what HEAD names
      if (from !== undefined && dep.version !== undefined)
        pending = pending.catch((e: unknown) => {
          if (e instanceof ExpiredError) return attachment(open, url, "HEAD");
          throw e;
        });
      read.set(key, pending);
    }
    const found = await pending;
    done.set(key, found);
    const { a, deps } = found;
    const same = seen.get(name)?.a;
    // A version ID names one version wherever it is read from, origin or mirror
    if (same) {
      if (same.version === a.version) return;
      throw new FdlError(
        `${name} is both ${same.url} at ${same.version} and ${a.url} at ${a.version}`,
      );
    }
    seen.set(name, found);
    await Promise.all(
      Object.entries(deps).map(([n, d]) => visit(n, d, `${a.url} at ${a.version}`)),
    );
  };
  await Promise.all(Object.entries(datasets).map(([n, d]) => visit(n, d)));
  // Then the plan is laid out in the order the entries are listed, each dataset after the ones
  // its views read, so it does not depend on which read finished first
  const plan: Attachment[] = [];
  const placed = new Set<string>();
  const place = (name: string, dep: Dependency): void => {
    if (placed.has(name)) return;
    placed.add(name);
    // The URL this entry names, and the dependencies followed for its version
    const found = done.get(keyOf(dep));
    const followed = seen.get(name);
    if (!found || !followed) throw new FdlError(`${name} at ${dep.url} was not read`);
    for (const [n, d] of Object.entries(followed.deps)) place(n, d);
    plan.push({ name, ...found.a });
  };
  for (const [n, d] of Object.entries(datasets)) place(n, d);
  return plan;
};

const literal = (s: string) => `'${s.replaceAll("'", "''")}'`;
const identifier = (s: string) => `"${s.replaceAll('"', '""')}"`;

/** The ATTACH statement for one entry of a plan */
export const attachSql = (a: Attachment): string =>
  `ATTACH ${literal(`ducklake:${a.metadata}`)} AS ${identifier(a.name)} ` +
  `(DATA_PATH ${literal(a.dataPath)}, OVERRIDE_DATA_PATH true, READ_ONLY)`;
