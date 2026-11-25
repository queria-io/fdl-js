// What to ATTACH in DuckDB to read FDL datasets, following
// https://fdl.queria.io/spec/reading. Nothing here runs DuckDB: it returns the statements,
// and the caller runs them wherever its DuckDB is (WASM, Node, a server)
import { FdlError, readConfig, readVersion, resolve, type Dependency } from "./repo.js";
import type { Store } from "./store.js";

/** A dataset to attach: the name SQL reads it under, the version, and the URLs of its metadata and data */
export type Attachment = {
  name: string;
  /** The dataset's root, ending in "/" */
  url: string;
  version: string;
  metadata: string;
  dataPath: string;
};

/** The store for a dataset's root. The caller decides how to reach it, with which credentials */
export type Opener = (url: string) => Store;

const withSlash = (url: string) => (url.endsWith("/") ? url : url + "/");

/**
 * The version an entry names, or the one main holds when it names none, checked as a reader must
 * (its record, its FDL version, not past retention), with the datasets its views read
 */
const attachment = async (
  open: Opener,
  name: string,
  dep: Dependency,
): Promise<{ a: Attachment; deps: Record<string, Dependency> }> => {
  const url = withSlash(dep.url);
  const store = open(url);
  const [config, id] = await Promise.all([
    readConfig(store),
    resolve(store, dep.version ?? "refs/heads/main"),
  ]);
  const version = await readVersion(store, id, config);
  return {
    a: {
      name,
      url,
      version: id,
      metadata: `${url}versions/${id}/metadata.db`,
      dataPath: url + config.data_dir,
    },
    deps: version.files.dependencies ?? {},
  };
};

/**
 * The datasets to attach to read the given ones, each keyed by the name SQL reads it under, as
 * tables.json's dependencies are. The datasets their views read come first, followed to the end.
 * Each entry is the version it names, or the one main holds. Throws when one name would be two
 * datasets or two versions, since SQL could not tell them apart
 */
export const planAttach = async (
  open: Opener,
  datasets: Readonly<Record<string, Dependency>>,
): Promise<Attachment[]> => {
  const plan: Attachment[] = [];
  // Names are taken before their dependencies are followed, so a cycle ends at the second visit
  const seen = new Map<string, Attachment>();
  const visit = async (name: string, dep: Dependency): Promise<void> => {
    const { a, deps } = await attachment(open, name, dep);
    const same = seen.get(name);
    if (same) {
      if (same.url === a.url && same.version === a.version) return;
      throw new FdlError(
        `${name} is both ${same.url} at ${same.version} and ${a.url} at ${a.version}`,
      );
    }
    seen.set(name, a);
    await Object.entries(deps).reduce<Promise<void>>(
      (prev, [n, d]) => prev.then(() => visit(n, d)),
      Promise.resolve(),
    );
    plan.push(a);
  };
  await Object.entries(datasets).reduce<Promise<void>>(
    (prev, [n, d]) => prev.then(() => visit(n, d)),
    Promise.resolve(),
  );
  return plan;
};

const literal = (s: string) => `'${s.replaceAll("'", "''")}'`;
const identifier = (s: string) => `"${s.replaceAll('"', '""')}"`;

/** The ATTACH statement for one entry of a plan */
export const attachSql = (a: Attachment): string =>
  `ATTACH ${literal(`ducklake:${a.metadata}`)} AS ${identifier(a.name)} ` +
  `(DATA_PATH ${literal(a.dataPath)}, OVERRIDE_DATA_PATH true, READ_ONLY)`;
