// Read the _fdl schema of a version (https://fdl.queria.io/spec/fdl-schema): its tables are
// ordinary tables in files.json, read from their data files like any other
import { openParquet, readRows, type OpenOptions } from "./parquet.js";
import { tableFiles, type Version } from "./repo.js";
import type { Store } from "./store.js";

// Struct fields are read as the file holds them: a field the writer left out of the struct is
// absent, not null
export type License = {
  id?: string | null;
  spdx?: string | null;
  url?: string | null;
  title?: string | null;
};
export type Source = { title?: string | null; path?: string | null };
export type Contributor = Source & { roles?: string[] | null };
export type TemporalCoverage = { start?: string | null; end?: string | null };

/** What the dataset as a whole means */
export type DatasetDoc = {
  title: string | null;
  description: string | null;
  language: string | null;
  homepage: string | null;
  issued: Date | null;
  update_frequency: string | null;
  licenses: License[] | null;
  sources: Source[] | null;
  contributors: Contributor[] | null;
  temporal_coverage: TemporalCoverage | null;
  spatial_coverage: string[] | null;
};
export type SchemaDoc = { schema: string; title: string | null; description: string | null };
/** A table or a view */
export type TableDoc = {
  schema: string;
  name: string;
  title: string | null;
  description: string | null;
  licenses: License[] | null;
  sources: Source[] | null;
  temporal_coverage: TemporalCoverage | null;
  spatial_coverage: string[] | null;
  primary_key: string[] | null;
  profile: boolean | null;
  sql: string | null;
  depends_on: string[] | null;
};
export type ColumnDoc = {
  schema: string;
  table: string;
  name: string;
  title: string | null;
  description: string | null;
  asset: boolean | null;
};
/** A file of the dataset, stored as the asset assets/<sha256> */
export type FileDoc = {
  path: string;
  sha256: string | null;
  bytes: number | bigint | null;
  media_type: string | null;
};
export type CatalogDoc = { name: string; url: string | null };
export type AttributeDoc = {
  schema: string;
  table: string;
  column: string;
  key: string;
  value: unknown;
};

/** The rows of the _fdl schema. A table the version lacks reads as no rows */
export type FdlSchema = {
  dataset: DatasetDoc | undefined;
  schemas: SchemaDoc[];
  tables: TableDoc[];
  columns: ColumnDoc[];
  files: FileDoc[];
  catalogs: CatalogDoc[];
  attributes: AttributeDoc[];
};

/** The columns this reader knows of each table of _fdl. A column the version lacks reads as null */
const COLUMNS = {
  dataset: [
    "title",
    "description",
    "language",
    "homepage",
    "issued",
    "update_frequency",
    "licenses",
    "sources",
    "contributors",
    "temporal_coverage",
    "spatial_coverage",
  ],
  schemas: ["schema", "title", "description"],
  tables: [
    "schema",
    "name",
    "title",
    "description",
    "licenses",
    "sources",
    "temporal_coverage",
    "spatial_coverage",
    "primary_key",
    "profile",
    "sql",
    "depends_on",
  ],
  columns: ["schema", "table", "name", "title", "description", "asset"],
  files: ["path", "sha256", "bytes", "media_type"],
  catalogs: ["name", "url"],
  attributes: ["schema", "table", "column", "key", "value"],
} as const;

type Name = keyof typeof COLUMNS;

const rowsOf = async (
  store: Store,
  version: Version,
  name: Name,
  options: OpenOptions,
): Promise<Record<string, unknown>[]> => {
  const columns = COLUMNS[name];
  const files = tableFiles(version, `_fdl.${name}`) ?? [];
  const read = await Promise.all(
    files.map(async (file) =>
      readRows(await openParquet(store, file, options), { columns: [...columns] }),
    ),
  );
  return read.flat().map((row) => Object.fromEntries(columns.map((c) => [c, row[c] ?? null])));
};

/**
 * Reads the tables of the version's _fdl schema: what the dataset, its schemas, tables and
 * columns mean, its files, the repositories its views read, and attributes for other readers.
 * Columns this reader does not know are left out. Primary keys and asset columns are also in
 * files.json (`primary_key`, `assets`), for versions written before _fdl too
 */
export const readFdlSchema = async (
  store: Store,
  version: Version,
  options: OpenOptions = {},
): Promise<FdlSchema> => {
  const read = <T>(name: Name) => rowsOf(store, version, name, options) as Promise<T[]>;
  const [dataset, schemas, tables, columns, files, catalogs, attributes] = await Promise.all([
    read<DatasetDoc>("dataset"),
    read<SchemaDoc>("schemas"),
    read<TableDoc>("tables"),
    read<ColumnDoc>("columns"),
    read<FileDoc>("files"),
    read<CatalogDoc>("catalogs"),
    read<AttributeDoc>("attributes"),
  ]);
  return { dataset: dataset[0], schemas, tables, columns, files, catalogs, attributes };
};
