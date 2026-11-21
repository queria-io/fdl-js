// Read a version's Parquet files by range with hyparquet: the footer first (cached, since data
// files never change), then only the row groups whose min/max can hold the value asked for
import { decompress } from "fzstd";
import {
  type AsyncBuffer,
  type Compressors,
  type FileMetaData,
  parquetMetadata,
  parquetReadObjects,
} from "hyparquet";
import { type Column, type DataFile, FdlError } from "./repo.js";
import type { Store } from "./store.js";

/** DuckDB writes zstd by default. fzstd is plain JS, so it runs in Workers and browsers */
export const compressors: Compressors = { ZSTD: (input) => decompress(input) };

/** Called after every read, with the bytes read and the milliseconds spent waiting */
export type OnRead = (bytes: number, ms: number) => void;

/** A data file as hyparquet's AsyncBuffer */
export const asyncBuffer = (
  store: Store,
  key: string,
  byteLength: number,
  onRead?: OnRead,
): AsyncBuffer => ({
  byteLength,
  slice: async (start, end = byteLength) => {
    const t0 = Date.now();
    const body = await store.range(key, start, end - start);
    onRead?.(end - start, Date.now() - t0);
    return body;
  },
});

/** Where footers are kept between requests. Keys are data file keys; files never change */
export type FooterCache = {
  get: (key: string) => Promise<ArrayBuffer | undefined>;
  put: (key: string, footer: ArrayBuffer) => Promise<void>;
};

/** A FooterCache on a Cache API cache (caches.default in Workers, caches.open() in browsers) */
export const cacheApiFooterCache = (
  cache: Cache,
  base = "https://fdl.invalid/footer/",
): FooterCache => ({
  get: async (key) => (await cache.match(base + encodeURIComponent(key)))?.arrayBuffer(),
  put: (key, footer) =>
    cache.put(
      base + encodeURIComponent(key),
      new Response(footer, {
        headers: {
          "content-type": "application/octet-stream",
          "cache-control": "public, max-age=31536000, immutable",
        },
      }),
    ),
});

/** A FooterCache in memory, for one process or isolate */
export const memoryFooterCache = (): FooterCache => {
  const kept = new Map<string, ArrayBuffer>();
  return {
    get: async (key) => kept.get(key),
    put: async (key, footer) => void kept.set(key, footer),
  };
};

const MAGIC = 0x31524150; // "PAR1"

/** The footer (metadata and the last 8 bytes). One read when files.json gives footer_bytes */
const readFooter = async (file: AsyncBuffer, footerBytes?: number): Promise<ArrayBuffer> => {
  const size = file.byteLength;
  const guess = footerBytes !== undefined && footerBytes + 8 <= size ? footerBytes + 8 : 8;
  const tail = await file.slice(size - guess, size);
  const view = new DataView(tail);
  if (view.getUint32(guess - 4, true) !== MAGIC)
    throw new Error("not a Parquet file (no PAR1 at the end)");
  const length = view.getUint32(guess - 8, true) + 8;
  if (length > size) throw new Error("Parquet footer is longer than the file");
  return length <= guess ? tail.slice(guess - length) : file.slice(size - length, size);
};

/**
 * A table's column as one data file holds it: its name in the file (undefined when the file was
 * written before the column was added), the value rows of such a file hold, and the cast from the
 * file's values to the column's type (an int32 file of a column widened to int64 gives numbers)
 */
export type FileColumn = {
  name: string;
  physical: string | undefined;
  fill: () => unknown;
  cast: (value: unknown) => unknown;
};

export type ParquetFile = {
  key: string;
  file: AsyncBuffer;
  metadata: FileMetaData;
  /** The table's columns, in files.json's order, when the DataFile gave them */
  columns?: FileColumn[];
  /** Positions of the deleted rows, when the DataFile has a delete file */
  deleted?: Set<number>;
};

export type OpenOptions = { cache?: FooterCache; onRead?: OnRead };

/** The top-level columns of a file, with their field_id. schema[0] is the root */
const topLevel = (metadata: FileMetaData): { name: string; id?: number }[] => {
  const out: { name: string; id?: number }[] = [];
  let i = 1;
  while (i < metadata.schema.length) {
    const s = metadata.schema[i]!;
    out.push({ name: s.name, ...(s.field_id !== undefined ? { id: s.field_id } : {}) });
    // Skip the element's subtree
    let pending = s.num_children ?? 0;
    i++;
    while (pending > 0) pending += (metadata.schema[i++]!.num_children ?? 0) - 1;
  }
  return out;
};

const INTEGER = new Set(["int8", "int16", "int32", "uint8", "uint16", "uint32"]);
const BIGINT = new Set(["int64", "uint64"]);

/** DuckDB writes infinities and NaN as inf, -inf and nan, which Number() does not read */
const SPECIAL: Record<string, number> = { inf: Infinity, "-inf": -Infinity, nan: NaN };
const float = (text: string) => SPECIAL[text.toLowerCase()] ?? Number(text);

/** The value a column's default text stands for, typed as hyparquet returns the column */
const defaultValue = ({ name, type, default: text }: Column): (() => unknown) => {
  if (text === undefined) return () => null;
  if (INTEGER.has(type)) return () => Number(text);
  if (type === "float32" || type === "float64") return () => float(text);
  if (BIGINT.has(type)) return () => BigInt(text);
  if (type === "boolean") return () => text === "true";
  if (type === "varchar" || type === "json") return () => text;
  return () => {
    throw new FdlError(`cannot fill ${name}: a default of type ${type}`);
  };
};

/** Finds each column of the table in the file by field_id (by name when the file has none) */
const fileColumns = (metadata: FileMetaData, columns: Column[]): FileColumn[] => {
  const inFile = topLevel(metadata);
  return columns.map((c) => ({
    name: c.name,
    physical: (
      inFile.find((f) => f.id === c.id) ??
      inFile.find((f) => f.id === undefined && f.name === c.name)
    )?.name,
    fill: defaultValue(c),
    // A column widened from a narrower integer type keeps numbers in the files written before
    cast: BIGINT.has(c.type) ? (v) => (typeof v === "number" ? BigInt(v) : v) : (v) => v,
  }));
};

const deletedPositions = async (
  store: Store,
  deletes: NonNullable<DataFile["deletes"]>,
  options: OpenOptions,
): Promise<Set<number>> => {
  const parquet = await openParquet(store, deletes, options);
  const rows = await readRows<{ pos: number | bigint }>(parquet, { columns: ["pos"] });
  return new Set(rows.map((r) => Number(r.pos)));
};

/**
 * Opens a data file: reads its footer, from the cache when it is there, and the positions in its
 * delete file when it has one
 */
export const openParquet = async (
  store: Store,
  { key, bytes, footerBytes, deletes, columns }: DataFile,
  options: OpenOptions = {},
): Promise<ParquetFile> => {
  const { cache, onRead } = options;
  const file = asyncBuffer(store, key, bytes, onRead);
  const footer = async () => {
    // The cache only saves reads: when it fails, the footer is read from the file
    const hit = await cache?.get(key).catch(() => undefined);
    if (hit) return hit;
    const read = await readFooter(file, footerBytes);
    await cache?.put(key, read).catch(() => {});
    return read;
  };
  const [metadata, deleted] = await Promise.all([
    footer().then(parquetMetadata),
    deletes ? deletedPositions(store, deletes, options) : undefined,
  ]);
  return {
    key,
    file,
    metadata,
    ...(columns ? { columns: fileColumns(metadata, columns) } : {}),
    ...(deleted ? { deleted } : {}),
  };
};

export type RowRange = { rowStart: number; rowEnd: number };
type Value = string | number | bigint;

/** The index in a row group's columns of a top-level leaf column. schema[0] is the root */
const columnIndex = (metadata: FileMetaData, column: string): number => {
  let leaf = 0;
  let i = 1;
  while (i < metadata.schema.length) {
    const s = metadata.schema[i]!;
    if (!s.num_children) {
      if (s.name === column) return leaf;
      leaf++;
      i++;
      continue;
    }
    if (s.name === column) throw new Error(`${column} is not a leaf column`);
    // Skip the group's subtree, counting its leaves
    let pending = s.num_children;
    i++;
    while (pending > 0) {
      const child = metadata.schema[i++]!.num_children ?? 0;
      if (child === 0) leaf++;
      pending += child - 1;
    }
  }
  throw new Error(`no column ${column}`);
};

// A UTF-16 code unit, shifted so that units compare in code point order: surrogates (U+10000
// and above) after U+E000-U+FFFF
const unit = (s: string, i: number) => {
  const u = s.charCodeAt(i);
  return u >= 0xe000 ? u - 0x800 : u >= 0xd800 ? u + 0x2000 : u;
};

// Strings compare in code point order, which is UTF-8 byte order, the order of Parquet's
// statistics. JavaScript's < compares UTF-16 code units, which differs past U+FFFF
const atMost = (a: Value, b: Value): boolean => {
  if (typeof a !== "string" || typeof b !== "string") return a <= b;
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const d = unit(a, i) - unit(b, i);
    if (d !== 0) return d < 0;
  }
  return a.length <= b.length;
};

const within = (value: Value, min: unknown, max: unknown) =>
  (min === undefined || min === null || atMost(min as Value, value)) &&
  (max === undefined || max === null || atMost(value, max as Value));

/**
 * The rows of the row groups whose min/max for column can hold value. Row groups without
 * statistics are kept. Adjacent row groups are merged into one range
 */
export const rowRangesFor = (metadata: FileMetaData, column: string, value: Value): RowRange[] => {
  const c = columnIndex(metadata, column);
  return metadata.row_groups.reduce<{ start: number; ranges: RowRange[] }>(
    ({ start, ranges }, group) => {
      const end = start + Number(group.num_rows);
      const stats = group.columns[c]?.meta_data?.statistics;
      if (!within(value, stats?.min_value ?? stats?.min, stats?.max_value ?? stats?.max))
        return { start: end, ranges };
      const last = ranges.at(-1);
      return {
        start: end,
        ranges:
          last?.rowEnd === start
            ? [...ranges.slice(0, -1), { rowStart: last.rowStart, rowEnd: end }]
            : [...ranges, { rowStart: start, rowEnd: end }],
      };
    },
    { start: 0, ranges: [] },
  ).ranges;
};

/**
 * Reads the rows given (all of them by default), the columns given (all by default), leaving out
 * deleted rows. With the table's columns, columns are found by field_id, so a renamed column is
 * read under its new name, a dropped one is left out, and one added after the file was written
 * holds its default (or null). Without them, columns the file lacks are left out
 */
export const readRows = async <T = Record<string, unknown>>(
  { file, metadata, columns: table, deleted }: ParquetFile,
  { columns, ...range }: Partial<RowRange> & { columns?: string[] } = {},
): Promise<T[]> => {
  const start = range.rowStart ?? 0;
  const keep = <R>(rows: R[]) => (deleted ? rows.filter((_, i) => !deleted.has(start + i)) : rows);
  if (!table) {
    const names = new Set(metadata.schema.slice(1).map((s) => s.name));
    const rows = await parquetReadObjects({
      file,
      metadata,
      compressors,
      ...range,
      // Columns the file lacks are left out, so a reader can ask for a column added later
      ...(columns ? { columns: columns.filter((c) => names.has(c)) } : {}),
    });
    return keep(rows) as T[];
  }
  // Columns the table lacks are left out, so a reader can ask for a column added later
  const wanted = columns ? columns.flatMap((name) => table.filter((t) => t.name === name)) : table;
  const physical = wanted.flatMap((c) => (c.physical === undefined ? [] : [c.physical]));
  const rows =
    physical.length > 0
      ? await parquetReadObjects({ file, metadata, compressors, ...range, columns: physical })
      : Array.from(
          {
            length: Math.max(
              0,
              Math.min(range.rowEnd ?? Infinity, Number(metadata.num_rows)) - start,
            ),
          },
          (): Record<string, unknown> => ({}),
        );
  return keep(rows).map((row) =>
    Object.fromEntries(
      wanted.map((c) => [c.name, c.physical === undefined ? c.fill() : c.cast(row[c.physical])]),
    ),
  ) as T[];
};

// INT64 columns come back as bigint; let 5 and 5n match. == compares a bigint with a number
// by value, and unlike BigInt() it does not throw for 1.5
const same = (a: unknown, b: Value) =>
  a === b ||
  ((typeof a === "bigint" || typeof b === "bigint") &&
    (typeof a === "bigint" || typeof a === "number") &&
    typeof b !== "string" &&
    // oxlint-disable-next-line eqeqeq
    a == b);

/**
 * The rows where column equals value. Reads only the row groups that can hold it, then drops
 * the other rows. Works best on files sorted by column
 */
export const readWhere = async <T = Record<string, unknown>>(
  parquet: ParquetFile,
  column: string,
  value: Value,
  columns?: string[],
): Promise<T[]> => {
  const c = parquet.columns?.find((t) => t.name === column);
  if (parquet.columns && !c) throw new Error(`no column ${column}`);
  // A column added after the file was written holds the same value in every row
  if (c && c.physical === undefined && !same(c.fill(), value)) return [];
  const ranges =
    c && c.physical === undefined
      ? [{ rowStart: 0, rowEnd: Number(parquet.metadata.num_rows) }]
      : rowRangesFor(parquet.metadata, c?.physical ?? column, value);
  const wanted = columns && [column, ...columns.filter((c) => c !== column)];
  const rows = await Promise.all(
    ranges.map((r) =>
      readRows<Record<string, unknown>>(parquet, { ...r, ...(wanted ? { columns: wanted } : {}) }),
    ),
  );
  return rows.flat().filter((row) => same(row[column], value)) as T[];
};
