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
import type { DataFile } from "./repo";
import type { Store } from "./store";

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

export type ParquetFile = { key: string; file: AsyncBuffer; metadata: FileMetaData };

export type OpenOptions = { cache?: FooterCache; onRead?: OnRead };

/** Opens a data file: reads its footer, from the cache when it is there */
export const openParquet = async (
  store: Store,
  { key, bytes, footerBytes }: DataFile,
  { cache, onRead }: OpenOptions = {},
): Promise<ParquetFile> => {
  const file = asyncBuffer(store, key, bytes, onRead);
  const hit = await cache?.get(key);
  if (hit) return { key, file, metadata: parquetMetadata(hit) };
  const footer = await readFooter(file, footerBytes);
  await cache?.put(key, footer);
  return { key, file, metadata: parquetMetadata(footer) };
};

export type RowRange = { rowStart: number; rowEnd: number };
type Value = string | number | bigint;

const columnIndex = (metadata: FileMetaData, column: string): number => {
  // schema[0] is the root; leaves follow in column order (flat tables only)
  const i = metadata.schema.findIndex((s, n) => n > 0 && s.name === column) - 1;
  if (i < 0) throw new Error(`no column ${column}`);
  return i;
};

const within = (value: Value, min: unknown, max: unknown) =>
  (min === undefined || min === null || (min as Value) <= value) &&
  (max === undefined || max === null || value <= (max as Value));

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

/** Reads the rows given (all of them by default), the columns given (all by default) */
export const readRows = <T = Record<string, unknown>>(
  { file, metadata }: ParquetFile,
  { columns, ...range }: Partial<RowRange> & { columns?: string[] } = {},
): Promise<T[]> => {
  const names = new Set(metadata.schema.slice(1).map((s) => s.name));
  return parquetReadObjects({
    file,
    metadata,
    compressors,
    ...range,
    // Columns the file lacks are left out, so a reader can ask for a column added later
    ...(columns ? { columns: columns.filter((c) => names.has(c)) } : {}),
  }) as Promise<T[]>;
};

// INT64 columns come back as bigint; let 5 and 5n match
const same = (a: unknown, b: Value) =>
  a === b ||
  ((typeof a === "bigint" || typeof a === "number") &&
    typeof b !== "string" &&
    BigInt(a) === BigInt(b));

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
  const ranges = rowRangesFor(parquet.metadata, column, value);
  const wanted = columns && [column, ...columns.filter((c) => c !== column)];
  const rows = await Promise.all(
    ranges.map((r) =>
      readRows<Record<string, unknown>>(parquet, { ...r, ...(wanted ? { columns: wanted } : {}) }),
    ),
  );
  return rows.flat().filter((row) => same(row[column], value)) as T[];
};
